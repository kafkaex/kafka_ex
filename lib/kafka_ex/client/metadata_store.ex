defmodule KafkaEx.Client.MetadataStore do
  @moduledoc false
  @behaviour :gen_statem

  require Logger

  alias KafkaEx.Client
  alias KafkaEx.Client.Connection
  alias KafkaEx.Client.Infra
  alias KafkaEx.Client.MetadataLog
  alias KafkaEx.Client.MetadataStore.Round
  alias KafkaEx.Client.MetadataStore.Snapshot
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Config
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Support.Retry

  @defaults [
    min_interval: 100,
    backoff_base: 100,
    backoff_cap: 1_000,
    coordinator_base: 500,
    coordinator_cap: 1_000,
    ladder_delay: 300
  ]
  @call_timeout 5_000

  @type address :: {binary(), :inet.port_number()}
  @type args :: %{
          client: pid(),
          infra: pid(),
          bootstrap: [address()],
          initial_topics: [binary()],
          interval: pos_integer(),
          allow_auto_topic_creation: boolean(),
          connection: %{
            use_ssl: boolean(),
            transport: module(),
            secrets: (-> map()),
            pacing: :ets.tid(),
            opts: Keyword.t()
          },
          opts: Keyword.t()
        }
  @type refresh :: %{topics: [binary()], allow_auto_topic_creation: boolean()}

  @spec start_link(args()) :: :gen_statem.start_ret()
  def start_link(args), do: :gen_statem.start_link(__MODULE__, args, [])

  @spec await_ready(pid(), timeout()) :: {:ok, %{table: Snapshot.t(), api_versions: map()}} | {:error, {:boot, term()}}
  def await_ready(store, timeout), do: :gen_statem.call(store, :await_ready, timeout)

  @spec table(pid(), timeout()) :: {:ok, Snapshot.t()}
  def table(store, timeout), do: :gen_statem.call(store, :table, timeout)

  @spec refresh(pid(), refresh(), timeout()) :: {:ok, %{unknown: [binary()]}} | {:error, term()}
  def refresh(store, refresh, timeout), do: :gen_statem.call(store, {:refresh, refresh}, timeout)

  @spec coordinator(pid(), binary(), timeout()) :: :ok | {:error, term()}
  def coordinator(store, group, timeout), do: :gen_statem.call(store, {:coordinator, group}, timeout)

  @spec forget_coordinator(pid(), binary(), timeout()) :: :ok
  def forget_coordinator(store, group, timeout), do: :gen_statem.call(store, {:forget_coordinator, group}, timeout)

  @spec connection(pid(), address(), timeout()) :: {:ok, pid()}
  def connection(store, address, timeout), do: :gen_statem.call(store, {:connection, address}, timeout)

  @spec candidates(pid(), timeout()) :: [address()]
  def candidates(store, timeout), do: :gen_statem.call(store, :candidates, timeout)

  @spec next_seq(pid(), timeout()) :: pos_integer()
  def next_seq(store, timeout), do: :gen_statem.call(store, :next_seq, timeout)

  @spec merge(pid(), pos_integer(), ClusterMetadata.t(), Snapshot.scope(), timeout()) :: :ok
  def merge(store, seq, metadata, scope, timeout),
    do: :gen_statem.call(store, {:merge, seq, metadata, scope}, timeout)

  @spec stop(pid(), term()) :: :ok
  def stop(store, reason), do: :gen_statem.stop(store, reason, @call_timeout)

  @spec call_timeout(pos_integer()) :: pos_integer()
  def call_timeout(brokers), do: brokers * (Config.connect_timeout() + 1_500 + Config.request_timeout() + 1_000) + 2_600

  @doc false
  @spec prune(Snapshot.freshness(), pos_integer()) :: Snapshot.freshness()
  def prune(freshness, floor), do: %{freshness | topics: Map.filter(freshness.topics, fn {_, seq} -> seq >= floor end)}

  @impl :gen_statem
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl :gen_statem
  def init(args) do
    data = %{
      client: args.client,
      infra: args.infra,
      bootstrap: args.bootstrap,
      interval: args.interval,
      allow_default: args.allow_auto_topic_creation,
      connection: args.connection,
      opts: Map.new(Keyword.merge(@defaults, args.opts)),
      conn_sup: nil,
      table: Snapshot.new(),
      freshness: %{topics: %{}, brokers: 0},
      seq: 0,
      tickets: %{},
      versions: nil,
      current: nil,
      conns: %{},
      tracked: MapSet.new(args.initial_topics),
      known_missing: MapSet.new(),
      missing_logged_at: nil,
      in_flight: %{},
      queued: %{},
      boot_waiters: [],
      load_waiters: [],
      failures: 0,
      degraded_at: nil
    }

    {:ok, :loading, data, [{:next_event, :internal, :start}]}
  end

  @impl :gen_statem
  def handle_event(:enter, _old, _state, _data), do: :keep_state_and_data

  def handle_event(:internal, :start, :loading, data),
    do: start_load(%{data | conn_sup: Infra.connection_supervisor(data.infra)}, [])

  def handle_event({:call, from}, :await_ready, :loading, data),
    do: {:keep_state, %{data | boot_waiters: data.boot_waiters ++ [from]}}

  def handle_event({:call, from}, :await_ready, _state, data),
    do: {:keep_state_and_data, [{:reply, from, {:ok, %{table: data.table, api_versions: data.versions}}}]}

  def handle_event({:call, from}, :table, _state, data), do: {:keep_state_and_data, [{:reply, from, {:ok, data.table}}]}

  def handle_event({:call, from}, {trigger, _} = msg, :loading, data) when trigger in [:refresh, :coordinator] do
    data = track(data, msg)
    start_load(%{data | load_waiters: data.load_waiters ++ [{from, msg}]}, [])
  end

  def handle_event({:call, from}, {:refresh, refresh} = msg, _state, data) do
    key = {:tracked, refresh.allow_auto_topic_creation}
    data |> track(msg) |> trigger(key, refresh.topics, from)
  end

  def handle_event({:call, from}, {:coordinator, group}, _state, data),
    do: trigger(data, {:coordinator, group}, [], from)

  def handle_event({:call, from}, {:forget_coordinator, group}, _state, data) do
    Snapshot.drop_coordinator(data.table, group)
    {:keep_state_and_data, [{:reply, from, :ok}]}
  end

  def handle_event({:call, from}, {:connection, address}, _state, data) do
    {data, pid} = ensure_connection(data, address)
    {:keep_state, data, [{:reply, from, {:ok, pid}}]}
  end

  def handle_event({:call, from}, :candidates, _state, data),
    do: {:keep_state_and_data, [{:reply, from, candidate_addresses(data)}]}

  def handle_event({:call, from}, :next_seq, _state, data) do
    seq = data.seq + 1
    expires_at = now() + ticket_ttl(data)
    tickets = Map.put(data.tickets, seq, expires_at)
    {:keep_state, %{data | seq: seq, tickets: tickets}, [{:reply, from, seq}, sweep(tickets)]}
  end

  def handle_event({:call, from}, {:merge, seq, metadata, scope}, _state, data) do
    data =
      case Map.pop(data.tickets, seq) do
        {nil, _tickets} -> data
        {_expires_at, tickets} -> apply_snapshot(%{data | tickets: tickets}, seq, metadata, scope)
      end

    {:keep_state, data, [{:reply, from, :ok}]}
  end

  def handle_event(:info, {:round, ref, result}, state, data) do
    case take_round(data, &(&1.ref == ref)) do
      {nil, _data} ->
        :keep_state_and_data

      {round, data} ->
        Process.demonitor(round.mref, [:flush])
        finish(round, result, state, data)
    end
  end

  def handle_event(:info, {:DOWN, mref, :process, _pid, _reason}, state, data) do
    case take_round(data, &(&1.mref == mref)) do
      {nil, data} -> {:keep_state, connection_down(data, mref)}
      {round, data} -> finish(round, {:error, :round_down}, state, data)
    end
  end

  def handle_event({:timeout, {:round, ref}}, nil, state, data) do
    case take_round(data, &(&1.ref == ref)) do
      {nil, _data} ->
        :keep_state_and_data

      {round, data} ->
        Process.exit(round.pid, :kill)
        Process.demonitor(round.mref, [:flush])
        finish(round, {:error, :round_timeout}, state, data)
    end
  end

  def handle_event({:timeout, :periodic}, nil, state, data) when state in [:ready, :degraded] do
    {:keep_state, data, actions} = trigger(data, {:tracked, data.allow_default}, [], nil)
    {:keep_state, data, [periodic(data) | actions]}
  end

  def handle_event({:timeout, :sweep}, nil, _state, data) do
    data = prune_freshness(data)
    {:keep_state, data, if(data.tickets == %{}, do: [], else: [sweep(data.tickets)])}
  end

  def handle_event(:state_timeout, :retry, :loading, data), do: start_load(data, [])

  def handle_event(:state_timeout, :retry, :degraded, data),
    do: trigger(data, {:tracked, data.allow_default}, [], nil)

  def handle_event(type, content, _state, _data) do
    Logger.debug("#{inspect(__MODULE__)} ignoring #{inspect(type)} #{inspect(content, limit: 5)}")
    :keep_state_and_data
  end

  @impl :gen_statem
  def format_status(status), do: Map.update(status, :data, nil, &redact/1)

  defp redact(%{connection: connection} = data), do: %{data | connection: %{connection | secrets: :redacted}}
  defp redact(data), do: data

  defp track(data, {:refresh, %{topics: topics}}), do: %{data | tracked: MapSet.union(data.tracked, MapSet.new(topics))}
  defp track(data, _msg), do: data

  defp trigger(data, key, topics, from) do
    waiters = if from, do: [from], else: []

    case data.in_flight do
      %{^key => round} ->
        if covers?(round, topics),
          do: {:keep_state, put_in(data.in_flight[key].waiters, round.waiters ++ waiters), []},
          else: {:keep_state, %{data | queued: Map.update(data.queued, key, waiters, &(&1 ++ waiters))}, []}

      _ ->
        {data, actions} = start_round(data, key, waiters)
        {:keep_state, data, actions}
    end
  end

  defp covers?(%{topics: round_topics}, topics),
    do: MapSet.size(round_topics) == 0 or Enum.all?(topics, &MapSet.member?(round_topics, &1))

  defp start_load(%{in_flight: %{load: _}} = data, actions), do: {:keep_state, data, actions}

  defp start_load(data, actions) do
    {data, round_actions} = start_round(data, :load, [])
    {:keep_state, data, actions ++ round_actions}
  end

  defp start_round(data, key, waiters) do
    {kind, request_opts, addresses} = round_request(data, key)
    {data, candidates} = Enum.reduce(addresses, {data, []}, &add_candidate/2)
    seq = data.seq + 1
    {spec, bound} = round_spec(data, kind, request_opts, Enum.reverse(candidates))
    ref = make_ref()
    store = self()
    {pid, mref} = spawn_monitor(fn -> send(store, {:round, ref, Round.run(spec)}) end)
    topics = MapSet.new(Keyword.get(request_opts, :topics, []))
    round = %{key: key, kind: kind, ref: ref, pid: pid, mref: mref, seq: seq, topics: topics, waiters: waiters}
    data = %{data | seq: seq, in_flight: Map.put(data.in_flight, key, round)}
    {data, [{{:timeout, {:round, ref}}, bound, nil}]}
  end

  defp round_spec(data, kind, request_opts, candidates) do
    budget = %{
      kind: kind,
      candidates: candidates,
      ladder_delay: data.opts.ladder_delay,
      connect_timeout: Keyword.get_lazy(data.connection.opts, :connect_timeout, &Config.connect_timeout/0),
      request_timeout: Config.request_timeout()
    }

    bound = Round.bound(budget)

    spec =
      Map.merge(budget, %{
        request_opts: request_opts,
        known_missing: data.known_missing,
        brokers: Map.new(Snapshot.brokers(data.table), &{{&1.host, &1.port}, &1}),
        deadline: now() + bound
      })

    {spec, bound}
  end

  defp round_request(data, :load),
    do: {:load, metadata_opts(data, data.allow_default), data.bootstrap}

  defp round_request(data, {:tracked, allow}), do: {:metadata, metadata_opts(data, allow), candidate_addresses(data)}

  defp round_request(data, {:coordinator, group}),
    do: {:find_coordinator, [group_id: group], candidate_addresses(data)}

  defp metadata_opts(data, allow),
    do: [topics: MapSet.to_list(data.tracked), allow_auto_topic_creation: allow]

  defp add_candidate(address, {data, candidates}) do
    {data, pid} = ensure_connection(data, address)
    {data, [{address, pid} | candidates]}
  end

  defp take_round(data, match) do
    case Enum.find(Map.values(data.in_flight), match) do
      nil -> {nil, data}
      round -> {round, %{data | in_flight: Map.delete(data.in_flight, round.key)}}
    end
  end

  defp finish(round, result, state, data) do
    {queued, rest} = Map.pop(data.queued, round.key)
    {next_state, data, actions} = handle_result(round.kind, round, result, state, %{data | queued: rest})
    {data, requeued} = requeue(data, round.key, queued)
    {:next_state, next_state, data, [{{:timeout, {:round, round.ref}}, :cancel} | actions ++ requeued]}
  end

  defp requeue(data, _key, nil), do: {data, []}
  defp requeue(data, key, waiters), do: start_round(data, key, waiters)

  defp handle_result(:load, round, {:ok, result}, _state, data) do
    data = data |> apply_snapshot(round.seq, result.metadata, :tracked) |> note_missing(result.missing)
    data = %{data | current: result.address, versions: result.versions, failures: 0}
    ready = {:ok, %{table: data.table, api_versions: data.versions}}
    replies = for from <- data.boot_waiters, do: {:reply, from, ready}
    triggers = for {from, msg} <- data.load_waiters, do: {:next_event, {:call, from}, msg}
    {:ready, %{data | boot_waiters: [], load_waiters: []}, replies ++ [periodic(data) | triggers]}
  end

  defp handle_result(:load, _round, failure, :loading, data) do
    reason = reason(failure)
    boot = for from <- data.boot_waiters, do: {:reply, from, {:error, {:boot, reason}}}
    loading = for {from, _msg} <- data.load_waiters, do: {:reply, from, {:error, {:loading, reason}}}
    data = %{data | boot_waiters: [], load_waiters: [], failures: data.failures + 1}
    {:loading, data, boot ++ loading ++ [{:state_timeout, backoff(data), :retry}]}
  end

  defp handle_result(:metadata, round, {:ok, result}, state, data) do
    data = data |> apply_snapshot(round.seq, result.metadata, :tracked) |> note_missing(result.missing)
    replies = for from <- round.waiters, do: {:reply, from, {:ok, %{unknown: result.unknown}}}
    recovered(state, %{data | current: result.address, failures: 0}, replies)
  end

  defp handle_result(:find_coordinator, round, {:ok, %{coordinator: %FindCoordinator{} = coordinator}}, state, data) do
    {:coordinator, group} = round.key
    Snapshot.put_coordinator(data.table, group, FindCoordinator.coordinator_node_id(coordinator))
    {state, data, for(from <- round.waiters, do: {:reply, from, :ok})}
  end

  defp handle_result(:find_coordinator, round, {:ok, %{coordinator: {:error, error}}}, state, data),
    do: {state, data, for(from <- round.waiters, do: {:reply, from, {:error, error}})}

  defp handle_result(_kind, round, failure, state, data) do
    reason = reason(failure)
    replies = for from <- round.waiters, do: {:reply, from, {:error, reason}}
    degraded(state, reason, %{data | failures: data.failures + 1}, replies)
  end

  defp reason({:raised, module}), do: {:raised, module}
  defp reason({:error, reason}), do: reason

  # Only a :metadata success leaves :degraded: a coordinator answer does not prove metadata works.
  defp recovered(:degraded, data, replies) do
    duration = System.monotonic_time() - data.degraded_at
    :telemetry.execute([:kafka_ex, :metadata_store, :recovered], %{duration: duration}, %{})
    {:ready, %{data | degraded_at: nil}, replies}
  end

  defp recovered(state, data, replies), do: {state, data, replies}

  defp degraded(:ready, reason, data, replies) do
    :telemetry.execute([:kafka_ex, :metadata_store, :degraded], %{system_time: System.system_time()}, %{reason: reason})
    data = %{data | degraded_at: System.monotonic_time()}
    {:degraded, data, replies ++ [{:state_timeout, backoff(data), :retry}]}
  end

  defp degraded(state, _reason, data, replies), do: {state, data, replies ++ [{:state_timeout, backoff(data), :retry}]}

  defp backoff(data), do: Retry.backoff_delay(max(data.failures - 1, 0), data.opts.backoff_base, data.opts.backoff_cap)

  defp periodic(data), do: {{:timeout, :periodic}, data.interval, nil}

  defp apply_snapshot(data, seq, metadata, scope) do
    case Snapshot.apply(data.table, metadata, %{seq: seq, scope: scope}, data.freshness) do
      {:applied, freshness} ->
        send(data.client, {:kafka_ex_metadata_store, :swapped})
        prune_freshness(stop_departed(%{data | freshness: freshness}))

      :ignored ->
        data
    end
  end

  # No response older than the oldest live seq can still arrive, so older freshness entries decide nothing.
  defp sweep(tickets), do: {{:timeout, :sweep}, Enum.min(Map.values(tickets)), nil, [abs: true]}

  defp prune_freshness(data) do
    now = now()
    tickets = Map.filter(data.tickets, fn {_seq, expires_at} -> expires_at > now end)
    live = Map.keys(tickets) ++ Enum.map(Map.values(data.in_flight), & &1.seq)
    floor = Enum.min([data.seq + 1 | live])
    %{data | tickets: tickets, freshness: prune(data.freshness, floor)}
  end

  defp ticket_ttl(data) do
    connect_timeout = Keyword.get_lazy(data.connection.opts, :connect_timeout, &Config.connect_timeout/0)
    Client.retry_count() * (connect_timeout + 1_500 + Config.request_timeout() + 1_000) + @call_timeout
  end

  defp note_missing(data, []) do
    if MapSet.size(data.known_missing) > 0,
      do: Logger.info("Kafka metadata recovered for topics #{inspect(Enum.sort(data.known_missing))}")

    %{data | known_missing: MapSet.new(), missing_logged_at: nil}
  end

  defp note_missing(data, missing) do
    missing_set = MapSet.new(missing)
    now = now()

    if MetadataLog.should_log?(missing_set, data.known_missing, data.missing_logged_at, now) do
      Logger.warning(
        "Kafka metadata unavailable for topics #{inspect(Enum.sort(missing))} " <>
          "after #{Client.retry_count()} attempts (topic unknown or leader not available)"
      )

      %{data | known_missing: missing_set, missing_logged_at: now}
    else
      %{data | known_missing: missing_set}
    end
  end

  defp candidate_addresses(data) do
    case Enum.map(Snapshot.brokers(data.table), &{&1.host, &1.port}) do
      [] -> data.bootstrap
      addresses -> current_first(data.current, Enum.uniq(addresses))
    end
  end

  defp current_first(current, addresses) do
    if current in addresses, do: [current | Enum.shuffle(addresses -- [current])], else: Enum.shuffle(addresses)
  end

  defp ensure_connection(data, address) do
    case data.conns do
      %{^address => %{pid: pid}} ->
        {data, pid}

      _ ->
        {:ok, pid} = DynamicSupervisor.start_child(data.conn_sup, {Connection, connection_args(data, address)})
        {%{data | conns: Map.put(data.conns, address, %{pid: pid, ref: Process.monitor(pid)})}, pid}
    end
  end

  defp connection_args(data, {host, port}) do
    data.connection
    |> Map.take([:use_ssl, :transport, :secrets, :pacing, :opts])
    |> Map.merge(%{client: self(), host: host, port: port, role: :metadata})
  end

  defp stop_departed(data) do
    addresses = MapSet.new(Snapshot.brokers(data.table), &{&1.host, &1.port})
    {departed, kept} = Enum.split_with(data.conns, fn {address, _conn} -> not MapSet.member?(addresses, address) end)

    Enum.each(departed, fn {_address, conn} ->
      Process.demonitor(conn.ref, [:flush])
      Connection.stop(conn.pid, :metadata_update)
    end)

    current = if MapSet.member?(addresses, data.current), do: data.current
    %{data | conns: Map.new(kept), current: current}
  end

  defp connection_down(data, mref) do
    case Enum.find(data.conns, fn {_address, conn} -> conn.ref == mref end) do
      nil -> data
      {address, _conn} -> %{data | conns: Map.delete(data.conns, address), current: unless_current(data, address)}
    end
  end

  defp unless_current(%{current: address}, address), do: nil
  defp unless_current(%{current: current}, _address), do: current

  defp now, do: System.monotonic_time(:millisecond)
end
