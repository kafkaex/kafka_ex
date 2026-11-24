defmodule KafkaEx.Client.MetadataStore do
  @moduledoc false
  @behaviour :gen_statem

  require Logger

  alias KafkaEx.Client
  alias KafkaEx.Client.ConnectionStateMachine
  alias KafkaEx.Client.MetadataLog
  alias KafkaEx.Client.MetadataStore.MetadataTable
  alias KafkaEx.Client.MetadataStore.Round
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Config
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Support.Retry
  alias KafkaEx.Telemetry

  @defaults [
    min_interval: 100,
    backoff_base: 100,
    backoff_cap: 1_000,
    coordinator_base: 500,
    coordinator_cap: 1_000,
    ladder_delay: 300,
    connect_slack: 1_500,
    request_slack: 1_000
  ]
  @call_timeout 5_000

  @type address :: {binary(), :inet.port_number()}
  @type args :: %{
          client: pid(),
          supervisor: pid(),
          bootstrap: [address()],
          initial_topics: [binary()],
          interval: pos_integer(),
          allow_auto_topic_creation: boolean(),
          connection: %{
            use_ssl: boolean(),
            secrets: (-> map()),
            reconnect_backoff: :ets.tid()
          }
        }
  @type refresh :: %{topics: [binary()], allow_auto_topic_creation: boolean()}

  @spec start_link(args()) :: :gen_statem.start_ret()
  def start_link(args), do: :gen_statem.start_link(__MODULE__, args, [])

  @spec await_ready(pid(), timeout()) ::
          {:ok, %{table: MetadataTable.t(), api_versions: map()}} | {:error, {:boot, term()}}
  def await_ready(store, timeout), do: :gen_statem.call(store, :await_ready, timeout)

  @spec table(pid(), timeout()) :: {:ok, MetadataTable.t()}
  def table(store, timeout), do: :gen_statem.call(store, :table, timeout)

  @spec refresh(pid(), refresh(), timeout()) :: {:ok, %{unknown: [binary()]}} | {:error, term()}
  def refresh(store, refresh, timeout), do: :gen_statem.call(store, {:refresh, refresh}, timeout)

  @spec coordinator(pid(), binary(), timeout()) :: :ok | {:error, term()}
  def coordinator(store, group, timeout), do: :gen_statem.call(store, {:coordinator, group}, timeout)

  @spec forget_coordinator(pid(), binary(), timeout()) :: :ok
  def forget_coordinator(store, group, timeout), do: :gen_statem.call(store, {:forget_coordinator, group}, timeout)

  @spec connection(pid(), address(), timeout()) :: {:ok, pid() | nil}
  def connection(store, address, timeout), do: :gen_statem.call(store, {:connection, address}, timeout)

  @spec candidates(pid(), timeout()) :: [address()]
  def candidates(store, timeout), do: :gen_statem.call(store, :candidates, timeout)

  @spec snapshot(pid(), timeout()) :: {:ok, ClusterMetadata.t(), non_neg_integer()}
  def snapshot(store, timeout), do: :gen_statem.call(store, :snapshot, timeout)

  @spec next_seq(pid(), timeout()) :: pos_integer()
  def next_seq(store, timeout), do: :gen_statem.call(store, :next_seq, timeout)

  @spec merge(pid(), pos_integer(), ClusterMetadata.t(), MetadataTable.scope(), timeout()) :: :ok
  def merge(store, seq, metadata, scope, timeout) do
    :gen_statem.call(store, {:merge, seq, metadata, scope}, timeout)
  end

  @spec stop(pid(), term()) :: :ok
  def stop(store, reason), do: :gen_statem.stop(store, reason, @call_timeout)

  # A trigger can wait for the in-flight round of its key, then its own, plus the longest pacing delay (cap x 1.2).
  @spec call_timeout(non_neg_integer(), Round.kind()) :: pos_integer()
  def call_timeout(brokers, kind \\ :metadata) do
    opts = store_options()
    budget = budget(kind, List.duplicate(nil, max(brokers, 1)), opts)

    2 * Round.bound(budget) + div(max(opts.backoff_cap, opts.coordinator_cap) * 6, 5)
  end

  defp prune(freshness, floor), do: %{freshness | topics: Map.filter(freshness.topics, fn {_, seq} -> seq >= floor end)}

  defp store_options do
    Map.new(Keyword.merge(@defaults, Application.get_env(:kafka_ex, :metadata_store, [])))
  end

  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  @impl :gen_statem
  def init(args) do
    data = %{
      client: args.client,
      supervisor: args.supervisor,
      bootstrap: args.bootstrap,
      interval: args.interval,
      allow_default: args.allow_auto_topic_creation,
      connection: args.connection,
      opts: store_options(),
      table: MetadataTable.new(),
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
      scheduled: %{},
      backoffs: %{},
      boot_waiters: [],
      load_waiters: [],
      failures: 0,
      degraded_at: nil
    }

    {:ok, :loading, data, [{:next_event, :internal, :start}]}
  end

  @impl :gen_statem
  def handle_event(:internal, :start, :loading, data), do: start_load(data, [])

  def handle_event({:call, from}, :await_ready, :loading, data) do
    {:keep_state, %{data | boot_waiters: data.boot_waiters ++ [from]}}
  end

  def handle_event({:call, from}, :await_ready, _state, data) do
    {:keep_state_and_data, [{:reply, from, {:ok, %{table: data.table, api_versions: data.versions}}}]}
  end

  def handle_event({:call, from}, :table, _state, data), do: {:keep_state_and_data, [{:reply, from, {:ok, data.table}}]}

  # The store is the only writer, so no swap can interleave with this read.
  def handle_event({:call, from}, :snapshot, _state, data) do
    {:ok, counter} = MetadataTable.counter(data.table)
    {:keep_state_and_data, [{:reply, from, {:ok, MetadataTable.to_cluster_metadata(data.table), counter}}]}
  end

  def handle_event({:call, from}, {trigger, _} = msg, :loading, data) when trigger in [:refresh, :coordinator] do
    data = track(data, msg)
    start_load(%{data | load_waiters: data.load_waiters ++ [{from, msg}]}, [])
  end

  def handle_event({:call, from}, {:refresh, refresh} = msg, _state, data) do
    key = {:tracked, refresh.allow_auto_topic_creation}
    data |> track(msg) |> trigger(key, refresh.topics, from, pacing_keys(refresh.topics))
  end

  def handle_event({:call, from}, {:coordinator, group}, _state, data) do
    trigger(data, {:coordinator, group}, [], from, [{:coordinator, group}])
  end

  def handle_event({:call, from}, {:forget_coordinator, group}, _state, data) do
    MetadataTable.drop_coordinator(data.table, group)
    {:keep_state_and_data, [{:reply, from, :ok}]}
  end

  def handle_event({:call, from}, {:connection, address}, _state, data) do
    {data, pid} = ensure_connection(data, address)
    {:keep_state, data, [{:reply, from, {:ok, pid}}]}
  end

  def handle_event({:call, from}, :candidates, _state, data) do
    {:keep_state_and_data, [{:reply, from, candidate_addresses(data)}]}
  end

  def handle_event({:call, from}, :next_seq, _state, data) do
    seq = data.seq + 1
    expires_at = now() + ticket_ttl(data)
    data = %{data | seq: seq, tickets: Map.put(data.tickets, seq, expires_at)}
    {:keep_state, data, [{:reply, from, seq} | sweep(data)]}
  end

  def handle_event({:call, from}, {:merge, seq, metadata, scope}, _state, data) do
    data =
      case Map.pop(data.tickets, seq) do
        {nil, _tickets} ->
          data

        {_expires_at, tickets} ->
          {_outcome, data} = apply_snapshot(%{data | tickets: tickets}, seq, metadata, scope)
          data
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
    {:keep_state, data, actions} = trigger(data, {:tracked, data.allow_default}, [], nil, [])
    {:keep_state, data, [periodic(data) | actions]}
  end

  def handle_event({:timeout, {:paced, key}}, nil, _state, data) do
    now = now()

    case data do
      %{scheduled: %{^key => entry}} ->
        at = due_at(data, entry, now)

        if at > now do
          {:keep_state_and_data, [paced(key, at)]}
        else
          {data, actions} = launch(data, key, [], [])
          {:keep_state, data, actions}
        end

      _none ->
        :keep_state_and_data
    end
  end

  def handle_event({:timeout, :sweep}, nil, _state, data) do
    data = data |> prune_freshness() |> prune_backoffs()
    {:keep_state, data, sweep(data)}
  end

  def handle_event(:state_timeout, :retry, :loading, data), do: start_load(data, [])

  def handle_event(:state_timeout, :retry, :degraded, data) do
    trigger(data, {:tracked, data.allow_default}, [], nil, [])
  end

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

  defp pacing_keys([]), do: [:tracked]
  defp pacing_keys(topics), do: topics

  defp trigger(data, key, topics, from, pacing_keys) do
    waiters = if from, do: [from], else: []
    reasons = MapSet.new(pacing_keys)
    now = now()

    case data.in_flight do
      %{^key => round} ->
        if covers?(round, topics) do
          {:keep_state, join(data, key, waiters, reasons), []}
        else
          schedule(data, key, waiters, reasons)
        end

      _idle ->
        if ready_at(data, reasons, now) > now do
          schedule(data, key, waiters, reasons)
        else
          {data, actions} = launch(data, key, waiters, pacing_keys)
          {:keep_state, data, actions}
        end
    end
  end

  # A round fetches every tracked topic anyway, so the key's scheduled waiters ride along at no extra request.
  defp launch(data, key, waiters, pacing_keys) do
    case Map.pop(data.scheduled, key) do
      {nil, _scheduled} ->
        start_round(data, key, waiters, MapSet.new(pacing_keys))

      {entry, scheduled} ->
        {waiters, reasons} =
          Enum.reduce(entry.triggers, {waiters, MapSet.new(pacing_keys)}, fn trigger, {waiters, reasons} ->
            {trigger.waiters ++ waiters, MapSet.union(trigger.reasons, reasons)}
          end)

        {data, actions} = start_round(%{data | scheduled: scheduled}, key, waiters, reasons)
        {data, [{{:timeout, {:paced, key}}, :cancel} | actions]}
    end
  end

  defp covers?(%{topics: round_topics}, topics), do: Enum.all?(topics, &MapSet.member?(round_topics, &1))

  defp join(data, key, waiters, reasons) do
    update_in(data.in_flight[key], fn round ->
      %{round | waiters: round.waiters ++ waiters, reasons: MapSet.union(round.reasons, reasons)}
    end)
  end

  defp schedule(data, key, waiters, reasons) do
    entry = Map.get(data.scheduled, key, %{triggers: []})
    data = put_in(data.scheduled[key], %{entry | triggers: [%{waiters: waiters, reasons: reasons} | entry.triggers]})
    {:keep_state, data, if(Map.has_key?(data.in_flight, key), do: [], else: rearm(data, key))}
  end

  defp rearm(data, key) do
    case data.scheduled do
      %{^key => entry} -> [paced(key, due_at(data, entry, now()))]
      _none -> []
    end
  end

  defp paced(key, at), do: {{:timeout, {:paced, key}}, at, nil, [abs: true]}

  defp due_at(data, entry, now), do: entry.triggers |> Enum.map(&ready_at(data, &1.reasons, now)) |> Enum.min()

  defp ready_at(data, reasons, now) do
    Enum.reduce(reasons, now, fn reason, at ->
      case data.backoffs do
        %{^reason => %{next_at: next_at}} -> max(at, next_at)
        _first -> at
      end
    end)
  end

  defp pace(data, round, result) do
    now = now()

    backoffs =
      Enum.reduce(round.reasons, data.backoffs, fn reason, backoffs ->
        entry = next_at(backoffs[reason], outcome(reason, result, data), now, pacing(data.opts, reason))
        Map.put(backoffs, reason, entry)
      end)

    %{data | backoffs: backoffs}
  end

  defp next_at(_entry, :ok, now, pacing), do: %{failures: 0, next_at: now + pacing.min_interval}
  defp next_at(entry, :failed, now, pacing), do: %{failures: failures(entry), next_at: now + pacing.min_interval}

  defp next_at(entry, :bad, now, pacing) do
    failures = failures(entry) + 1
    %{failures: failures, next_at: now + max(pacing.min_interval, delay(failures, pacing.base, pacing.cap))}
  end

  defp failures(nil), do: 0
  defp failures(entry), do: entry.failures

  defp pacing(opts, {:coordinator, _group}) do
    %{min_interval: opts.min_interval, base: opts.coordinator_base, cap: opts.coordinator_cap}
  end

  defp pacing(opts, _reason), do: %{min_interval: opts.min_interval, base: opts.backoff_base, cap: opts.backoff_cap}

  defp outcome({:coordinator, _group}, {:ok, %{coordinator: %FindCoordinator{} = coordinator}}, data) do
    node_id = FindCoordinator.coordinator_node_id(coordinator)
    if Enum.any?(MetadataTable.brokers(data.table), &(&1.node_id == node_id)), do: :ok, else: :bad
  end

  defp outcome({:coordinator, _group}, _result, _data), do: :bad

  defp outcome(topic, {:ok, %{metadata: metadata, unknown: unknown}}, _data) when is_binary(topic) do
    case Map.fetch(metadata.topics, topic) do
      {:ok, %Topic{partition_leaders: leaders}} ->
        if topic in unknown or Enum.any?(Map.values(leaders), &(&1 < 0)), do: :bad, else: :ok

      :error ->
        :bad
    end
  end

  defp outcome(:tracked, {:ok, _result}, _data), do: :ok

  # A failed metadata round is the degraded retry's to back off, not the trigger's.
  defp outcome(_reason, _failure, _data), do: :failed

  defp prune_backoffs(data) do
    now = now()
    %{data | backoffs: Map.reject(data.backoffs, fn {reason, entry} -> prunable_at(data, reason, entry) <= now end)}
  end

  defp prunable_at(_data, _reason, %{failures: 0, next_at: next_at}), do: next_at
  defp prunable_at(data, reason, %{next_at: next_at}), do: next_at + pacing(data.opts, reason).cap

  defp start_load(%{in_flight: %{load: _}} = data, actions), do: {:keep_state, data, actions}

  defp start_load(data, actions) do
    {data, round_actions} = start_round(data, :load, [], MapSet.new())
    {:keep_state, data, actions ++ round_actions}
  end

  defp start_round(data, key, waiters, reasons) do
    {kind, request_opts, addresses} = round_request(data, key)
    {data, candidates} = Enum.reduce(addresses, {data, []}, &add_candidate/2)
    seq = data.seq + 1
    {spec, bound} = round_spec(data, kind, request_opts, Enum.reverse(candidates))
    ref = make_ref()
    store = self()
    {pid, mref} = spawn_monitor(fn -> send(store, {:round, ref, Round.run(spec)}) end)
    topics = MapSet.new(Keyword.get(request_opts, :topics, []))

    round = %{
      key: key,
      kind: kind,
      ref: ref,
      pid: pid,
      mref: mref,
      seq: seq,
      topics: topics,
      waiters: waiters,
      reasons: reasons
    }

    data = %{data | seq: seq, in_flight: Map.put(data.in_flight, key, round)}
    {data, [{{:timeout, {:round, ref}}, bound, nil}]}
  end

  defp round_spec(data, kind, request_opts, candidates) do
    budget = budget(kind, candidates, data.opts)
    bound = Round.bound(budget)

    spec =
      Map.merge(budget, %{
        request_opts: request_opts,
        known_missing: data.known_missing,
        brokers: Map.new(MetadataTable.brokers(data.table), &{{&1.host, &1.port}, &1}),
        deadline: now() + bound
      })

    {spec, bound}
  end

  defp budget(kind, candidates, opts) do
    %{
      kind: kind,
      candidates: candidates,
      ladder_delay: opts.ladder_delay,
      connect_slack: opts.connect_slack,
      request_slack: opts.request_slack,
      connect_timeout: Config.connect_timeout(),
      request_timeout: Config.request_timeout()
    }
  end

  defp round_request(data, :load) do
    {:load, metadata_opts(data, data.allow_default), data.bootstrap}
  end

  defp round_request(data, {:tracked, allow}), do: {:metadata, metadata_opts(data, allow), candidate_addresses(data)}

  defp round_request(data, {:coordinator, group}) do
    {:find_coordinator, [group_id: group], candidate_addresses(data)}
  end

  defp metadata_opts(data, allow) do
    [topics: MapSet.to_list(data.tracked), allow_auto_topic_creation: allow]
  end

  defp add_candidate(address, {data, candidates}) do
    case ensure_connection(data, address) do
      {data, nil} -> {data, candidates}
      {data, pid} -> {data, [{address, pid} | candidates]}
    end
  end

  defp take_round(data, match) do
    case Enum.find(Map.values(data.in_flight), match) do
      nil -> {nil, data}
      round -> {round, %{data | in_flight: Map.delete(data.in_flight, round.key)}}
    end
  end

  defp finish(round, result, state, data) do
    data = pace(data, round, result)
    {next_state, data, actions} = handle_result(round.kind, round, result, state, data)

    {:next_state, next_state, data,
     [{{:timeout, {:round, round.ref}}, :cancel} | actions ++ rearm(data, round.key) ++ sweep(data)]}
  end

  defp handle_result(:load, round, {:ok, result}, _state, data) do
    data = %{refreshed(data, round.seq, result) | current: result.address, versions: result.versions, failures: 0}
    ready = {:ok, %{table: data.table, api_versions: data.versions}}
    replies = for from <- data.boot_waiters, do: {:reply, from, ready}
    triggers = for {from, msg} <- data.load_waiters, do: {:next_event, {:call, from}, msg}
    {:ready, %{data | boot_waiters: [], load_waiters: []}, replies ++ [periodic(data) | triggers]}
  end

  defp handle_result(:load, _round, failure, :loading, data) do
    reason = reason(failure)
    boot = for from <- data.boot_waiters, do: {:reply, from, {:error, {:boot, boot_reason(failure)}}}
    loading = for {from, _msg} <- data.load_waiters, do: {:reply, from, {:error, {:loading, reason}}}
    data = %{data | boot_waiters: [], load_waiters: [], failures: data.failures + 1}
    {:loading, data, boot ++ loading ++ [{:state_timeout, backoff(data), :retry}]}
  end

  defp handle_result(:metadata, round, {:ok, result}, state, data) do
    data = refreshed(data, round.seq, result)
    replies = for from <- round.waiters, do: {:reply, from, {:ok, %{unknown: result.unknown}}}
    recovered(state, %{data | current: result.address, failures: 0}, replies)
  end

  defp handle_result(:find_coordinator, round, {:ok, %{coordinator: %FindCoordinator{} = coordinator}}, state, data) do
    {:coordinator, group} = round.key
    MetadataTable.put_coordinator(data.table, group, FindCoordinator.coordinator_node_id(coordinator))
    {state, data, for(from <- round.waiters, do: {:reply, from, :ok})}
  end

  defp handle_result(:find_coordinator, round, {:ok, %{coordinator: {:error, error}}}, state, data) do
    {state, data, for(from <- round.waiters, do: {:reply, from, {:error, error}})}
  end

  defp handle_result(_kind, round, failure, state, data) do
    reason = reason(failure)
    replies = for from <- round.waiters, do: {:reply, from, {:error, reason}}
    degraded(state, failure, %{data | failures: data.failures + 1}, replies)
  end

  # Only the boot waiter gets the exception itself, so the client can re-raise it; telemetry carries the module.
  defp reason({:raised, {_kind, exception, _stacktrace}}), do: {:raised, exception.__struct__}
  defp reason({:error, reason}), do: reason

  defp boot_reason({:raised, _raised} = raised), do: raised
  defp boot_reason(failure), do: reason(failure)

  # Only a :metadata success leaves :degraded: a coordinator answer does not prove metadata works.
  defp recovered(:degraded, data, replies) do
    Logger.info("Periodic metadata update recovered")
    duration = System.monotonic_time() - data.degraded_at
    Telemetry.emit_metadata_store_recovered(duration)
    {:ready, %{data | degraded_at: nil}, replies}
  end

  defp recovered(state, data, replies), do: {state, data, replies}

  defp degraded(:ready, failure, data, replies) do
    Logger.warning("Periodic metadata update failed: #{failure_text(failure)}")
    Telemetry.emit_metadata_store_degraded(reason(failure))
    data = %{data | degraded_at: System.monotonic_time()}
    {:degraded, data, replies ++ [{:state_timeout, backoff(data), :retry}]}
  end

  defp degraded(state, _failure, data, replies), do: {state, data, replies ++ [{:state_timeout, backoff(data), :retry}]}

  defp failure_text({:raised, {_kind, exception, _stacktrace}}) do
    "#{inspect(exception.__struct__)}: #{Exception.message(exception)}"
  end

  defp failure_text({:error, reason}), do: inspect(reason)

  defp backoff(data), do: delay(data.failures, data.opts.backoff_base, data.opts.backoff_cap)
  defp delay(failures, base, cap), do: Retry.backoff_delay_jittered(failures - 1, base, cap)

  defp periodic(data), do: {{:timeout, :periodic}, data.interval, nil}

  # A response the snapshot ignores says nothing about which topics are missing.
  defp refreshed(data, seq, result) do
    case apply_snapshot(data, seq, result.metadata, :tracked) do
      {:applied, data} -> note_missing(data, result.missing)
      {:ignored, data} -> data
    end
  end

  defp apply_snapshot(data, seq, metadata, scope) do
    case MetadataTable.apply(data.table, metadata, %{seq: seq, scope: scope}, data.freshness) do
      {:applied, freshness} ->
        send(data.client, {:kafka_ex_metadata_store, :swapped})
        {:applied, prune_freshness(stop_departed(%{data | freshness: freshness}))}

      :ignored ->
        {:ignored, data}
    end
  end

  defp sweep(data) do
    expiries =
      Map.values(data.tickets) ++ Enum.map(data.backoffs, fn {reason, entry} -> prunable_at(data, reason, entry) end)

    if expiries == [], do: [], else: [{{:timeout, :sweep}, Enum.min(expiries), nil, [abs: true]}]
  end

  # No response older than the oldest live seq can still arrive, so older freshness entries decide nothing.
  defp prune_freshness(data) do
    now = now()
    tickets = Map.filter(data.tickets, fn {_seq, expires_at} -> expires_at > now end)
    live = Map.keys(tickets) ++ Enum.map(Map.values(data.in_flight), & &1.seq)
    floor = Enum.min([data.seq + 1 | live])
    %{data | tickets: tickets, freshness: prune(data.freshness, floor)}
  end

  defp ticket_ttl(data) do
    per_attempt =
      Config.connect_timeout() + data.opts.connect_slack + Config.request_timeout() +
        data.opts.request_slack

    Client.retry_count() * per_attempt + @call_timeout
  end

  defp note_missing(data, []) do
    if MapSet.size(data.known_missing) > 0 do
      Logger.info("Kafka metadata recovered for topics #{inspect(Enum.sort(data.known_missing))}")
    end

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
    case Enum.map(MetadataTable.brokers(data.table), &{&1.host, &1.port}) do
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
        case Client.start_connection(data.supervisor, connection_args(data, address)) do
          {:ok, pid} -> {%{data | conns: Map.put(data.conns, address, %{pid: pid, ref: Process.monitor(pid)})}, pid}
          :restarting -> {data, nil}
        end
    end
  end

  defp connection_args(data, {host, port}) do
    data.connection
    |> Map.take([:use_ssl, :secrets, :reconnect_backoff])
    |> Map.merge(%{client: self(), host: host, port: port, role: :metadata})
  end

  defp stop_departed(data) do
    addresses = MapSet.new(MetadataTable.brokers(data.table), &{&1.host, &1.port})
    {departed, kept} = Enum.split_with(data.conns, fn {address, _conn} -> not MapSet.member?(addresses, address) end)

    Enum.each(departed, fn {_address, conn} ->
      Process.demonitor(conn.ref, [:flush])
      ConnectionStateMachine.stop(conn.pid, :metadata_update)
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
