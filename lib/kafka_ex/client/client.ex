defmodule KafkaEx.Client do
  @moduledoc """
  Kayrock-compatible KafkaEx.Server implementation

  This implementation attempts to keep as much Kafka 'business logic' as possible
  out of the server implementation, with the motivation that this should make
  the client easier to maintain as the Kafka protocol evolves.

  This implementation does, however, include implementations of all of the
  legacy KafkaEx.Server `handle_call` clauses so that it can be compatible with
  the legacy KafkaEx API.
  """

  alias KafkaEx.Config
  alias KafkaEx.Telemetry

  alias KafkaEx.Client.ConnectionStateMachine
  alias KafkaEx.Client.ConnectionStateMachine.ReconnectBackoff
  alias KafkaEx.Client.Error
  alias KafkaEx.Client.MetadataStore
  alias KafkaEx.Client.MetadataStore.MetadataTable
  alias KafkaEx.Client.MetadataStore.Round
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestBudget
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.ResponseParser
  alias KafkaEx.Client.State
  alias KafkaEx.Client.Supervisor, as: ClientSupervisor
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Messages.ConsumerGroupHeartbeat
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.Support.OptionalDeps
  alias KafkaEx.Support.Retry

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  use GenServer

  @type args :: [KafkaEx.worker_setting() | {:allow_auto_topic_creation, boolean}]
  @type call_budget :: {non_neg_integer(), integer()}

  @doc """
  Start the server in a supervision tree
  """
  @spec start_link(args, GenServer.name() | :no_name) :: GenServer.on_start()
  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args, nil])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], name: name)
  end

  @doc """
  Send a protocol request to the appropriate broker
  Broker metadata will be updated if necessary
  """
  @spec send_request(KafkaEx.API.client(), map, KafkaEx.Client.NodeSelector.t(), pos_integer | nil) ::
          {:ok, term} | {:error, term}
  def send_request(server, request, node_selector, timeout \\ nil) do
    call_timeout = timeout_val(timeout)
    opts = RequestBudget.put_call_budget([], call_timeout)
    GenServer.call(server, {:network_request, request, node_selector, opts}, call_timeout)
  end

  require Logger

  # The request retry budget. This is the single source of truth: KafkaEx.API's
  # fetch call_timeout budget (@fetch_max_retries) is derived from it via
  # retry_count/0, so the two can no longer drift by hand. See #357.
  @retry_count 3
  @no_bytes %{bytes_sent: 0, bytes_received: 0}
  @park :kafka_ex_park

  # JoinGroup/SyncGroup are sent send-once at the client (one attempt, no
  # transport-level socket retry): the broker legitimately holds these responses
  # for the rebalance/session window, so retrying the same socket is pointless,
  # and ConsumerGroup.Manager owns rejoin, retrying recoverable errors until they clear.
  # Matches Java/kafka-python/librdkafka/brod, which all send-once + rejoin higher up.
  @coordinator_max_attempts 1

  @store_call_timeout 5_000

  @doc false
  # The request retry budget (single source of truth for #357). KafkaEx.API
  # derives its fetch call_timeout from this so the two cannot drift.
  @spec retry_count() :: pos_integer()
  def retry_count, do: @retry_count

  @doc false
  # Exposed so KafkaEx.API can size the JoinGroup/SyncGroup call budget from it.
  @spec coordinator_max_attempts() :: pos_integer()
  def coordinator_max_attempts, do: @coordinator_max_attempts

  @impl true
  def init([args, _name]) do
    state = State.static_init(args)

    unless KafkaEx.valid_consumer_group?(state.consumer_group_for_auto_commit) do
      raise KafkaEx.InvalidConsumerGroupError,
            state.consumer_group_for_auto_commit
    end

    # Crash loudly at boot if the user's config implies an optional
    # dep that isn't loaded (e.g. :msk_iam SASL without :aws_signature,
    # or :snappy compression without :snappyer). Much friendlier than
    # UndefinedFunctionError at first produce/auth.
    :ok = OptionalDeps.validate!(state.auth)

    # One-time deprecation notice for the legacy :sync_timeout key (→ :request_timeout).
    :ok = Config.warn_deprecated_timeout_config()

    state = %{
      state
      | reconnect_backoff: ReconnectBackoff.new(),
        connection_options: ConnectionStateMachine.options()
    }

    {:ok, supervisor} = ClientSupervisor.start_link(%{store: store_args(state, args)})
    state = %{state | supervisor: supervisor}
    store = ClientSupervisor.metadata_store(supervisor)

    case await_boot(store, state) do
      {:ok, %{table: tid, api_versions: api_versions}} ->
        {:ok, %{state | api_versions: api_versions, store: %{pid: store, ref: Process.monitor(store), tid: tid}}}

      {:error, reason} ->
        stop_supervisor_after_failed_boot(supervisor, store)
        boot_failed!(reason)
    end
  end

  defp store_args(state, args) do
    %{
      client: self(),
      bootstrap: state.bootstrap_uris,
      initial_topics: Keyword.get(args, :initial_topics, []),
      interval: state.metadata_update_interval,
      allow_auto_topic_creation: state.allow_auto_topic_creation,
      connection: %{
        use_ssl: state.use_ssl,
        secrets: secrets_fun(state),
        reconnect_backoff: state.reconnect_backoff
      }
    }
  end

  defp await_boot(nil, _state), do: {:error, :no_store}

  defp await_boot(store, state) do
    timeout = MetadataStore.call_timeout(length(state.bootstrap_uris), :load)

    case MetadataStore.await_ready(store, timeout) do
      {:ok, ready} -> {:ok, ready}
      {:error, {:boot, reason}} -> {:error, reason}
    end
  catch
    :exit, reason -> {:error, {:await_ready, reason}}
  end

  # Never raises, so the boot's own error is the one raised.
  defp stop_supervisor_after_failed_boot(supervisor, store) do
    connections = Enum.map(boot_connections(supervisor), fn pid -> {pid, Process.monitor(pid)} end)
    stop_store(store)

    Enum.each(connections, fn {pid, ref} ->
      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
      after
        5_000 -> :ok
      end
    end)

    ClientSupervisor.stop(supervisor, :normal, 5_000)
  catch
    _kind, _reason -> :ok
  end

  defp stop_store(nil), do: :ok

  defp stop_store(store) do
    MetadataStore.stop(store, {:shutdown, :init_error})
  catch
    :exit, _reason -> :ok
  end

  defp boot_connections(supervisor) do
    case ClientSupervisor.connection_supervisor(supervisor) do
      nil ->
        []

      connections ->
        for {_id, pid, _type, _modules} <- DynamicSupervisor.which_children(connections), is_pid(pid), do: pid
    end
  end

  @spec boot_failed!(term()) :: no_return()
  defp boot_failed!({:api_versions, reason}), do: api_versions_failed!(reason)

  defp boot_failed!({:raised, {kind, reason, stacktrace}}) do
    sleep_for_reconnect()
    :erlang.raise(kind, reason, stacktrace)
  end

  defp boot_failed!(_reason) do
    sleep_for_reconnect()
    raise "Brokers sockets are not opened"
  end

  @spec api_versions_failed!(term()) :: no_return()
  defp api_versions_failed!(:parse_error) do
    sleep_for_reconnect()

    raise "ApiVersions negotiation failed: unable to parse broker response after #{Round.api_versions_max_retries()} retries. " <>
            "This may indicate network issues, broker instability, or protocol incompatibility."
  end

  defp api_versions_failed!(reason) do
    sleep_for_reconnect()

    raise "ApiVersions negotiation failed: #{inspect(reason)}. " <>
            "Check broker connectivity and network configuration."
  end

  # The caller's GenServer.call gives up after its budget, so the client stops waiting on the store before that.
  @impl true
  def handle_call(message, {pid, _tag} = from, state) do
    fallback = RequestBudget.deadline_budget(caller_budget(message))
    {message, deadline} = admit(message, node(pid) == node(), System.monotonic_time(:millisecond), fallback)
    serve(message, from, %{state | deadline: deadline})
  end

  defp serve(message, from, state) do
    case handle_client_call(message, from, state) do
      {:reply, reply, state} -> {:reply, reply, %{state | deadline: nil}}
      {:noreply, state} -> {:noreply, %{state | deadline: nil}}
    end
  catch
    :throw, {:caller_budget_spent, state} -> {:reply, budget_spent_reply(message), %{state | deadline: nil}}
  end

  # Each call keeps the timeout shape it has on its own path: the retry loop builds an %Error{}.
  defp budget_spent_reply({:network_request, _, _, _}), do: {:error, :timeout}
  defp budget_spent_reply(_message), do: {:error, Error.build(:timeout, %{})}

  @doc false
  @spec admit(term(), boolean(), integer(), non_neg_integer()) :: {term(), integer()}
  def admit({:metadata, topics, opts, api_version}, local?, now, default) when is_list(opts) do
    {budget, opts} = pop_budget(opts)
    {{:metadata, topics, opts, api_version}, deadline(budget, local?, now, default)}
  end

  # The old shape took any term as the creation flag and passed it on as given.
  def admit({:topic_metadata, topics, allow}, local?, now, default) when not is_list(allow) do
    admit({:topic_metadata, topics, [allow_topic_creation: allow]}, local?, now, default)
  end

  def admit({:network_request, request, selector}, local?, now, default) do
    admit({:network_request, request, selector, []}, local?, now, default)
  end

  def admit(message, local?, now, default) when is_tuple(message) and tuple_size(message) > 1 do
    last = tuple_size(message) - 1
    opts = elem(message, last)

    if is_list(opts) do
      {budget, opts} = pop_budget(opts)
      {put_elem(message, last, opts), deadline(budget, local?, now, default)}
    else
      {message, now + default}
    end
  end

  def admit(message, _local?, now, default), do: {message, now + default}

  defp pop_budget(opts) do
    case Enum.split_with(opts, &match?({:call_budget, _}, &1)) do
      {[{:call_budget, budget} | _], rest} -> {budget, rest}
      {[], rest} -> {nil, rest}
    end
  end

  # A send time is comparable only on the caller's own node, where every process shares one monotonic clock.
  @doc false
  @spec deadline(call_budget() | term(), boolean(), integer(), non_neg_integer()) :: integer()
  def deadline({ms, sent_at}, true, _now, _default) when is_integer(ms) and is_integer(sent_at), do: sent_at + ms
  def deadline({ms, sent_at}, false, now, _default) when is_integer(ms) and is_integer(sent_at), do: now + ms
  def deadline(_unusable, _local?, now, default), do: now + default

  # A spent budget ends the operation's span with the timeout error a timed-out request reports, not an exception.
  defp within_budget(stop_metadata, operation) do
    operation.()
  catch
    :throw, {:caller_budget_spent, state} -> {{{:error, Error.build(:timeout, %{})}, state}, stop_metadata}
  end

  # Mirrors the budgets KafkaEx.API and send_request/4 give, for a message that carries none.
  defp caller_budget(message) when is_tuple(message) and elem(message, 0) == :network_request do
    RequestBudget.call_budget(Config.request_timeout())
  end

  defp caller_budget(message) do
    per_attempt = message_network_timeout(message) || Config.request_timeout()
    RequestBudget.call_budget(per_attempt, budget_attempts(message))
  end

  @budgeted_by_network_timeout [
    :fetch,
    :heartbeat,
    :consumer_group_heartbeat,
    :join_group,
    :sync_group,
    :create_topics,
    :delete_topics
  ]

  defp message_network_timeout(message)
       when is_tuple(message) and elem(message, 0) in @budgeted_by_network_timeout do
    opts = elem(message, tuple_size(message) - 1)

    case Keyword.keyword?(opts) and Keyword.get(opts, :network_timeout) do
      timeout when is_integer(timeout) and timeout > 0 ->
        timeout

      absent when absent in [false, nil] ->
        nil

      invalid ->
        Logger.debug("Sizing the call budget from :request_timeout; :network_timeout is #{inspect(invalid)}")
        nil
    end
  end

  defp message_network_timeout(_message), do: nil

  defp budget_attempts({:join_group, _group, _member, _opts}), do: @coordinator_max_attempts
  defp budget_attempts({:sync_group, _group, _generation, _member, _opts}), do: @coordinator_max_attempts
  defp budget_attempts(_message), do: @retry_count

  defp handle_client_call({:cluster_metadata, _opts}, from, state) do
    handle_client_call(:cluster_metadata, from, state)
  end

  defp handle_client_call(:cluster_metadata, _from, state) do
    {view, state} = state |> ensure_store() |> cluster_metadata_view()
    {:reply, {:ok, view}, state}
  catch
    :throw, {:caller_budget_spent, state} -> {:reply, {:ok, state.last_view}, state}
  end

  defp handle_client_call(:correlation_id, _from, state) do
    {:reply, {:ok, state.correlation_id}, state}
  end

  defp handle_client_call(:update_metadata, _from, state) do
    {_result, state} = refresh(state, %{topics: [], allow_auto_topic_creation: state.allow_auto_topic_creation})
    {view, state} = cluster_metadata_view(state)
    {:reply, {:ok, view}, state}
  catch
    :throw, {:caller_budget_spent, state} -> {:reply, {:ok, state.last_view}, state}
  end

  defp handle_client_call(
         {:set_consumer_group_for_auto_commit, consumer_group},
         _from,
         state
       ) do
    if KafkaEx.valid_consumer_group?(consumer_group) do
      {:reply, :ok, %{state | consumer_group_for_auto_commit: consumer_group}}
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  defp handle_client_call({:topic_metadata, topics, opts}, _from, state) do
    allow_topic_creation = Keyword.get(opts, :allow_topic_creation, false)
    {result, state} = refresh(state, %{topics: topics, allow_auto_topic_creation: allow_topic_creation})
    {:reply, topics_reply(result, topics, cached_topics(state, topics)), state}
  catch
    :throw, {:caller_budget_spent, state} ->
      {:reply, topics_reply({:error, :timeout}, topics, cached_topics(state, topics)), state}
  end

  defp handle_client_call({:network_request, request, node_selector, _opts}, _from, state) do
    {response, updated_state} = network_request(request, node_selector, state, :as_is)

    {:reply, response, updated_state}
  end

  defp handle_client_call(:consumer_group, _from, state) do
    {:reply, state.consumer_group_for_auto_commit, state}
  end

  defp handle_client_call(message, from, state) do
    case request_spec(message) do
      {:error, _reason} = error ->
        {:reply, error, state}

      %{api: :fetch} = spec ->
        fetch_reply(fetch(spec, message, state), from)

      spec ->
        {result, state} = perform(message, spec, state)
        {:reply, result, state}
    end
  end

  defp fetch_reply({:parked, parked}, from), do: {:noreply, park(parked, from)}
  defp fetch_reply({result, %State{} = state}, _from), do: {:reply, result, %{state | parking: nil}}

  @impl true
  def handle_info({:timeout, _timer, {:parked, tag}}, state) when is_map_key(state.parked, tag) do
    {:noreply, resume(tag, {:error, :timeout, @no_bytes}, state)}
  end

  def handle_info(message, %State{parked: parked} = state) when map_size(parked) > 0 do
    case Enum.find_value(parked, &answered(message, &1)) do
      nil -> unparked_info(message, state)
      {tag, outcome} -> {:noreply, resume(tag, outcome, state)}
    end
  end

  # Defining any handle_info/2 replaces the catch-all use GenServer injects; without one an
  # unmatched message would crash the client.
  def handle_info(message, state), do: unparked_info(message, state)

  defp unparked_info({:kafka_ex_metadata_store, :swapped}, state) do
    {:noreply, state |> ensure_store() |> stop_departed_connections()}
  end

  defp unparked_info({:DOWN, ref, :process, _pid, _reason}, %State{store: %{ref: ref}} = state) do
    {:noreply, %{state | store: nil, hints: %{}}}
  end

  defp unparked_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, %{state | conns: Map.reject(state.conns, fn {_key, entry} -> entry.ref == ref end)}}
  end

  defp unparked_info(message, state) do
    Logger.warning("#{inspect(__MODULE__)} ignoring unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  @impl true
  def terminate(reason, state) do
    Logger.debug("Shutting down worker #{inspect(worker_identity())}, reason: #{inspect(reason)}")
    for {_tag, entry} <- state.parked, do: Telemetry.exception_span(entry.span, entry.metadata, :exit, reason, [])
    ClientSupervisor.stop(state.supervisor, :shutdown, 5_000)
  end

  defp worker_identity do
    case Process.info(self(), :registered_name) do
      {:registered_name, []} -> self()
      {:registered_name, name} -> name
    end
  end

  # An empty broker list means the table is gone, not that every broker departed.
  defp stop_departed_connections(state) do
    case brokers(state) do
      [] -> state
      brokers -> stop_unadvertised(state, MapSet.new(brokers, &{&1.host, &1.port}))
    end
  end

  defp stop_unadvertised(state, advertised) do
    {departed, kept} =
      Enum.split_with(state.conns, fn {{host, port, _role}, _entry} -> not MapSet.member?(advertised, {host, port}) end)

    Enum.each(departed, fn {_key, %{pid: pid, ref: ref}} ->
      Process.demonitor(ref, [:flush])
      ConnectionStateMachine.stop(pid, :metadata_update)
    end)

    %{state | conns: Map.new(kept)}
  end

  defp ensure_store(%State{store: nil} = state) do
    {_budget, timeout} = budgeted(state, @store_call_timeout)

    with store when is_pid(store) <- ClientSupervisor.metadata_store(state.supervisor),
         {:ok, tid} <- MetadataStore.table(store, timeout) do
      %{state | store: %{pid: store, ref: Process.monitor(store), tid: tid}}
    else
      _none -> state
    end
  catch
    :exit, _reason -> state
  end

  defp ensure_store(state), do: state

  defp drop_store(%State{store: nil} = state), do: state

  defp drop_store(%State{store: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    %{state | store: nil}
  end

  defp with_store(state, unavailable, wanted, call) do
    case ensure_store(state) do
      %State{store: nil} = state -> {unavailable, state}
      %State{store: store} = state -> call_store(state, store, unavailable, budgeted(state, wanted.(store)), call)
    end
  end

  defp budgeted(%State{deadline: nil}, wanted), do: {:full, wanted}

  defp budgeted(%State{deadline: deadline} = state, wanted) do
    case deadline - System.monotonic_time(:millisecond) do
      left when left <= 0 -> throw({:caller_budget_spent, state})
      left when left < wanted -> {:capped, left}
      _left -> {:full, wanted}
    end
  end

  defp call_store(state, store, unavailable, {budget, timeout}, call) do
    {call.(store, timeout), state}
  catch
    :exit, {:timeout, _call} when budget == :capped -> throw({:caller_budget_spent, state})
    :exit, _reason -> {unavailable, drop_store(state)}
  end

  defp short(_store), do: @store_call_timeout

  # A failed refresh reads as a timeout; a version refusal keeps the atom the client's request path returns.
  defp refresh(state, refresh) do
    with_store(state, {:error, :timeout}, &round_call_timeout(state, &1), fn store, timeout ->
      case MetadataStore.refresh(store.pid, refresh, timeout) do
        {:ok, _result} = ok -> ok
        {:error, :api_version_no_supported} = refused -> refused
        {:error, _reason} -> {:error, :timeout}
      end
    end)
  end

  defp topics_reply({:ok, %{unknown: unknown}}, topics, cached) do
    if Enum.any?(topics, &(&1 in unknown)), do: {:error, :unknown_topic_or_partition}, else: {:ok, cached}
  end

  defp topics_reply({:error, _reason} = error, topics, cached) do
    if length(cached) == length(Enum.uniq(topics)), do: {:ok, cached}, else: error
  end

  defp rediscover_coordinator(state, group) do
    with_store(state, {:error, :timeout}, &round_call_timeout(state, &1), fn store, timeout ->
      MetadataStore.coordinator(store.pid, group, timeout)
    end)
  end

  # An empty snapshot means the store is loading, and a refresh then waits for its load round.
  defp round_call_timeout(state, store) do
    case MetadataTable.brokers(store.tid) do
      [] -> MetadataStore.call_timeout(length(state.bootstrap_uris), :load)
      brokers -> MetadataStore.call_timeout(length(brokers))
    end
  end

  defp select(%State{store: nil}, _selector), do: {:error, :unavailable}
  defp select(%State{store: %{tid: tid}, hints: hints}, selector), do: MetadataTable.select(tid, selector, hints)

  defp brokers(%State{store: nil}), do: []
  defp brokers(%State{store: %{tid: tid}}), do: MetadataTable.brokers(tid)

  defp cached_topics(%State{store: nil}, _topics), do: []

  defp cached_topics(%State{store: %{tid: tid}}, topics) do
    case MetadataTable.topics(tid, topics) do
      {:ok, cached} -> cached
      {:error, :unavailable} -> []
    end
  end

  defp cluster_metadata_view(%State{store: nil} = state), do: {state.last_view, state}

  defp cluster_metadata_view(%State{store: %{tid: tid}} = state) do
    case with_store(state, :unavailable, &short/1, &MetadataStore.snapshot(&1.pid, &2)) do
      {{:ok, metadata, counter}, state} -> remember_view(state, metadata, {tid, counter})
      {:unavailable, state} -> {state.last_view, state}
    end
  end

  # A booted snapshot always has a broker, so an empty one is a restarted store still loading.
  defp remember_view(state, %ClusterMetadata{brokers: brokers}, _stamp) when map_size(brokers) == 0 do
    {state.last_view, state}
  end

  defp remember_view(state, metadata, stamp) do
    view = with_hints(metadata, state.hints, stamp)
    {view, %{state | last_view: view}}
  end

  defp with_hints(metadata, hints, stamp), do: %{metadata | preferred_replicas: valid_hints(hints, stamp)}

  defp valid_hints(hints, stamp) do
    for {partition, {node_id, ^stamp}} <- hints, into: %{}, do: {partition, node_id}
  end

  defp snapshot_stamp(%State{store: nil}), do: nil

  defp snapshot_stamp(%State{store: %{tid: tid}}) do
    case MetadataTable.counter(tid) do
      {:ok, counter} -> {tid, counter}
      {:error, :unavailable} -> nil
    end
  end

  defp store_tid(%State{store: nil}), do: nil
  defp store_tid(%State{store: %{tid: tid}}), do: tid

  defp with_topic_view(state, nil, _topics), do: %{state | cluster_metadata: %ClusterMetadata{}}
  defp with_topic_view(state, tid, topics), do: %{state | cluster_metadata: MetadataTable.topic_view(tid, topics)}

  @typep request_spec :: %{
           api: atom(),
           route: NodeSelector.t(),
           opts: Keyword.t(),
           parse: (term() -> {:ok, term()} | {:error, term()}),
           retryable?: (atom() -> boolean()),
           send_once?: (atom() -> boolean()),
           rediscover_coordinator?: boolean(),
           network_timeout: pos_integer() | nil,
           store_tid: :ets.tid() | nil
         }

  @group_requests [
    :offset_fetch,
    :offset_commit,
    :heartbeat,
    :join_group,
    :leave_group,
    :sync_group,
    :consumer_group_heartbeat
  ]

  @spec request_spec(tuple()) :: request_spec() | {:error, atom()}
  defp request_spec(message) do
    if valid_group?(message), do: spec_for(message), else: {:error, :invalid_consumer_group}
  end

  defp valid_group?({:describe_groups, [group], _opts}), do: KafkaEx.valid_consumer_group?(group)
  defp valid_group?({:consumer_group_describe, [group], _opts}), do: valid_group_id?(group)
  defp valid_group?(message) when elem(message, 0) in @group_requests, do: valid_group_id?(elem(message, 1))
  defp valid_group?(_message), do: true

  defp valid_group_id?(group), do: KafkaEx.valid_consumer_group?(group) and is_binary(group)

  defp spec_for({:api_versions, opts}), do: spec(:api_versions, NodeSelector.random(), opts)

  defp spec_for({:metadata, topics, opts, _api_version}) do
    spec(:metadata, NodeSelector.random(), [{:topics, topics} | opts], parse: &metadata_with_topic_errors/1)
  end

  defp spec_for({:list_offsets, [{topic, [%{partition_num: partition}] = partitions}], opts}) do
    spec(:list_offsets, NodeSelector.topic_partition(topic, partition), [{:topics, [{topic, partitions}]} | opts])
  end

  # Backward compatibility, to be deleted once we delete legacy code
  defp spec_for({:offset, topic, partition, timestamp}) do
    spec_for({:list_offsets, [{topic, [%{partition_num: partition, timestamp: timestamp}]}], []})
  end

  defp spec_for({:describe_groups, [group], opts}) do
    spec(:describe_groups, NodeSelector.consumer_group(group), [{:group_names, [group]} | opts])
  end

  defp spec_for({:list_groups, node_id, opts}), do: spec(:list_groups, NodeSelector.node_id(node_id), opts)

  defp spec_for({:offset_fetch, group, [{topic, partitions}], opts}) do
    opts = [{:group_id, group}, {:topics, [{topic, partitions}]} | opts]
    spec(:offset_fetch, NodeSelector.consumer_group(group), opts)
  end

  defp spec_for({:offset_commit, group, [{topic, partitions}], opts}) do
    opts = [{:group_id, group}, {:topics, [{topic, partitions}]} | opts]
    spec(:offset_commit, NodeSelector.consumer_group(group), opts, retryable?: &Retry.commit_retryable?/1)
  end

  defp spec_for({:heartbeat, group, member_id, generation_id, opts}) do
    opts = [{:group_id, group}, {:member_id, member_id}, {:generation_id, generation_id} | opts]
    spec(:heartbeat, NodeSelector.consumer_group(group), opts, retryable?: &Retry.heartbeat_retryable?/1)
  end

  defp spec_for({:join_group, group, member_id, opts}) do
    opts = [{:group_id, group}, {:member_id, member_id} | opts]
    spec(:join_group, NodeSelector.consumer_group(group), opts, retryable?: &Retry.join_group_retryable?/1)
  end

  defp spec_for({:leave_group, group, member_id, opts}) do
    spec(:leave_group, NodeSelector.consumer_group(group), [{:group_id, group}, {:member_id, member_id} | opts])
  end

  defp spec_for({:sync_group, group, generation_id, member_id, opts}) do
    opts = [{:group_id, group}, {:generation_id, generation_id}, {:member_id, member_id} | opts]
    spec(:sync_group, NodeSelector.consumer_group(group), opts, retryable?: &Retry.sync_group_retryable?/1)
  end

  # Validated before the telemetry span opens, so a malformed field is an error with no event.
  defp spec_for({:consumer_group_heartbeat, group, opts}) do
    with :ok <- RequestBuilder.validate_consumer_group_heartbeat_opts(opts) do
      spec(:consumer_group_heartbeat, NodeSelector.consumer_group(group), [{:group_id, group} | opts],
        retryable?: &Retry.consumer_group_heartbeat_retryable?/1,
        send_once?: heartbeat_send_once(Keyword.fetch!(opts, :member_epoch)),
        rediscover_coordinator?: true
      )
    end
  end

  defp spec_for({:consumer_group_describe, [group], opts}) do
    spec(:consumer_group_describe, NodeSelector.consumer_group(group), [{:group_ids, [group]} | opts])
  end

  defp spec_for({:produce, topic, partition, messages, opts}) do
    with {:ok, acks} <- resolve_acks(opts) do
      opts = [{:topic, topic}, {:partition, partition}, {:messages, messages} | Keyword.put(opts, :acks, acks)]
      spec(:produce, NodeSelector.topic_partition(topic, partition), opts, retryable?: &Retry.produce_retryable?/1)
    end
  end

  defp spec_for({:fetch, topic, partition, offset, opts}) do
    opts = [{:topic, topic}, {:partition, partition}, {:offset, offset} | opts]
    spec(:fetch, NodeSelector.topic_partition_replica(topic, partition), opts)
  end

  defp spec_for({:find_coordinator, group_id, opts}) do
    spec(:find_coordinator, NodeSelector.first_available(), [{:group_id, group_id} | opts])
  end

  defp spec_for({api, topics, timeout, opts}) when api in [:create_topics, :delete_topics] do
    spec(api, NodeSelector.controller(), [{:topics, topics}, {:timeout, timeout} | opts])
  end

  # :network_timeout is the per-attempt socket deadline, not a protocol field, for the calls budgeted by it.
  defp spec(api, route, opts, overrides \\ []) do
    {network_timeout, opts} =
      if api in @budgeted_by_network_timeout, do: Keyword.pop(opts, :network_timeout), else: {nil, opts}

    Map.merge(
      %{
        api: api,
        route: route,
        opts: opts,
        parse: &@protocol.parse_response(api, &1),
        retryable?: &Retry.data_plane_retryable?/1,
        send_once?: &Retry.transport_timeout?/1,
        rediscover_coordinator?: false,
        network_timeout: network_timeout,
        store_tid: nil
      },
      Map.new(overrides)
    )
  end

  @topic_id_requests [:offset_fetch, :offset_commit, :consumer_group_heartbeat]

  # After validation, inside the span; one table gives the request its topic ids and the response its topic names.
  defp attach_store(%{api: api} = spec, state) when api in @topic_id_requests do
    state = ensure_store(state)
    resolver = topic_name_parse_opts(store_tid(state))
    {%{spec | store_tid: store_tid(state), parse: &@protocol.parse_response(api, &1, resolver)}, state}
  end

  defp attach_store(spec, state), do: {spec, state}

  defp topic_name_parse_opts(nil), do: [topic_name_resolver: fn _topic_id -> {:error, :no_such_topic} end]
  defp topic_name_parse_opts(tid), do: [topic_name_resolver: &MetadataTable.topic_name_by_id(tid, &1)]

  # Only a member heartbeat (epoch > 0) is incremental and fenced when
  # resent after the broker bumped the epoch. A join (0) and a leave (-1,
  # -2) are accepted again, so the client keeps retrying them.
  defp heartbeat_send_once(epoch) when epoch > 0, do: &Retry.consumer_group_heartbeat_send_once?/1
  defp heartbeat_send_once(_epoch), do: &Retry.transport_timeout?/1

  # Reject a bad acks value here; unvalidated it reaches Kayrock's int16 encoder and crashes the client.
  defp resolve_acks(opts) do
    case Keyword.get(opts, :acks, Keyword.get(opts, :required_acks, -1)) do
      acks when acks in [-1, 0, 1] -> {:ok, acks}
      # Java/librdkafka spelling for "all in-sync replicas"; accept it as the -1 the wire carries.
      acks when acks in [:all, :any] -> {:ok, -1}
      _ -> {:error, :invalid_acks}
    end
  end

  defp perform({:offset_commit, group, [{topic, partitions}], _opts}, spec, state) do
    metadata = Telemetry.commit_metadata(group, Config.client_id(), topic, length(partitions))
    in_span([:kafka_ex, :consumer, :commit], metadata, fn -> {run(spec, state), metadata} end)
  end

  defp perform({:heartbeat, group, member_id, generation_id, _opts}, spec, state) do
    metadata = Telemetry.heartbeat_metadata(group, member_id, generation_id)
    in_span([:kafka_ex, :consumer, :heartbeat], metadata, fn -> {run(spec, state), metadata} end)
  end

  defp perform({:leave_group, group, member_id, _opts}, spec, state) do
    metadata = Telemetry.leave_group_metadata(group, member_id)
    in_span([:kafka_ex, :consumer, :leave], metadata, fn -> {run(spec, state), metadata} end)
  end

  defp perform({:join_group, group, member_id, opts}, spec, state) do
    metadata = Telemetry.join_group_metadata(group, member_id || "", Keyword.get(opts, :topics, []))
    in_span([:kafka_ex, :consumer, :join], metadata, fn -> join(spec, state, metadata) end)
  end

  defp perform({:sync_group, group, generation_id, member_id, opts}, spec, state) do
    # Leader sends non-empty group_assignment, followers send empty
    is_leader = Keyword.get(opts, :group_assignment, []) != []
    metadata = Telemetry.sync_group_metadata(group, member_id, generation_id, is_leader)

    in_span([:kafka_ex, :consumer, :sync], metadata, fn ->
      case run(spec, state, @coordinator_max_attempts) do
        {{:ok, result}, _state} = ok -> {ok, Map.put(metadata, :assigned_partitions, assigned_partitions(result))}
        error_result -> {error_result, metadata}
      end
    end)
  end

  defp perform({:consumer_group_heartbeat, group, opts}, spec, state) do
    member_id = Keyword.get(opts, :member_id) || ""
    metadata = Telemetry.consumer_group_heartbeat_metadata(group, member_id, Keyword.fetch!(opts, :member_epoch))

    in_span([:kafka_ex, :consumer, :consumer_group_heartbeat], metadata, fn ->
      member_heartbeat(spec, state, metadata)
    end)
  end

  defp perform({:produce, topic, partition, messages, _opts}, spec, state) do
    metadata = Telemetry.produce_metadata(topic, partition, Config.client_id(), Keyword.fetch!(spec.opts, :acks))

    Telemetry.span([:kafka_ex, :produce], Map.put(metadata, :message_count, length(messages)), fn ->
      within_budget(metadata, fn -> produce(spec, state, metadata) end)
    end)
  end

  defp perform({:metadata, topics, _opts, _api_version}, spec, state) do
    topic_list = if is_list(topics), do: topics, else: [topics]
    metadata = Telemetry.metadata_update_metadata(Config.client_id(), topic_list)

    Telemetry.span([:kafka_ex, :metadata, :update], metadata, fn ->
      within_budget(%{}, fn -> metadata_request(spec, state) end)
    end)
  end

  defp perform(_message, spec, state), do: run(spec, state)

  defp in_span(event, metadata, run), do: Telemetry.span(event, metadata, fn -> within_budget(metadata, run) end)

  defp join(spec, state, metadata) do
    case run(spec, state, @coordinator_max_attempts) do
      {{:ok, result}, _state} = ok ->
        {ok,
         Map.merge(metadata, %{generation_id: result.generation_id, is_leader: result.leader_id == result.member_id})}

      # KIP-394: a v4+ join without a member id is answered with one to join again with.
      {{:error, %Error{error: :member_id_required, metadata: %{member_id: assigned_id}}}, state} ->
        Logger.info("JoinGroup returned member_id_required (KIP-394), retrying with assigned member_id")
        join(%{spec | opts: Keyword.put(spec.opts, :member_id, assigned_id)}, state, metadata)

      error_result ->
        {error_result, metadata}
    end
  end

  defp assigned_partitions(result), do: Enum.reduce(result.partition_assignments, 0, &(length(&1.partitions) + &2))

  defp member_heartbeat(spec, state, metadata) do
    {spec, state} = attach_store(spec, state)

    case build(spec, state) do
      {:ok, request} ->
        # KIP-1082: a v1+ join gets its member id at build; report and rebuild with it, so a retry keeps one member.
        metadata = %{metadata | member_id: request.member_id}
        spec = %{spec | opts: Keyword.put(spec.opts, :member_id, request.member_id)}

        case retry(spec, request, state, @retry_count, nil) do
          {{:ok, result}, _state} = ok -> {ok, Map.merge(metadata, heartbeat_result_metadata(result))}
          error_result -> {error_result, metadata}
        end

      error ->
        {{error, state}, metadata}
    end
  end

  defp heartbeat_result_metadata(result) do
    %{
      member_id: result.member_id,
      new_member_epoch: result.member_epoch,
      assigned_partitions: ConsumerGroupHeartbeat.assigned_partition_count(result)
    }
  end

  defp produce(spec, state, metadata) do
    case build(spec, state) do
      {:ok, %{acks: 0} = request} -> {fire_and_forget(request, spec, state), metadata}
      {:ok, request} -> spec |> retry(request, state, @retry_count, nil) |> with_offset(metadata)
      error -> {{error, state}, metadata}
    end
  end

  defp with_offset({{:ok, %{base_offset: offset}}, _state} = ok, metadata) when offset != nil do
    {ok, Map.put(metadata, :offset, offset)}
  end

  defp with_offset(result, metadata), do: {result, metadata}

  # acks=0: the broker sends no response, and a resend would duplicate records.
  defp fire_and_forget(request, %{route: %NodeSelector{topic: topic, partition: partition}} = spec, state) do
    case network_request(request, spec.route, state, builder(spec)) do
      {{:ok, :no_response}, state} ->
        {{:ok, RecordMetadata.build(topic: topic, partition: partition, base_offset: nil)}, state}

      {{:error, reason}, state} ->
        Logger.warning("Fire-and-forget produce to #{topic}/#{partition} failed with #{inspect(reason)}")
        {{:error, build_transport_error(reason)}, state}
    end
  end

  defp fetch(spec, {:fetch, topic, partition, offset, _opts}, state) do
    metadata = Telemetry.fetch_metadata(topic, partition, offset, Config.client_id())
    span = Telemetry.start_span([:kafka_ex, :fetch], metadata)
    run = fn state -> within_budget(metadata, fn -> fetched(spec, offset, state, metadata) end) end
    spanned(span, metadata, %{state | parking: :first}, run)
  end

  # :telemetry.span/3 split, so a call parked on its connection's first connect keeps one open span.
  defp spanned(span, metadata, state, run) do
    Telemetry.stop_span(span, run.(state))
  catch
    :throw, {@park, key, pid, dispatch, parked_state} ->
      resume = &spanned(span, metadata, &1, run)

      {:parked,
       %{key: key, pid: pid, dispatch: dispatch, state: parked_state, run: resume, span: span, metadata: metadata}}

    kind, reason ->
      Telemetry.exception_span(span, metadata, kind, reason, __STACKTRACE__)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp fetched(spec, offset, state, metadata) do
    with {:ok, request} <- build(spec, state),
         {{:ok, result}, state} <- fetch_with_leader_fallback(spec, request, state) do
      filtered_result = Fetch.filter_from_offset(result, offset)
      stop_metadata = Map.put(metadata, :message_count, length(Map.get(filtered_result, :records, [])))
      {{{:ok, filtered_result}, cache_preferred_replica(state, spec.route, filtered_result)}, stop_metadata}
    else
      {:error, _reason} = error -> {{error, state}, metadata}
      error_result -> {error_result, metadata}
    end
  end

  # KIP-392: a replica's error is not authoritative (it may lag the leader), so the leader decides.
  defp fetch_with_leader_fallback(spec, request, state) do
    case retry(spec, request, state, @retry_count, nil) do
      {{:error, error}, state_out} = result ->
        if reading_from_replica?(state_out, spec.route) do
          after_replica_error(spec, request, error, forget_preferred_replica(state_out, spec.route))
        else
          result
        end

      result ->
        result
    end
  end

  # A single attempt keeps the caller's call budget, which covers one retry cycle.
  defp after_replica_error(spec, request, error, state) do
    if Retry.data_plane_retryable?(extract_error_atom(error)) do
      {{:error, error}, state}
    else
      retry(spec, request, state, 1, nil)
    end
  end

  defp forget_preferred_replica(state, %NodeSelector{topic: topic, partition: partition}) do
    %{state | hints: Map.delete(state.hints, {topic, partition})}
  end

  defp reading_from_replica?(%State{store: nil}, _selector), do: false

  defp reading_from_replica?(%State{store: %{tid: tid}, hints: hints}, selector) do
    MetadataTable.select(tid, selector, hints) != MetadataTable.select(tid, selector, %{})
  end

  # A -1 replica means "stay" (Java); the stamp, read before the send, lets a later swap invalidate the hint.
  defp cache_preferred_replica(
         %State{dispatch_stamp: {_tid, _counter} = stamp} = state,
         %NodeSelector{topic: topic, partition: partition},
         %Fetch{preferred_read_replica: node_id}
       )
       when is_integer(node_id) and node_id != -1 do
    %{state | hints: Map.put(state.hints, {topic, partition}, {node_id, stamp})}
  end

  defp cache_preferred_replica(state, _route, _result), do: state

  # The seq is taken before the request, so a refresh issued after it wins over this answer.
  defp metadata_request(spec, state) do
    case build(spec, state) do
      {:ok, request} ->
        {seq, state} = with_store(state, nil, &short/1, &MetadataStore.next_seq(&1.pid, &2))

        case retry(spec, request, state, @retry_count, nil) do
          {{:ok, {returned, topic_errors}}, state} ->
            state = merge_cluster_metadata(state, seq, returned, request)
            stop_metadata = %{broker_count: map_size(returned.brokers), topic_count: map_size(returned.topics)}
            {{named_topics_reply(request, returned, topic_errors), state}, stop_metadata}

          error_result ->
            {error_result, %{}}
        end

      error ->
        {{error, state}, %{}}
    end
  end

  defp metadata_with_topic_errors(response) do
    with {:ok, metadata} <- @protocol.parse_response(:metadata, response) do
      {:ok, {metadata, ResponseParser.metadata_topic_errors(response)}}
    end
  end

  # A named topic the broker does not know fails the call; the topics it did return are cached all the same.
  defp named_topics_reply(request, metadata, topic_errors) do
    unknown = for {topic, :unknown_topic_or_partition} <- topic_errors, do: topic

    if all_topics_request?(request) or unknown == [] do
      {:ok, metadata}
    else
      {:error, Error.build(:unknown_topic_or_partition, %{topics: Enum.sort(unknown)})}
    end
  end

  defp merge_cluster_metadata(state, nil, _metadata, _request), do: state

  defp merge_cluster_metadata(state, seq, %ClusterMetadata{} = metadata, request) do
    scope = merge_scope(request, metadata)
    state |> with_store(:ok, &short/1, &MetadataStore.merge(&1.pid, seq, metadata, scope, &2)) |> elem(1)
  end

  # A full request (no topics filter) lists every topic, so it replaces the
  # topic map (dropping deleted topics); a targeted one only adds or updates.
  defp merge_scope(request, metadata) do
    if all_topics_request?(request), do: :all, else: {:topics, Map.keys(metadata.topics)}
  end

  # Matches the built request. "All topics" is a null topic list from V1 on
  # (RequestHelpers.build_topics_list/1 sends both `nil` and `[]` as null), but
  # an empty list in V0, whose topic array is not nullable.
  defp all_topics_request?(%{topics: nil}), do: true
  defp all_topics_request?(%Kayrock.Metadata.V0.Request{topics: []}), do: true
  defp all_topics_request?(_request), do: false

  # ----------------------------------------------------------------------------------------------------
  # The request is built once; a connection that refuses its version gets it rebuilt by builder/1.
  @spec run(request_spec(), State.t(), pos_integer()) :: {{:ok, term()} | {:error, term()}, State.t()}
  defp run(spec, state, attempts \\ @retry_count) do
    {spec, state} = attach_store(spec, state)

    case build(spec, state) do
      {:ok, request} -> retry(spec, request, state, attempts, nil)
      error -> {error, state}
    end
  end

  defp build(spec, state), do: RequestBuilder.build(spec.api, spec.opts, topic_view(spec, state))

  defp builder(spec), do: &build(spec, &1)

  defp topic_view(%{api: api, opts: opts, store_tid: tid}, state) when api in [:offset_fetch, :offset_commit] do
    topics = for {topic, _partitions} <- Keyword.fetch!(opts, :topics), do: topic
    with_topic_view(state, tid, topics)
  end

  defp topic_view(_spec, state), do: state

  defp retry(_spec, _request, state, 0, last_error), do: {{:error, last_error}, state}

  defp retry(spec, request, state, attempts, _last_error) do
    {%sent{}, outcome} = attempt_request(request, builder(spec), spec.route, state, spec.network_timeout)

    case outcome do
      {:refused, refused, state_out} ->
        {refused, state_out}

      {:raised, {:error, reason}, state_out} ->
        final = %{spec | retryable?: fn _error -> false end, send_once?: fn _error -> false end}
        handle_request_error(final, request, state_out, attempts, build_transport_error(reason), sent)

      {{:ok, response}, state_out} ->
        case spec.parse.(response) do
          {:ok, result} ->
            {{:ok, result}, state_out}

          {:error, [error | _]} ->
            handle_request_error(spec, request, state_out, attempts, error, sent)

          {:error, %Error{} = error} ->
            handle_request_error(spec, request, state_out, attempts, error, sent)
        end

      # The same refusal as an unwritten one, met when a reconnect lands between the await and the write.
      {{:error, :api_version_no_supported}, _state_out} = refused ->
        refused

      {{:error, reason}, state_out} ->
        handle_request_error(spec, request, state_out, attempts, build_transport_error(reason), sent)
    end
  end

  defp build_transport_error(reason) when is_atom(reason), do: Error.build(reason, %{})
  # A non-atom reason (SSL {:tls_alert, _}) would collapse to :unknown, which the fetch loop treats
  # as fatal; tag it retryable instead.
  defp build_transport_error(reason), do: Error.build(:transport_error, %{transport_reason: reason})

  defp handle_request_error(spec, request, state, attempts, error, request_name) do
    error_atom = extract_error_atom(error)
    Logger.warning("Request #{inspect(request_name)} failed with error #{inspect(error_atom)}")

    cond do
      # Send-once only for coordinator requests (broker holds them for the rebalance/
      # session window; rejoin/commit re-issue higher up). Data-plane retries via spec.retryable?.
      spec.send_once?.(error_atom) and coordinator_request?(spec.route) ->
        Logger.info(
          "Coordinator request #{inspect(request_name)} failed with #{inspect(error_atom)}; " <>
            "not retrying at the client (send-once)"
        )

        {{:error, error}, act_on_error(error_atom, spec, state)}

      # A stale topic id (OffsetCommit/OffsetFetch v10+, Metadata by id) is baked
      # into the built request, so resending the same request cannot succeed.
      # Refresh metadata and return: a caller that retries (e.g. GenConsumer's
      # commit retry) rebuilds the request from the fresh ids.
      error_atom == :unknown_topic_id ->
        {{:error, error}, act_on_error(error_atom, spec, state)}

      spec.retryable?.(error_atom) ->
        # A retry has written or failed an attempt, so it never parks.
        state = %{act_on_error(error_atom, spec, state) | parking: nil}
        retry(spec, request, state, attempts - 1, error)

      true ->
        Logger.info("Error #{inspect(error_atom)} is not retryable for #{inspect(request_name)}, failing immediately")
        {{:error, error}, state}
    end
  end

  defp extract_error_atom(%Error{error: error}), do: error
  defp extract_error_atom(error) when is_atom(error), do: error
  defp extract_error_atom({_group_id, error}) when is_atom(error), do: error
  defp extract_error_atom(_), do: :unknown

  defp act_on_error(error_atom, spec, state) do
    actions =
      ClusterMetadata.error_actions(error_atom, spec.route,
        rediscover_coordinator?: spec.rediscover_coordinator?,
        allow_auto_topic_creation: state.allow_auto_topic_creation
      )

    log_error_actions(error_atom, actions)
    run_cluster_actions(actions, state)
  end

  defp log_error_actions(_error_atom, []), do: :ok

  defp log_error_actions(error_atom, [{:refresh_metadata, _} | rest]) do
    Logger.info("Refreshing metadata due to #{inspect(error_atom)} error")
    log_error_actions(error_atom, rest)
  end

  defp log_error_actions(error_atom, [{:drop_coordinator, group}, {:rediscover_coordinator, group} | rest]) do
    Logger.info("Re-discovering group coordinator due to #{inspect(error_atom)} error")
    log_error_actions(error_atom, rest)
  end

  defp log_error_actions(error_atom, [{:drop_coordinator, group} | rest]) do
    Logger.info("Forgetting the coordinator of #{group} after #{inspect(error_atom)}")
    log_error_actions(error_atom, rest)
  end

  defp log_error_actions(error_atom, [{:rediscover_coordinator, _group} | rest]) do
    Logger.info("Re-discovering group coordinator due to #{inspect(error_atom)} error")
    log_error_actions(error_atom, rest)
  end

  defp run_cluster_actions(actions, state), do: Enum.reduce(actions, state, &run_cluster_action/2)

  defp run_cluster_action({:refresh_metadata, %{api_version: nil} = refresh}, state) do
    state |> refresh(Map.take(refresh, [:topics, :allow_auto_topic_creation])) |> elem(1)
  end

  defp run_cluster_action({:rediscover_coordinator, group}, state) do
    state |> rediscover_coordinator(group) |> elem(1)
  end

  defp run_cluster_action({:drop_coordinator, group}, state) do
    state |> with_store(:ok, &short/1, &MetadataStore.forget_coordinator(&1.pid, group, &2)) |> elem(1)
  end

  defp coordinator_request?(%NodeSelector{strategy: :consumer_group}), do: true
  defp coordinator_request?(_), do: false

  defp sleep_for_reconnect do
    Process.sleep(Application.get_env(:kafka_ex, :sleep_for_reconnect, 400))
  end

  defp client_request(request, state) do
    %{request | client_id: Config.client_id(), correlation_id: state.correlation_id}
  end

  defp select_broker_with_update(state, selector) do
    case select(state, selector) do
      {:error, reason} ->
        updated_state =
          selector
          |> ClusterMetadata.selection_miss_actions(allow_auto_topic_creation: state.allow_auto_topic_creation)
          |> run_cluster_actions(if(reason == :unavailable, do: drop_store(state), else: state))

        case select(updated_state, selector) do
          {:error, reason} ->
            # Debug, not warning: the consumer's fetch loop retries this lookup and owns the throttled log.
            Logger.debug("No broker for #{describe_selector(selector)} after metadata refresh: #{inspect(reason)}")

            {nil, updated_state}

          {:ok, _node_id, broker} ->
            {broker, updated_state}
        end

      {:ok, _node_id, broker} ->
        {broker, state}
    end
  end

  defp describe_selector(%NodeSelector{strategy: topic_partition_strategy, topic: topic, partition: partition})
       when topic_partition_strategy in [:topic_partition, :topic_partition_replica],
       do: "#{topic}/#{partition}"

  defp describe_selector(%NodeSelector{strategy: :consumer_group, consumer_group_name: group}),
    do: "consumer group #{group}"

  # send_request/4 is send-once; budget from :request_timeout, never the implicit
  # 5000 default (a larger :request_timeout would exit the caller; #357).
  defp timeout_val(nil), do: RequestBudget.call_budget(Config.request_timeout())
  defp timeout_val(timeout) when is_integer(timeout), do: timeout

  # The per-attempt socket-recv deadline for synchronous requests that do not
  # set their own `network_timeout` (metadata, offset, heartbeat, produce ack,
  # …). Resolves via `Config.request_timeout/0` (`:request_timeout` > deprecated
  # `:sync_timeout` > default). JoinGroup/SyncGroup pass an explicit timeout.
  defp config_request_timeout(timeout) do
    timeout || Config.request_timeout()
  end

  defp network_request(request, node_selector, state, builder) do
    case attempt_request(request, builder, node_selector, state, nil) do
      {_sent, {tag, result, state}} when tag in [:raised, :refused] -> {result, state}
      {_sent, outcome} -> outcome
    end
  end

  # Pairs the outcome with the request last dispatched; no retry fixes a :raised or :refused outcome.
  defp attempt_request(request, builder, node_selector, state, network_timeout) do
    timeout = config_request_timeout(network_timeout)

    case targets(node_selector, request, ensure_store(state)) do
      {:no_broker, state} ->
        {request, {{:error, :no_broker}, state}}

      {{:error, _} = error, state} ->
        {request, {error, state}}

      {:first_available, brokers, state} ->
        first_available({client_request(request, state), builder, node_selector, timeout}, brokers, state, nil)

      {:one, broker, state} ->
        send_to(client_request(request, state), builder, node_selector, broker, timeout, %{
          state
          | dispatch_stamp: snapshot_stamp(state)
        })
    end
  end

  defp send_to(request, builder, selector, broker, timeout, state) do
    dispatch = request |> connection_dispatch(timeout, true, state) |> as_is(builder)

    case await_negotiated(dispatch, builder, state, &await_connection(&1, selector, broker, &2)) do
      {:ready, {_key, _pid, %{request: sent}} = conn, state} ->
        span = start_request_span(sent)
        {sent, opened(span, fn -> finish_request_span(span, write(conn, state), broker) end)}

      {:not_written, result, state} ->
        {request, {result, state}}

      {:refused, _result, _state} = refused ->
        {request, refused}
    end
  end

  # A refused request was never written, so rebuilding it at the connection's versions costs no attempt.
  defp await_negotiated(dispatch, builder, state, await) do
    case await.(dispatch, state) do
      {:not_written, {:error, {:unsupported_version, versions}}, state} ->
        rebuild(dispatch, builder, versions, state, await)

      awaited ->
        awaited
    end
  end

  defp rebuild(dispatch, builder, versions, state, await) do
    case builder.(%{state | api_versions: versions}) do
      {:ok, request} -> dispatch |> carrying(client_request(request, state)) |> await.(state) |> refused_again()
      # An explicit version pin above this connection's max ends here, unwritten and never downgraded.
      {:error, _} = build_error -> {:refused, build_error, state}
    end
  end

  defp refused_again({:not_written, {:error, {:unsupported_version, _}}, state}) do
    {:refused, {:error, :api_version_no_supported}, state}
  end

  defp refused_again(awaited), do: awaited

  # The rest of the call runs under the open span, so a crash in it still ends the span.
  defp opened({metadata, span}, rest_of_call) do
    rest_of_call.()
  catch
    # A spent caller budget is control flow, and the walk that threw it has already stopped the span.
    :throw, {:caller_budget_spent, _state} = spent ->
      throw(spent)

    kind, reason ->
      Telemetry.exception_span(span, metadata, kind, reason, __STACKTRACE__)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp start_request_span(request) do
    metadata = Telemetry.request_metadata(request, %{})
    {metadata, Telemetry.start_span([:kafka_ex, :request], metadata)}
  end

  defp restamp({_metadata, span}, request), do: {Telemetry.request_metadata(request, %{}), span}

  defp finish_request_span({metadata, span}, {:raised, {kind, reason, stacktrace}, state}, _broker) do
    Telemetry.exception_span(span, metadata, kind, reason, stacktrace)
    {:raised, {:error, :timeout}, state}
  end

  defp finish_request_span(span, {:written, result, meta, state}, broker) do
    stop_request_span(span, result, meta, broker, state)
  end

  defp stop_request_span({metadata, span}, result, meta, broker, state) do
    stop = %{bytes_sent: meta.bytes_sent, bytes_received: meta.bytes_received, broker: broker_to_telemetry_info(broker)}
    Telemetry.stop_span(span, {result, Map.merge(metadata, stop)})
    {result, State.increment_correlation_id(state)}
  end

  defp targets(%NodeSelector{strategy: :first_available} = selector, request, state) do
    {operation, _version} = @protocol.request_info(request)

    case NodeSelector.connection_role(selector, operation) do
      :metadata -> metadata_candidates(state)
      role -> {:first_available, connected_first(state, role), state}
    end
  end

  defp targets(%NodeSelector{strategy: strategy} = selector, _request, state)
       when strategy in [:topic_partition, :topic_partition_replica, :consumer_group] do
    case select_broker_with_update(state, selector) do
      {nil, state} -> {:no_broker, state}
      {broker, state} -> {:one, broker, state}
    end
  end

  defp targets(selector, _request, state) do
    case select_known(state, selector) do
      {:ok, _node_id, broker} -> {:one, broker, state}
      {:error, :unavailable} -> {{:error, :no_broker}, drop_store(state)}
      {:error, _} = error -> {error, state}
    end
  end

  # A restarted store answers from an empty snapshot until its load round lands.
  defp select_known(state, %NodeSelector{strategy: strategy} = selector) when strategy in [:random, :controller] do
    if brokers(state) == [], do: {:error, :no_broker}, else: select(state, selector)
  end

  defp select_known(state, selector), do: select(state, selector)

  defp metadata_candidates(state) do
    {addresses, state} = with_store(state, [], &short/1, &MetadataStore.candidates(&1.pid, &2))
    known = Map.new(brokers(state), &{{&1.host, &1.port}, &1})
    {:first_available, Enum.map(addresses, &Map.get_lazy(known, &1, fn -> bootstrap_broker(&1) end)), state}
  end

  defp bootstrap_broker({host, port}), do: %Broker{host: host, port: port}

  defp connected_first(state, role) do
    {connected, rest} = state |> brokers() |> Enum.split_with(&Map.has_key?(state.conns, {&1.host, &1.port, role}))
    Enum.shuffle(connected) ++ Enum.shuffle(rest)
  end

  defp first_available({request, _builder, _selector, _timeout}, [], state, nil) do
    {request, {{:error, :no_broker}, state}}
  end

  defp first_available({request, _builder, _selector, _timeout}, [], state, {:refused, refused}) do
    {request, {:refused, refused, state}}
  end

  defp first_available(_walk, [], state, {:open, span, meta, sent}) do
    {sent, stop_request_span(span, {:error, :broker_failed}, %{meta | bytes_received: 0}, nil, state)}
  end

  defp first_available({request, builder, selector, timeout} = walk, [broker | rest], state, open) do
    dispatch = request |> connection_dispatch(timeout, false, state) |> as_is(builder)

    case await_candidate(dispatch, builder, selector, broker, state, open) do
      {:ready, conn, state} ->
        write_candidate(conn, broker, {walk, rest}, state, open)

      {:not_written, {:error, :no_broker}, state} ->
        Logger.debug("#{Broker.to_string(broker)} not connected, skipping")
        first_available(walk, rest, state, open)

      {:refused, {:error, error} = refused, state} ->
        Logger.warning("Network call to #{Broker.to_string(broker)} failed: #{inspect(error)}")
        first_available(walk, rest, state, refusal(open, refused))

      {:not_written, {:error, error}, state} ->
        Logger.warning("Network call to #{Broker.to_string(broker)} failed: #{inspect(error)}")
        first_available(walk, rest, state, open)
    end
  end

  # A walk that writes nothing ends on the refusal, not on a missing broker.
  defp refusal({:open, _span, _meta, _sent} = open, _refused), do: open
  defp refusal(_open, refused), do: {:refused, refused}

  # A spent budget at a later candidate still closes the walk's span before the call ends.
  defp await_candidate(dispatch, builder, selector, broker, state, {:open, span, meta, _sent}) do
    await_negotiated(dispatch, builder, state, &await_connection(&1, selector, broker, &2))
  catch
    :throw, {:caller_budget_spent, state} ->
      {_result, state} = stop_request_span(span, {:error, :timeout}, %{meta | bytes_received: 0}, nil, state)
      throw({:caller_budget_spent, state})
  end

  defp await_candidate(dispatch, builder, selector, broker, state, _nothing_written) do
    await_negotiated(dispatch, builder, state, &await_connection(&1, selector, broker, &2))
  end

  defp write_candidate({_key, _pid, %{request: sent}} = conn, broker, walk, state, {:open, span, _meta, _sent}) do
    walk_on(write(conn, state), restamp(span, sent), broker, sent, walk)
  end

  defp write_candidate({_key, _pid, %{request: sent}} = conn, broker, walk, state, _nothing_written) do
    span = start_request_span(sent)
    opened(span, fn -> walk_on(write(conn, state), span, broker, sent, walk) end)
  end

  defp walk_on({:written, {:error, error}, meta, state}, span, broker, sent, {walk, rest}) do
    Logger.warning("Network call to #{Broker.to_string(broker)} failed: #{inspect(error)}")
    first_available(walk, rest, state, {:open, span, meta, sent})
  end

  defp walk_on(outcome, span, broker, sent, _walk), do: {sent, finish_request_span(span, outcome, broker)}

  defp await_connection(dispatch, selector, broker, state) do
    {operation, _version} = dispatch.info
    key = {broker.host, broker.port, NodeSelector.connection_role(selector, operation)}
    await(key, Map.put(dispatch, :node_id, broker.node_id), state, 1)
  end

  # The connection's deadline falls this long before the client's call gives up, so the reply always wins.
  @deadline_margin 500

  defp connection_dispatch(request, timeout, wait?, state) do
    deadline = System.monotonic_time(:millisecond) + call_timeout(timeout, state) - @deadline_margin
    carrying(%{timeout: timeout, wait?: wait?, deadline: deadline}, request)
  end

  # A rebuilt request keeps its dispatch's deadline, so the rebuild never opens a second reconnect window.
  defp carrying(dispatch, request) do
    info = @protocol.request_info(request)
    reply? = Map.get(request, :acks) != 0
    Map.merge(dispatch, %{request: request, info: info, reply?: reply?, long_poll: fetch_max_wait(info, request)})
  end

  # A raw send_request/4 struct is written as built, whatever the broker advertised.
  defp as_is(dispatch, :as_is), do: Map.put(dispatch, :as_is?, true)
  defp as_is(dispatch, _builder), do: dispatch

  defp call_timeout(timeout, state) do
    %{connect_timeout: connect_timeout, reconnect_window: reconnect_window} = state.connection_options
    connect_timeout + reconnect_window + timeout + 1_000
  end

  defp fetch_max_wait({:fetch, _version}, request), do: request.max_wait_time
  defp fetch_max_wait(_info, _request), do: nil

  # Only a fetch's first :fetch await may park: a parked call runs again from its first attempt.
  defp await({_host, _port, :fetch} = key, dispatch, %State{parking: parking} = state, redispatches)
       when parking != nil do
    first_fetch_await(parking, key, dispatch, %{state | parking: nil}, redispatches)
  end

  defp await(key, dispatch, state, redispatches) do
    case ensure_connection(state, key) do
      {nil, state} -> {:not_written, {:error, :no_broker}, state}
      {pid, state} -> await_ready(key, pid, dispatch, state, redispatches)
    end
  end

  # The park bounded only the wait; the attempt's budget starts at resume, as for a call taken from the mailbox.
  defp first_fetch_await({:resumed, key, pid, outcome}, key, dispatch, state, redispatches) do
    awaited(outcome, key, pid, dispatch, state, redispatches)
  end

  defp first_fetch_await({:resumed, _key, _pid, _outcome}, key, dispatch, state, redispatches) do
    await(key, dispatch, state, redispatches)
  end

  defp first_fetch_await(:first, key, dispatch, state, redispatches) do
    first_connect? = not Map.has_key?(state.conns, key) or parked_on?(state, key)

    case ensure_connection(state, key) do
      {nil, state} -> {:not_written, {:error, :no_broker}, state}
      {pid, state} when first_connect? -> throw({@park, key, pid, dispatch, state})
      {pid, state} -> await_ready(key, pid, dispatch, state, redispatches)
    end
  end

  defp parked_on?(state, key), do: Enum.any?(state.parked, fn {_tag, entry} -> entry.key == key end)

  # The parked caller waits as one more waiter on the connection, so its window and outage rules hold.
  defp park(%{dispatch: dispatch, state: state} = parked, from) do
    tag = make_ref()
    request = :gen_statem.send_request(parked.pid, {:await, dispatch})
    timer = :erlang.start_timer(dispatch.deadline + @deadline_margin, self(), {:parked, tag}, abs: true)

    entry =
      Map.merge(Map.drop(parked, [:dispatch, :state]), %{
        from: from,
        request: request,
        timer: timer,
        deadline: state.deadline
      })

    %{state | parked: Map.put(state.parked, tag, entry)}
  end

  defp answered(message, {tag, %{request: request}}) do
    case :gen_statem.check_response(message, request) do
      :no_reply -> nil
      {:reply, outcome} -> {tag, outcome}
      {:error, {reason, _pid}} when reason in [:noproc, :normal] -> {tag, :gone}
      {:error, _down} -> {tag, {:error, :timeout, @no_bytes}}
    end
  end

  defp resume(tag, outcome, state) do
    {entry, parked} = Map.pop!(state.parked, tag)
    cancel_parked_timer(entry.timer)
    state = %{state | parked: parked, parking: {:resumed, entry.key, entry.pid, outcome}, deadline: entry.deadline}
    {:reply, reply, state} = fetch_reply(entry.run.(state), entry.from)
    GenServer.reply(entry.from, reply)
    %{state | deadline: nil}
  end

  defp cancel_parked_timer(timer) do
    if :erlang.cancel_timer(timer) == false do
      receive do
        {:timeout, ^timer, _} -> :ok
      after
        0 -> :ok
      end
    end
  end

  # An idle stop can race the call: an exit with :noproc or :normal means nothing was written.
  defp await_ready(key, pid, dispatch, state, redispatches) do
    outcome = call_connection(&ConnectionStateMachine.await/3, pid, dispatch, call_timeout(dispatch.timeout, state))
    awaited(outcome, key, pid, dispatch, state, redispatches)
  end

  defp awaited(outcome, key, pid, dispatch, state, redispatches) do
    case outcome do
      :ok -> {:ready, {key, pid, dispatch}, state}
      :gone when redispatches > 0 -> await(key, dispatch, forget_connection(state, key, pid), redispatches - 1)
      :gone -> {:not_written, {:error, :no_broker}, forget_connection(state, key, pid)}
      {:error, :not_connected, _meta} -> {:not_written, {:error, :no_broker}, state}
      {:error, reason, _meta} -> {:not_written, {:error, reason}, state}
    end
  end

  # Same dispatch, so the same deadline: the write never opens a second reconnect window.
  defp write({key, pid, dispatch}, state) do
    left = max(dispatch.deadline + @deadline_margin - System.monotonic_time(:millisecond), 0)

    case call_connection(&ConnectionStateMachine.write/3, pid, dispatch, left) do
      :gone -> {:written, {:error, :closed}, @no_bytes, forget_connection(state, key, pid)}
      {:ok, :no_response, meta} -> {:written, {:ok, :no_response}, meta, state}
      {:ok, frame, meta} -> {:written, ResponseParser.deserialize(frame, dispatch.request), meta, state}
      {:error, {:exception, kind, reason, stacktrace}, _meta} -> {:raised, {kind, reason, stacktrace}, state}
      # A reconnect between the await and the write can land on a broker with other versions.
      {:error, {:unsupported_version, _}, meta} -> {:written, {:error, :api_version_no_supported}, meta, state}
      {:error, reason, meta} -> {:written, {:error, reason}, meta, state}
    end
  end

  defp call_connection(call, pid, dispatch, call_timeout) do
    call.(pid, dispatch, call_timeout)
  catch
    :exit, {reason, _call} when reason in [:noproc, :normal] -> :gone
    :exit, _reason -> {:error, :timeout, @no_bytes}
  end

  defp ensure_connection(state, {host, port, :metadata}) do
    with_store(state, nil, &short/1, fn store, timeout ->
      {:ok, pid} = MetadataStore.connection(store.pid, {host, port}, timeout)
      pid
    end)
  end

  defp ensure_connection(state, {host, port, role} = key) do
    case Map.fetch(state.conns, key) do
      {:ok, %{pid: pid}} ->
        {pid, state}

      :error ->
        args = %{
          client: self(),
          host: host,
          port: port,
          role: role,
          use_ssl: state.use_ssl,
          secrets: secrets_fun(state),
          reconnect_backoff: state.reconnect_backoff
        }

        case start_connection(state.supervisor, args) do
          {:ok, pid} -> {pid, %{state | conns: Map.put(state.conns, key, %{pid: pid, ref: Process.monitor(pid)})}}
          :restarting -> {nil, state}
        end
    end
  end

  @doc false
  # While the client supervisor restarts the connection supervisor there is none, or a dying one, to start under.
  @spec start_connection(pid(), map()) :: DynamicSupervisor.on_start_child() | :restarting
  def start_connection(supervisor, args) do
    case ClientSupervisor.connection_supervisor(supervisor) do
      nil -> :restarting
      connections -> DynamicSupervisor.start_child(connections, {ConnectionStateMachine, args})
    end
  catch
    :exit, {reason, _call} when reason in [:noproc, :shutdown, :killed] -> :restarting
  end

  # A parked call can learn of a dead pid after a retry has replaced it under the same key.
  defp forget_connection(state, key, pid) do
    case Map.pop(state.conns, key) do
      {%{pid: ^pid, ref: ref}, conns} ->
        Process.demonitor(ref, [:flush])
        %{state | conns: conns}

      _other ->
        state
    end
  end

  # Binds the two values, not the state, so a connection never holds the client's state.
  defp secrets_fun(state) do
    ssl_options = state.ssl_options
    auth = state.auth
    fn -> %{ssl_options: ssl_options, auth: auth} end
  end

  defp broker_to_telemetry_info(nil), do: %{}

  defp broker_to_telemetry_info(%Broker{} = broker) do
    %{node_id: broker.node_id, host: broker.host, port: broker.port, rack: broker.rack}
  end
end
