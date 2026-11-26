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

  alias KafkaEx.Client.ConnectionRegistry
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
    request(server, {:network_request, request, node_selector, []}, timeout_val(timeout))
  end

  @doc """
  Runs a request message in the calling process and answers within `timeout` ms.

  The single entry point for `KafkaEx.API` and `send_request/4`; calling the client
  with `GenServer.call/3` directly is not supported. A client on another node runs
  the request there.
  """
  @spec request(KafkaEx.API.client(), message :: tuple(), timeout :: pos_integer()) ::
          {:ok, term()} | {:error, term()}
  def request(client, message, timeout) do
    case GenServer.whereis(client) do
      pid when is_pid(pid) and node(pid) != node() -> remote_request(node(pid), client, message, timeout)
      {_name, node} -> remote_request(node, client, message, timeout)
      _local -> local_request(client, message, timeout)
    end
  end

  # The remote request ends on its own deadline; the margin covers the trip back.
  defp remote_request(node, client, message, timeout) do
    :erpc.call(node, __MODULE__, :request, [client, message, timeout], timeout + 1_000)
  catch
    :error, {:erpc, :timeout} -> deadline_reply(message)
    # An unreachable node exits the caller, as the GenServer.call to it always did.
    :error, {:erpc, reason} -> exit({reason, {__MODULE__, :request, [client, message, timeout]}})
  end

  defp local_request(client, message, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    context = %{GenServer.call(client, :context, timeout) | deadline: deadline}
    serve_in_turn(context.request_concurrency, client, message, context)
  catch
    :exit, {:timeout, _call} -> deadline_reply(message)
  end

  defp serve_in_turn(:multiplexed, client, message, context), do: serve(message, client, context)

  # A release after a timed-out wait takes the caller off the queue, so a late permit never goes to it.
  defp serve_in_turn(:serial, client, message, context) do
    :ok = GenServer.call(client, :permit, time_left(context.deadline))
    serve(message, client, context)
  after
    GenServer.cast(client, {:release, self()})
  end

  # Served without a request spec: no operation span, and a bare :timeout as these calls always answered.
  @specless_messages [:network_request, :topic_metadata, :cluster_metadata]

  # The operation's span opens before the wait for a table, so a request that times out waiting still ends it.
  defp serve(message, client, context) when elem(message, 0) in @specless_messages do
    message |> served(nil, client, await_table(client, context), %{}) |> elem(0)
  end

  defp serve(message, client, context) do
    with %{} = spec <- request_spec(message) do
      case span_of(message, spec) do
        nil ->
          message |> served(spec, client, await_table(client, context), %{}) |> elem(0)

        {event, metadata} ->
          Telemetry.span(event, metadata, fn ->
            served(message, spec, client, await_table(client, context), metadata)
          end)
      end
    end
  end

  # Only a request's first attempt, before anything is written, comes back stale, so serving again sends nothing twice.
  defp served(message, spec, client, {:ok, context}, metadata) do
    case operate(message, spec, context, metadata) do
      {:stale_context, _stop_metadata} -> served(message, spec, client, next_context(client, context), metadata)
      done -> done
    end
  end

  defp served(message, _spec, _client, :timeout, metadata), do: {deadline_reply(message), metadata}

  # The counter stays 0 until the first metadata lands, so a fresh table belongs to a store still loading.
  defp await_table(client, %State{store: %{pid: store, tid: tid}} = context) do
    case MetadataTable.counter(tid) do
      {:ok, 0} ->
        # A failed load round leaves the store loading again, so the request waits for its next round.
        case MetadataStore.await_ready(store, time_left(context.deadline)) do
          {:ok, _ready} -> {:ok, context}
          {:error, {:boot, _reason}} -> await_table(client, context)
        end

      {:ok, _counter} ->
        {:ok, context}

      {:error, :unavailable} ->
        next_context(client, context)
    end
  catch
    :exit, {_reason, {:gen_statem, :call, [^store, :await_ready, _timeout]}} -> next_context(client, context)
  end

  defp await_table(client, context), do: next_context(client, context)

  # A client that does not hand out a context in time ends the request as its deadline would.
  defp next_context(client, %State{deadline: deadline}) do
    case time_left(deadline) do
      0 ->
        :timeout

      left ->
        Process.sleep(Retry.backoff_delay_jittered(0, 100, left))
        await_table(client, %{GenServer.call(client, :context, time_left(deadline)) | deadline: deadline})
    end
  catch
    :exit, {:timeout, _call} -> :timeout
  end

  defp time_left(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  # The operation loop has always answered a timeout as an %Error{}.
  defp deadline_reply(message) when elem(message, 0) in @specless_messages, do: {:error, :timeout}
  defp deadline_reply(_message), do: {:error, Error.build(:timeout, %{})}

  require Logger

  # KafkaEx.API derives its fetch budget from this, so the two cannot drift (#357).
  @retry_count 3
  @no_bytes %{bytes_sent: 0, bytes_received: 0}

  # The broker holds JoinGroup/SyncGroup replies for the rebalance window; ConsumerGroup.Manager owns the rejoin.
  @coordinator_max_attempts 1

  @store_call_timeout 5_000

  # Bounds ensure_store's calls to the store and registry; its supervisor lookup has no timeout, as before.
  @lookup_timeout 1_000

  @doc false
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

    unless state.request_concurrency in [:serial, :multiplexed] do
      raise ArgumentError,
            "invalid request_concurrency #{inspect(state.request_concurrency)}; expected :serial or :multiplexed"
    end

    # Crash at boot, not at first produce/auth, when the config needs an optional dep that is not loaded.
    :ok = OptionalDeps.validate!(state.auth)

    # One-time deprecation notice for the legacy :sync_timeout key (→ :request_timeout).
    :ok = Config.warn_deprecated_timeout_config()

    state = %{
      state
      | reconnect_backoff: ReconnectBackoff.new(),
        correlation: :atomics.new(1, []),
        hints: :ets.new(:kafka_ex_replica_hints, [:public]),
        connection_options: ConnectionStateMachine.options()
    }

    {:ok, supervisor} = ClientSupervisor.start_link(%{store: store_args(state, args)})
    state = %{state | supervisor: supervisor}
    store = ClientSupervisor.metadata_store(supervisor)

    case await_boot(store, state) do
      {:ok, %{table: tid, api_versions: api_versions}} ->
        registry = ClientSupervisor.connection_registry(supervisor)
        conns = ConnectionRegistry.table(registry, @store_call_timeout)
        store = %{pid: store, ref: Process.monitor(store), tid: tid}
        {:ok, %{state | api_versions: api_versions, store: store, registry: registry, conns: conns}}

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

  @impl true
  def handle_call(:context, _from, state) do
    {:reply, %{state | ssl_options: [], auth: nil, supervisor: nil, permit: nil, waiting: :queue.new()}, state}
  end

  def handle_call(:permit, {pid, _tag}, %State{permit: nil} = state), do: {:reply, :ok, grant_permit(state, pid)}
  def handle_call(:permit, from, state), do: {:noreply, %{state | waiting: :queue.in(from, state.waiting)}}

  def handle_call(:correlation_id, _from, state), do: {:reply, {:ok, :atomics.get(state.correlation, 1)}, state}

  def handle_call(:consumer_group, _from, state), do: {:reply, state.consumer_group_for_auto_commit, state}

  def handle_call({:set_consumer_group_for_auto_commit, consumer_group}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group) do
      {:reply, :ok, %{state | consumer_group_for_auto_commit: consumer_group}}
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  # Requests run in the caller through request/3; an old direct call gets an error, not a crash of the shared client.
  def handle_call(message, _from, state) do
    Logger.warning("#{inspect(__MODULE__)} does not serve #{inspect(message, limit: 5)}; use KafkaEx.API")
    {:reply, {:error, :unsupported_call}, state}
  end

  @impl true
  def handle_cast({:release, pid}, %State{permit: {pid, ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, pass_permit(state)}
  end

  def handle_cast({:release, pid}, state) do
    {:noreply, %{state | waiting: :queue.filter(fn {waiter, _tag} -> waiter != pid end, state.waiting)}}
  end

  defp grant_permit(state, pid), do: %{state | permit: {pid, Process.monitor(pid)}}

  # A waiter that died in the queue still gets the permit and loses it at once to its monitor's :DOWN.
  defp pass_permit(state) do
    case :queue.out(state.waiting) do
      {{:value, {pid, _tag} = from}, waiting} ->
        GenServer.reply(from, :ok)
        grant_permit(%{state | waiting: waiting}, pid)

      {:empty, _waiting} ->
        %{state | permit: nil}
    end
  end

  @impl true
  def handle_info({:kafka_ex_metadata_store, :swapped}, state) do
    {:noreply, state |> ensure_store() |> stop_departed_connections()}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{permit: {_holder, ref}} = state) do
    {:noreply, pass_permit(state)}
  end

  # A restarted registry always restarts the store too, so the store's :DOWN also stands for the registry's.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{store: %{ref: ref}} = state) do
    :ets.delete_all_objects(state.hints)
    {:noreply, ensure_store(%{state | store: nil, registry: nil, conns: nil})}
  end

  def handle_info(message, state) do
    Logger.warning("#{inspect(__MODULE__)} ignoring unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  @impl true
  def terminate(reason, state) do
    Logger.debug("Shutting down worker #{inspect(worker_identity())}, reason: #{inspect(reason)}")
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

  # The registry deletes each row on the connection's :DOWN.
  defp stop_unadvertised(state, advertised) do
    for {{host, port, _role}, pid} <- :ets.tab2list(state.conns), not MapSet.member?(advertised, {host, port}) do
      ConnectionStateMachine.stop(pid, :metadata_update)
    end

    state
  rescue
    # A restarted registry took its table and connections with it before the client saw the store's :DOWN.
    ArgumentError -> state
  end

  # Until a store and its registry answer, the client keeps store: nil and callers wait for the table.
  defp ensure_store(%State{store: nil} = state) do
    with registry when is_pid(registry) <- ClientSupervisor.connection_registry(state.supervisor),
         conns = ConnectionRegistry.table(registry, @lookup_timeout),
         store when is_pid(store) <- ClientSupervisor.metadata_store(state.supervisor),
         {:ok, tid} <- MetadataStore.table(store, @lookup_timeout) do
      %{state | store: %{pid: store, ref: Process.monitor(store), tid: tid}, registry: registry, conns: conns}
    else
      _none -> state
    end
  catch
    :exit, _reason -> state
  end

  defp ensure_store(state), do: state

  defp operate({:cluster_metadata, _opts}, _spec, context, metadata), do: {cluster_metadata(context), metadata}

  defp operate({:topic_metadata, topics, opts}, _spec, context, metadata) do
    allow_topic_creation = Keyword.get(opts, :allow_topic_creation, false)
    result = refresh(context, %{topics: topics, allow_auto_topic_creation: allow_topic_creation})
    {topics_reply(result, topics, cached_topics(context, topics)), metadata}
  end

  defp operate({:network_request, request, node_selector, _opts}, _spec, context, metadata) do
    {network_request(request, node_selector, context, :as_is), metadata}
  end

  defp operate({:join_group, _group, _member_id, _opts}, spec, context, metadata), do: join(spec, context, metadata)

  defp operate({:sync_group, _group, _generation_id, _member_id, _opts}, spec, context, metadata) do
    case run(spec, context, @coordinator_max_attempts) do
      {:ok, result} = ok -> {ok, Map.put(metadata, :assigned_partitions, assigned_partitions(result))}
      error_result -> {error_result, metadata}
    end
  end

  defp operate({:consumer_group_heartbeat, _group, _opts}, spec, context, metadata) do
    member_heartbeat(spec, context, metadata)
  end

  # The produce stop event has always carried the offset, not the message count of its start.
  defp operate({:produce, _topic, _partition, _messages, _opts}, spec, context, metadata) do
    produce(spec, context, Map.delete(metadata, :message_count))
  end

  defp operate({:metadata, _topics, _opts, _api_version}, spec, context, _metadata), do: metadata_request(spec, context)

  defp operate({:fetch, _topic, _partition, offset, _opts}, spec, context, metadata) do
    fetched(spec, offset, context, metadata)
  end

  defp operate(_message, spec, context, metadata), do: {run(spec, context), metadata}

  defp with_store(context, unavailable, call) do
    with_store(context, unavailable, fn _store -> @store_call_timeout end, call)
  end

  defp with_store(%State{store: store} = context, unavailable, wanted, call) do
    call.(store, min(wanted.(store), time_left(context.deadline)))
  catch
    :exit, _reason -> unavailable
  end

  # A failed refresh reads as a timeout; a version refusal keeps the atom the client's request path returns.
  defp refresh(context, refresh) do
    with_store(context, {:error, :timeout}, &round_call_timeout(context, &1), fn store, timeout ->
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

  defp rediscover_coordinator(context, group) do
    with_store(context, {:error, :timeout}, &round_call_timeout(context, &1), fn store, timeout ->
      MetadataStore.coordinator(store.pid, group, timeout)
    end)
  end

  # An empty snapshot means the store is loading, and a refresh then waits for its load round.
  defp round_call_timeout(context, store) do
    case MetadataTable.brokers(store.tid) do
      [] -> MetadataStore.call_timeout(length(context.bootstrap_uris), :load)
      brokers -> MetadataStore.call_timeout(length(brokers))
    end
  end

  defp select(%State{store: %{tid: tid}} = context, selector) do
    MetadataTable.select(tid, selector, hints_for(context, selector))
  end

  defp brokers(%State{store: nil}), do: []
  defp brokers(%State{store: %{tid: tid}}), do: MetadataTable.brokers(tid)

  defp cached_topics(%State{store: %{tid: tid}}, topics) do
    case MetadataTable.topics(tid, topics) do
      {:ok, cached} -> cached
      {:error, :unavailable} -> []
    end
  end

  # A booted snapshot always has a broker, so an empty one is a restarted store still loading.
  defp cluster_metadata(%State{store: %{tid: tid}} = context) do
    case with_store(context, :stale_context, &MetadataStore.snapshot(&1.pid, &2)) do
      {:ok, %ClusterMetadata{brokers: brokers}, _counter} when map_size(brokers) == 0 -> :stale_context
      {:ok, metadata, counter} -> {:ok, with_hints(metadata, hints_for(context), {tid, counter})}
      :stale_context -> :stale_context
    end
  end

  defp with_hints(metadata, hints, stamp), do: %{metadata | preferred_replicas: valid_hints(hints, stamp)}

  defp valid_hints(hints, stamp) do
    for {partition, {node_id, ^stamp}} <- hints, into: %{}, do: {partition, node_id}
  end

  defp snapshot_stamp(%State{store: %{tid: tid}}) do
    case MetadataTable.counter(tid) do
      {:ok, counter} -> {tid, counter}
      {:error, :unavailable} -> nil
    end
  end

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

  @budgeted_by_network_timeout [
    :fetch,
    :heartbeat,
    :consumer_group_heartbeat,
    :join_group,
    :sync_group,
    :create_topics,
    :delete_topics
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
  defp attach_store(%{api: api} = spec, context) when api in @topic_id_requests do
    %State{store: %{tid: tid}} = context
    resolver = topic_name_parse_opts(tid)
    %{spec | store_tid: tid, parse: &@protocol.parse_response(api, &1, resolver)}
  end

  defp attach_store(spec, _context), do: spec

  defp topic_name_parse_opts(tid), do: [topic_name_resolver: &MetadataTable.topic_name_by_id(tid, &1)]

  # Only a member heartbeat (epoch > 0) is fenced when resent after an epoch bump; join and leave are accepted again.
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

  defp span_of({:offset_commit, group, [{topic, partitions}], _opts}, _spec) do
    {[:kafka_ex, :consumer, :commit], Telemetry.commit_metadata(group, Config.client_id(), topic, length(partitions))}
  end

  defp span_of({:heartbeat, group, member_id, generation_id, _opts}, _spec) do
    {[:kafka_ex, :consumer, :heartbeat], Telemetry.heartbeat_metadata(group, member_id, generation_id)}
  end

  defp span_of({:leave_group, group, member_id, _opts}, _spec) do
    {[:kafka_ex, :consumer, :leave], Telemetry.leave_group_metadata(group, member_id)}
  end

  defp span_of({:join_group, group, member_id, opts}, _spec) do
    {[:kafka_ex, :consumer, :join],
     Telemetry.join_group_metadata(group, member_id || "", Keyword.get(opts, :topics, []))}
  end

  defp span_of({:sync_group, group, generation_id, member_id, opts}, _spec) do
    # Leader sends non-empty group_assignment, followers send empty
    is_leader = Keyword.get(opts, :group_assignment, []) != []
    {[:kafka_ex, :consumer, :sync], Telemetry.sync_group_metadata(group, member_id, generation_id, is_leader)}
  end

  defp span_of({:consumer_group_heartbeat, group, opts}, _spec) do
    member_id = Keyword.get(opts, :member_id) || ""
    metadata = Telemetry.consumer_group_heartbeat_metadata(group, member_id, Keyword.fetch!(opts, :member_epoch))
    {[:kafka_ex, :consumer, :consumer_group_heartbeat], metadata}
  end

  defp span_of({:produce, topic, partition, messages, _opts}, spec) do
    metadata = Telemetry.produce_metadata(topic, partition, Config.client_id(), Keyword.fetch!(spec.opts, :acks))
    {[:kafka_ex, :produce], Map.put(metadata, :message_count, length(messages))}
  end

  defp span_of({:metadata, topics, _opts, _api_version}, _spec) do
    topic_list = if is_list(topics), do: topics, else: [topics]
    {[:kafka_ex, :metadata, :update], Telemetry.metadata_update_metadata(Config.client_id(), topic_list)}
  end

  defp span_of({:fetch, topic, partition, offset, _opts}, _spec) do
    {[:kafka_ex, :fetch], Telemetry.fetch_metadata(topic, partition, offset, Config.client_id())}
  end

  defp span_of(_message, _spec), do: nil

  defp join(spec, context, metadata, last_error \\ nil) do
    case run(spec, context, @coordinator_max_attempts, last_error) do
      {:ok, result} = ok ->
        {ok,
         Map.merge(metadata, %{generation_id: result.generation_id, is_leader: result.leader_id == result.member_id})}

      # KIP-394: a v4+ join without a member id is answered with one to join again with.
      {:error, %Error{error: :member_id_required, metadata: %{member_id: assigned_id}} = error} ->
        Logger.info("JoinGroup returned member_id_required (KIP-394), retrying with assigned member_id")
        join(%{spec | opts: Keyword.put(spec.opts, :member_id, assigned_id)}, context, metadata, error)

      error_result ->
        {error_result, metadata}
    end
  end

  defp assigned_partitions(result), do: Enum.reduce(result.partition_assignments, 0, &(length(&1.partitions) + &2))

  defp member_heartbeat(spec, context, metadata) do
    spec = attach_store(spec, context)

    case build(spec, context) do
      {:ok, request} ->
        # KIP-1082: a v1+ join gets its member id at build; report and rebuild with it, so a retry keeps one member.
        metadata = %{metadata | member_id: request.member_id}
        spec = %{spec | opts: Keyword.put(spec.opts, :member_id, request.member_id)}

        case retry(spec, request, context, @retry_count, nil) do
          {:ok, result} = ok -> {ok, Map.merge(metadata, heartbeat_result_metadata(result))}
          error_result -> {error_result, metadata}
        end

      error ->
        {error, metadata}
    end
  end

  defp heartbeat_result_metadata(result) do
    %{
      member_id: result.member_id,
      new_member_epoch: result.member_epoch,
      assigned_partitions: ConsumerGroupHeartbeat.assigned_partition_count(result)
    }
  end

  defp produce(spec, context, metadata) do
    case build(spec, context) do
      {:ok, %{acks: 0} = request} -> {fire_and_forget(request, spec, context), metadata}
      {:ok, request} -> spec |> retry(request, context, @retry_count, nil) |> with_offset(metadata)
      error -> {error, metadata}
    end
  end

  defp with_offset({:ok, %{base_offset: offset}} = ok, metadata) when offset != nil do
    {ok, Map.put(metadata, :offset, offset)}
  end

  defp with_offset(result, metadata), do: {result, metadata}

  # acks=0: the broker sends no response, and a resend would duplicate records.
  defp fire_and_forget(request, %{route: %NodeSelector{topic: topic, partition: partition}} = spec, context) do
    case network_request(request, spec.route, context, builder(spec)) do
      {:ok, :no_response} ->
        {:ok, RecordMetadata.build(topic: topic, partition: partition, base_offset: nil)}

      {:error, reason} ->
        Logger.warning("Fire-and-forget produce to #{topic}/#{partition} failed with #{inspect(reason)}")
        {:error, build_transport_error(reason)}

      :stale_context ->
        :stale_context
    end
  end

  defp fetched(spec, offset, context, metadata) do
    with {:ok, request} <- build(spec, context),
         {:ok, result} <- retry(spec, request, context, @retry_count, nil) do
      filtered_result = Fetch.filter_from_offset(result, offset)
      {{:ok, filtered_result}, Map.put(metadata, :message_count, length(Map.get(filtered_result, :records, [])))}
    else
      error_result -> {error_result, metadata}
    end
  end

  # The hint table dies with the client; a request that outlives it reads and writes no hint.
  defp forget_preferred_replica(context, %NodeSelector{topic: topic, partition: partition}) do
    :ets.delete(context.hints, {topic, partition})
  rescue
    ArgumentError -> true
  end

  defp reading_from_replica?(
         %State{store: %{tid: tid}} = context,
         %NodeSelector{strategy: :topic_partition_replica} = selector
       ) do
    MetadataTable.select(tid, selector, hints_for(context, selector)) != MetadataTable.select(tid, selector, %{})
  end

  defp reading_from_replica?(_context, _selector), do: false

  defp hints_for(%State{hints: table}, %NodeSelector{topic: topic, partition: partition}) do
    Map.new(:ets.lookup(table, {topic, partition}))
  rescue
    ArgumentError -> %{}
  end

  defp hints_for(%State{hints: table}) do
    Map.new(:ets.tab2list(table))
  rescue
    ArgumentError -> %{}
  end

  # A -1 replica means "stay" (Java); the stamp lets a later swap invalidate the hint.
  defp cache_preferred_replica(
         context,
         {_tid, _counter} = stamp,
         %NodeSelector{topic: topic, partition: partition},
         %Fetch{preferred_read_replica: node_id}
       )
       when is_integer(node_id) and node_id != -1 do
    :ets.insert(context.hints, {{topic, partition}, {node_id, stamp}})
  rescue
    ArgumentError -> true
  end

  defp cache_preferred_replica(_context, _stamp, _route, _result), do: true

  # The seq is taken before the request, so a refresh issued after it wins over this answer.
  defp metadata_request(spec, context) do
    case build(spec, context) do
      {:ok, request} ->
        seq = with_store(context, nil, &MetadataStore.next_seq(&1.pid, &2))

        case retry(spec, request, context, @retry_count, nil) do
          {:ok, {returned, topic_errors}} ->
            merge_cluster_metadata(context, seq, returned, request)
            stop_metadata = %{broker_count: map_size(returned.brokers), topic_count: map_size(returned.topics)}
            {named_topics_reply(request, returned, topic_errors), stop_metadata}

          error_result ->
            {error_result, %{}}
        end

      error ->
        {error, %{}}
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

  defp merge_cluster_metadata(_context, nil, _metadata, _request), do: :ok

  defp merge_cluster_metadata(context, seq, %ClusterMetadata{} = metadata, request) do
    scope = merge_scope(request, metadata)
    with_store(context, :ok, &MetadataStore.merge(&1.pid, seq, metadata, scope, &2))
  end

  # A full request replaces the topic map (dropping deleted topics); a targeted one only adds or updates.
  defp merge_scope(request, metadata) do
    if all_topics_request?(request), do: :all, else: {:topics, Map.keys(metadata.topics)}
  end

  # "All topics" is a null topic list from V1 on (`[]` is sent as null) but an empty list in V0, which is not nullable.
  defp all_topics_request?(%{topics: nil}), do: true
  defp all_topics_request?(%Kayrock.Metadata.V0.Request{topics: []}), do: true
  defp all_topics_request?(_request), do: false

  # The request is built once; a connection that refuses its version gets it rebuilt by builder/1.
  @spec run(request_spec(), State.t(), pos_integer(), term()) :: {:ok, term()} | {:error, term()} | :stale_context
  defp run(spec, context, attempts \\ @retry_count, last_error \\ nil) do
    spec = attach_store(spec, context)

    case build(spec, context) do
      {:ok, request} -> retry(spec, request, context, attempts, last_error)
      error -> error
    end
  end

  defp build(spec, context), do: RequestBuilder.build(spec.api, spec.opts, topic_view(spec, context))

  defp builder(spec), do: &build(spec, &1)

  defp topic_view(%{api: api, opts: opts, store_tid: tid}, context) when api in [:offset_fetch, :offset_commit] do
    topics = for {topic, _partitions} <- Keyword.fetch!(opts, :topics), do: topic
    %{context | cluster_metadata: MetadataTable.topic_view(tid, topics)}
  end

  defp topic_view(_spec, context), do: context

  defp retry(_spec, _request, _context, 0, last_error), do: {:error, last_error}

  defp retry(spec, request, context, attempts, last_error) do
    case time_left(context.deadline) do
      0 ->
        {:error, Error.build(:timeout, %{})}

      _left ->
        request
        |> attempt_request(builder(spec), spec.route, context, spec.network_timeout)
        |> attempted(spec, request, context, attempts, last_error)
    end
  end

  # Not rebuilt: the failed attempt may have reached the broker, and a join with a new member id leaves a ghost member.
  defp attempted(:stale_context, _spec, _request, _context, _attempts, nil), do: :stale_context

  defp attempted(:stale_context, spec, %sent{} = request, context, attempts, _last_error) do
    handle_request_error(spec, request, context, attempts, Error.build(:no_broker, %{}), sent)
  end

  defp attempted({_sent, {:refused, refused}, _stamp}, _spec, _request, _context, _attempts, _last_error), do: refused

  defp attempted({%sent{}, {:raised, {:error, reason}}, _stamp}, spec, request, context, attempts, _last_error) do
    final = %{spec | retryable?: fn _error -> false end, send_once?: fn _error -> false end}
    handle_request_error(final, request, context, attempts, build_transport_error(reason), sent)
  end

  # The hint is written as its answer arrives, against the table the request was routed by.
  defp attempted({%sent{}, {:ok, response}, stamp}, spec, request, context, attempts, _last_error) do
    case spec.parse.(response) do
      {:ok, result} ->
        cache_preferred_replica(context, stamp, spec.route, result)
        {:ok, result}

      {:error, [error | _]} ->
        handle_request_error(spec, request, context, attempts, error, sent)

      {:error, %Error{} = error} ->
        handle_request_error(spec, request, context, attempts, error, sent)
    end
  end

  # The same refusal as an unwritten one, met when a reconnect lands between the await and the write.
  defp attempted(
         {_sent, {:error, :api_version_no_supported} = refused, _stamp},
         _spec,
         _request,
         _context,
         _attempts,
         _
       ) do
    refused
  end

  defp attempted({%sent{}, {:error, reason}, _stamp}, spec, request, context, attempts, _last_error) do
    handle_request_error(spec, request, context, attempts, build_transport_error(reason), sent)
  end

  defp build_transport_error(reason) when is_atom(reason), do: Error.build(reason, %{})
  # A non-atom reason (SSL {:tls_alert, _}) would collapse to :unknown, which the fetch loop treats as fatal.
  defp build_transport_error(reason), do: Error.build(:transport_error, %{transport_reason: reason})

  defp handle_request_error(spec, request, context, attempts, error, request_name) do
    error_atom = extract_error_atom(error)
    Logger.warning("Request #{inspect(request_name)} failed with error #{inspect(error_atom)}")

    cond do
      # KIP-392: a replica's error is not authoritative (it may lag the leader), so the leader decides.
      reading_from_replica?(context, spec.route) ->
        forget_preferred_replica(context, spec.route)
        retry(spec, request, context, attempts, error)

      # Send-once only for coordinator requests: the broker holds them for the rebalance window.
      spec.send_once?.(error_atom) and coordinator_request?(spec.route) ->
        Logger.info(
          "Coordinator request #{inspect(request_name)} failed with #{inspect(error_atom)}; " <>
            "not retrying at the client (send-once)"
        )

        act_on_error(error_atom, spec, context)
        {:error, error}

      # A stale topic id is baked into the built request, so a resend cannot succeed; the caller rebuilds it.
      error_atom == :unknown_topic_id ->
        act_on_error(error_atom, spec, context)
        {:error, error}

      spec.retryable?.(error_atom) ->
        act_on_error(error_atom, spec, context)
        back_off(attempts, context)
        retry(spec, request, context, attempts - 1, error)

      true ->
        Logger.info("Error #{inspect(error_atom)} is not retryable for #{inspect(request_name)}, failing immediately")
        {:error, error}
    end
  end

  # The last attempt has no next one to wait for; the deadline caps the wait, so a spent budget ends on :timeout.
  defp back_off(1, _context), do: :ok

  defp back_off(attempts, context) do
    Process.sleep(min(Retry.backoff_delay_jittered(@retry_count - attempts, 100, 1_000), time_left(context.deadline)))
  end

  defp extract_error_atom(%Error{error: error}), do: error
  defp extract_error_atom(error) when is_atom(error), do: error
  defp extract_error_atom({_group_id, error}) when is_atom(error), do: error
  defp extract_error_atom(_), do: :unknown

  defp act_on_error(error_atom, spec, context) do
    actions =
      ClusterMetadata.error_actions(error_atom, spec.route,
        rediscover_coordinator?: spec.rediscover_coordinator?,
        allow_auto_topic_creation: context.allow_auto_topic_creation
      )

    log_error_actions(error_atom, actions)
    run_cluster_actions(actions, context)
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

  defp run_cluster_actions(actions, context), do: Enum.each(actions, &run_cluster_action(&1, context))

  defp run_cluster_action({:refresh_metadata, %{api_version: nil} = refresh}, context) do
    refresh(context, Map.take(refresh, [:topics, :allow_auto_topic_creation]))
  end

  defp run_cluster_action({:rediscover_coordinator, group}, context), do: rediscover_coordinator(context, group)

  defp run_cluster_action({:drop_coordinator, group}, context) do
    with_store(context, :ok, &MetadataStore.forget_coordinator(&1.pid, group, &2))
  end

  defp coordinator_request?(%NodeSelector{strategy: :consumer_group}), do: true
  defp coordinator_request?(_), do: false

  defp sleep_for_reconnect do
    Process.sleep(Application.get_env(:kafka_ex, :sleep_for_reconnect, 400))
  end

  defp client_request(request), do: %{request | client_id: Config.client_id()}

  # Taken as a request span opens, one per span, so concurrent callers never report the same id.
  defp next_correlation_id(context), do: :atomics.add_get(context.correlation, 1, 1) - 1

  defp with_correlation_id({key, pid, dispatch}, id) do
    {key, pid, %{dispatch | request: %{dispatch.request | correlation_id: id}}}
  end

  # A missing table answers :stale_context, so the caller waits for one instead of failing the route.
  defp select_broker_with_update(context, selector) do
    case select(context, selector) do
      {:error, :unavailable} ->
        :stale_context

      {:error, _reason} ->
        selector
        |> ClusterMetadata.selection_miss_actions(allow_auto_topic_creation: context.allow_auto_topic_creation)
        |> run_cluster_actions(context)

        case select(context, selector) do
          {:error, :unavailable} ->
            :stale_context

          {:error, reason} ->
            # Debug, not warning: the consumer's fetch loop retries this lookup and owns the throttled log.
            Logger.debug("No broker for #{describe_selector(selector)} after metadata refresh: #{inspect(reason)}")
            nil

          {:ok, _node_id, broker} ->
            broker
        end

      {:ok, _node_id, broker} ->
        broker
    end
  end

  defp describe_selector(%NodeSelector{strategy: topic_partition_strategy, topic: topic, partition: partition})
       when topic_partition_strategy in [:topic_partition, :topic_partition_replica] do
    "#{topic}/#{partition}"
  end

  defp describe_selector(%NodeSelector{strategy: :consumer_group, consumer_group_name: group}) do
    "consumer group #{group}"
  end

  # Never the implicit 5000 default: a larger :request_timeout would exit the caller (#357).
  defp timeout_val(nil), do: RequestBudget.call_budget(Config.request_timeout())
  defp timeout_val(timeout) when is_integer(timeout), do: timeout

  defp network_request(request, node_selector, context, builder) do
    case attempt_request(request, builder, node_selector, context, nil) do
      :stale_context -> :stale_context
      {_sent, {tag, result}, _stamp} when tag in [:raised, :refused] -> result
      {_sent, outcome, _stamp} -> outcome
    end
  end

  # The outcome comes with the request last sent and the table stamp read before it; no retry fixes :raised or :refused.
  defp attempt_request(request, builder, node_selector, context, network_timeout) do
    timeout = network_timeout || Config.request_timeout()

    case targets(node_selector, request, context) do
      :stale_context ->
        :stale_context

      :no_broker ->
        {request, {:error, :no_broker}, nil}

      {:error, _} = error ->
        {request, error, nil}

      {:first_available, brokers} ->
        stamped(first_available({client_request(request), builder, node_selector, timeout}, brokers, context, nil), nil)

      {:one, broker} ->
        stamp = snapshot_stamp(context)
        stamped(send_to(client_request(request), builder, node_selector, broker, timeout, context), stamp)
    end
  end

  defp stamped(:stale_context, _stamp), do: :stale_context
  defp stamped({sent, outcome}, stamp), do: {sent, outcome, stamp}

  defp send_to(request, builder, selector, broker, timeout, context) do
    dispatch = request |> connection_dispatch(timeout, true, context) |> as_is(builder)

    case await_negotiated(dispatch, builder, context, &await_connection(&1, selector, broker, &2)) do
      {:ready, conn} ->
        {_key, _pid, %{request: sent}} = conn = with_correlation_id(conn, next_correlation_id(context))
        span = start_request_span(sent)
        {sent, opened(span, fn -> finish_request_span(span, write(conn), broker) end)}

      {:not_written, result} ->
        {request, result}

      {:refused, _result} = refused ->
        {request, refused}

      :stale_context ->
        :stale_context
    end
  end

  # A refused request was never written, so rebuilding it at the connection's versions costs no attempt.
  defp await_negotiated(dispatch, builder, context, await) do
    case await.(dispatch, context) do
      {:not_written, {:error, {:unsupported_version, versions}}} ->
        rebuild(dispatch, builder, versions, context, await)

      awaited ->
        awaited
    end
  end

  defp rebuild(dispatch, builder, versions, context, await) do
    case builder.(%{context | api_versions: versions}) do
      {:ok, request} -> dispatch |> carrying(client_request(request)) |> await.(context) |> refused_again()
      # An explicit version pin above this connection's max ends here, unwritten and never downgraded.
      {:error, _} = build_error -> {:refused, build_error}
    end
  end

  defp refused_again({:not_written, {:error, {:unsupported_version, _}}}) do
    {:refused, {:error, :api_version_no_supported}}
  end

  defp refused_again(awaited), do: awaited

  # The rest of the call runs under the open span, so a crash in it still ends the span.
  defp opened({metadata, span}, rest_of_call) do
    rest_of_call.()
  catch
    kind, reason ->
      Telemetry.exception_span(span, metadata, kind, reason, __STACKTRACE__)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp start_request_span(request) do
    metadata = Telemetry.request_metadata(request, %{})
    {metadata, Telemetry.start_span([:kafka_ex, :request], metadata)}
  end

  defp restamp({_metadata, span}, request), do: {Telemetry.request_metadata(request, %{}), span}

  defp finish_request_span({metadata, span}, {:raised, {kind, reason, stacktrace}}, _broker) do
    Telemetry.exception_span(span, metadata, kind, reason, stacktrace)
    {:raised, {:error, :timeout}}
  end

  defp finish_request_span(span, {:written, result, meta}, broker), do: stop_request_span(span, result, meta, broker)

  defp stop_request_span({metadata, span}, result, meta, broker) do
    stop = %{bytes_sent: meta.bytes_sent, bytes_received: meta.bytes_received, broker: broker_to_telemetry_info(broker)}
    Telemetry.stop_span(span, {result, Map.merge(metadata, stop)})
  end

  defp targets(%NodeSelector{strategy: :first_available} = selector, request, context) do
    {operation, _version} = @protocol.request_info(request)

    case NodeSelector.connection_role(selector, operation) do
      :metadata -> metadata_candidates(context)
      role -> context |> brokers() |> connected_first(context.conns, role)
    end
  end

  defp targets(%NodeSelector{strategy: strategy} = selector, _request, context)
       when strategy in [:topic_partition, :topic_partition_replica, :consumer_group] do
    case select_broker_with_update(context, selector) do
      # A refresh the deadline cut short found no route because of that deadline.
      nil -> if time_left(context.deadline) == 0, do: {:error, :timeout}, else: :no_broker
      :stale_context -> :stale_context
      broker -> {:one, broker}
    end
  end

  defp targets(selector, _request, context) do
    case select_known(context, selector) do
      {:ok, _node_id, broker} -> {:one, broker}
      {:error, :unavailable} -> :stale_context
      {:error, _} = error -> error
    end
  end

  # A booted table always lists a broker, so an empty one belongs to a store still loading or gone.
  defp select_known(context, %NodeSelector{strategy: strategy} = selector) when strategy in [:random, :controller] do
    if brokers(context) == [], do: {:error, :unavailable}, else: select(context, selector)
  end

  defp select_known(context, selector), do: select(context, selector)

  defp metadata_candidates(context) do
    case with_store(context, :stale_context, &MetadataStore.candidates(&1.pid, &2)) do
      :stale_context ->
        :stale_context

      addresses ->
        known = Map.new(brokers(context), &{{&1.host, &1.port}, &1})
        {:first_available, Enum.map(addresses, &Map.get_lazy(known, &1, fn -> bootstrap_broker(&1) end))}
    end
  end

  defp bootstrap_broker({host, port}), do: %Broker{host: host, port: port}

  # A booted table always lists a broker, so an empty list means the table went away.
  defp connected_first([], _conns, _role), do: :stale_context

  defp connected_first(brokers, conns, role) do
    {connected, rest} = Enum.split_with(brokers, &registered(conns, {&1.host, &1.port, role}))
    {:first_available, Enum.shuffle(connected) ++ Enum.shuffle(rest)}
  end

  defp first_available({request, _builder, _selector, _timeout}, [], _context, nil) do
    {request, {:error, :no_broker}}
  end

  defp first_available({request, _builder, _selector, _timeout}, [], _context, {:refused, refused}) do
    {request, {:refused, refused}}
  end

  # A walk the request's deadline cut short ends on the :timeout its spent budget stands for.
  defp first_available(_walk, [], context, {:open, span, meta, sent}) do
    error = if time_left(context.deadline) == 0, do: :timeout, else: :broker_failed
    {sent, stop_request_span(span, {:error, error}, %{meta | bytes_received: 0}, nil)}
  end

  defp first_available({request, builder, selector, timeout} = walk, [broker | rest], context, open) do
    dispatch = request |> connection_dispatch(timeout, false, context) |> as_is(builder)

    case await_negotiated(dispatch, builder, context, &await_connection(&1, selector, broker, &2)) do
      {:ready, conn} ->
        write_candidate(conn, broker, {walk, rest}, context, open)

      {:not_written, {:error, :no_broker}} ->
        Logger.debug("#{Broker.to_string(broker)} not connected, skipping")
        first_available(walk, rest, context, open)

      # A registry restarting under the walk skips the broker, as an unconnected one is skipped.
      :stale_context ->
        first_available(walk, rest, context, open)

      {:refused, {:error, error} = refused} ->
        Logger.warning("Network call to #{Broker.to_string(broker)} failed: #{inspect(error)}")
        first_available(walk, rest, context, refusal(open, refused))

      {:not_written, {:error, error}} ->
        Logger.warning("Network call to #{Broker.to_string(broker)} failed: #{inspect(error)}")
        first_available(walk, rest, context, open)
    end
  end

  # A walk that writes nothing ends on the refusal, not on a missing broker.
  defp refusal({:open, _span, _meta, _sent} = open, _refused), do: open
  defp refusal(_open, refused), do: {:refused, refused}

  # Every candidate of one walk writes under the walk's one span and correlation id.
  defp write_candidate(conn, broker, walk, context, {:open, span, _meta, %{correlation_id: id}}) do
    {_key, _pid, %{request: sent}} = conn = with_correlation_id(conn, id)
    walk_on(write(conn), restamp(span, sent), broker, sent, walk, context)
  end

  defp write_candidate(conn, broker, walk, context, _nothing_written) do
    {_key, _pid, %{request: sent}} = conn = with_correlation_id(conn, next_correlation_id(context))
    span = start_request_span(sent)
    opened(span, fn -> walk_on(write(conn), span, broker, sent, walk, context) end)
  end

  defp walk_on({:written, {:error, error}, meta}, span, broker, sent, {walk, rest}, context) do
    Logger.warning("Network call to #{Broker.to_string(broker)} failed: #{inspect(error)}")
    first_available(walk, rest, context, {:open, span, meta, sent})
  end

  defp walk_on(outcome, span, broker, sent, _walk, _context), do: {sent, finish_request_span(span, outcome, broker)}

  defp await_connection(dispatch, selector, broker, context) do
    {operation, _version} = dispatch.info
    key = {broker.host, broker.port, NodeSelector.connection_role(selector, operation)}
    await(key, Map.put(dispatch, :node_id, broker.node_id), context, 1)
  end

  # The caller waits this long past the connection's deadline, so its own timeout reply usually arrives first.
  @deadline_margin 500

  # Capped by the request's deadline, never shortened by the margin, so a short budget can still write.
  defp connection_dispatch(request, timeout, wait?, context) do
    own = System.monotonic_time(:millisecond) + call_timeout(timeout, context) - @deadline_margin
    carrying(%{timeout: timeout, wait?: wait?, deadline: min(own, context.deadline)}, request)
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

  defp call_timeout(timeout, context) do
    %{connect_timeout: connect_timeout, reconnect_window: reconnect_window} = context.connection_options
    connect_timeout + reconnect_window + timeout + 1_000
  end

  # Await and write share the dispatch's deadline, so the write never opens a second reconnect window.
  defp dispatch_time_left(dispatch), do: time_left(dispatch.deadline + @deadline_margin)

  defp fetch_max_wait({:fetch, _version}, request), do: request.max_wait_time
  defp fetch_max_wait(_info, _request), do: nil

  defp await(key, dispatch, context, redispatches) do
    case ensure_connection(context, key) do
      nil -> {:not_written, {:error, :no_broker}}
      :stale_context -> :stale_context
      {:error, :timeout} = timeout -> {:not_written, timeout}
      pid -> await_ready(key, pid, dispatch, context, redispatches)
    end
  end

  # An idle stop can race the call: an exit with :noproc or :normal means nothing was written.
  defp await_ready(key, pid, dispatch, context, redispatches) do
    case call_connection(&ConnectionStateMachine.await/3, pid, dispatch, dispatch_time_left(dispatch)) do
      :ok -> {:ready, {key, pid, dispatch}}
      :gone when redispatches > 0 -> await(key, dispatch, context, redispatches - 1)
      :gone -> {:not_written, {:error, :no_broker}}
      {:error, :not_connected, _meta} -> {:not_written, {:error, :no_broker}}
      {:error, reason, _meta} -> {:not_written, {:error, reason}}
    end
  end

  defp write({_key, pid, dispatch}) do
    case call_connection(&ConnectionStateMachine.write/3, pid, dispatch, dispatch_time_left(dispatch)) do
      :gone -> {:written, {:error, :closed}, @no_bytes}
      {:ok, :no_response, meta} -> {:written, {:ok, :no_response}, meta}
      {:ok, frame, meta} -> {:written, ResponseParser.deserialize(frame, dispatch.request), meta}
      {:error, {:exception, kind, reason, stacktrace}, _meta} -> {:raised, {kind, reason, stacktrace}}
      # A reconnect between the await and the write can land on a broker with other versions.
      {:error, {:unsupported_version, _}, meta} -> {:written, {:error, :api_version_no_supported}, meta}
      {:error, reason, meta} -> {:written, {:error, reason}, meta}
    end
  end

  defp call_connection(call, pid, dispatch, call_timeout) do
    call.(pid, dispatch, call_timeout)
  catch
    :exit, {reason, _call} when reason in [:noproc, :normal] -> :gone
    :exit, _reason -> {:error, :timeout, @no_bytes}
  end

  # The store answers nil while its connection supervisor restarts, and that restart takes the store along.
  defp ensure_connection(context, {host, port, :metadata}) do
    case with_store(context, :stale_context, &MetadataStore.connection(&1.pid, {host, port}, &2)) do
      {:ok, nil} -> :stale_context
      {:ok, pid} -> pid
      :stale_context -> :stale_context
    end
  end

  defp ensure_connection(context, key) do
    case registered(context.conns, key) do
      nil -> connect(context, key)
      pid -> pid
    end
  end

  # A dead pid is a miss: its row stays until the registry handles the :DOWN, and an idle stop can race the lookup.
  defp registered(conns, key) do
    case :ets.lookup(conns, key) do
      [{^key, pid}] -> if(Process.alive?(pid), do: pid)
      [] -> nil
    end
  rescue
    # The table dies with a restarting registry before the client sees the store's :DOWN.
    ArgumentError -> nil
  end

  # A missing registry means a stale context; one restarting connections leaves the broker unconnected.
  defp connect(context, key) do
    case ConnectionRegistry.connect(context.registry, key, min(@store_call_timeout, time_left(context.deadline))) do
      {:ok, pid} -> pid
      :restarting -> nil
    end
  catch
    # A wait the deadline cut short ends on :timeout, as a route the deadline cut short does.
    :exit, {:timeout, _call} -> if time_left(context.deadline) == 0, do: {:error, :timeout}, else: nil
    :exit, {reason, _call} when reason in [:noproc, :shutdown, :killed] -> :stale_context
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
