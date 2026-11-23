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
  alias KafkaEx.Client.MetadataLog
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestBudget
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.RequestContext
  alias KafkaEx.Client.ResponseParser
  alias KafkaEx.Client.State
  alias KafkaEx.Client.Supervisor, as: ClientSupervisor
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Messages.ConsumerGroupHeartbeat
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.FindCoordinator, as: FindCoordinatorMsg
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
    GenServer.call(server, {:network_request, request, node_selector}, timeout_val(timeout))
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

  # ApiVersions negotiation retry settings (issue #433)
  # Handles intermittent parse errors during initial connection
  @api_versions_max_retries 3
  @api_versions_retry_base_delay_ms 100

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
    :ok = validate_transport!(state.transport)
    :ok = validate_connection_opts!(state.connection_opts)

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

    brokers =
      state.bootstrap_uris
      |> Enum.with_index(1)
      |> Map.new(fn {{host, port}, key} -> {key, %Broker{host: host, port: port}} end)

    {:ok, supervisor} = ClientSupervisor.start_link()

    state = %{
      state
      | cluster_metadata: %ClusterMetadata{brokers: brokers},
        supervisor: supervisor,
        reconnect_backoff: ReconnectBackoff.new(),
        connection_options: ConnectionStateMachine.options(state.connection_opts)
    }

    try do
      {api_versions, state} = negotiate_at_boot(state)
      initial_topics = Keyword.get(args, :initial_topics, [])
      state = %{state | api_versions: api_versions}

      state =
        try do
          update_metadata(state, initial_topics)
        rescue
          e ->
            sleep_for_reconnect()
            Kernel.reraise(e, __STACKTRACE__)
        end

      {:ok, timer_ref} = :timer.send_interval(state.metadata_update_interval, :update_metadata)
      state = %{state | metadata_timer_ref: timer_ref}

      {:ok, state}
    rescue
      e ->
        stop_supervisor_after_failed_boot(supervisor)
        reraise e, __STACKTRACE__
    end
  end

  # Never raises, so the boot's own exception is the one reraised.
  defp stop_supervisor_after_failed_boot(supervisor) do
    supervisor
    |> boot_connections()
    |> Enum.map(fn pid -> {pid, Process.monitor(pid)} end)
    |> Enum.each(fn {pid, ref} ->
      ConnectionStateMachine.stop(pid, :init_error)

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

  defp boot_connections(supervisor) do
    case ClientSupervisor.connection_supervisor(supervisor) do
      nil ->
        []

      connections ->
        for {_id, pid, _type, _modules} <- DynamicSupervisor.which_children(connections), is_pid(pid), do: pid
    end
  end

  defp negotiate_at_boot(state, retries_left \\ @api_versions_max_retries) do
    case connect_bootstrap(state) do
      {:ok, api_versions, state} ->
        {api_versions, state}

      {:error, failures, state} ->
        case for({:api_versions, reason} <- Enum.reverse(failures), do: reason) do
          [] ->
            sleep_for_reconnect()
            raise "Brokers sockets are not opened"

          [reason | _] ->
            retry_api_versions(reason, state, retries_left)
        end
    end
  end

  defp connect_bootstrap(state) do
    Enum.reduce_while(state.bootstrap_uris, {:error, [], state}, fn {host, port}, {:error, failures, acc} ->
      {pid, acc} = ensure_connection(acc, {host, port, :metadata})

      case connect_at_boot(pid, acc) do
        {:ok, api_versions} -> {:halt, {:ok, api_versions, acc}}
        {:error, reason} -> {:cont, {:error, [reason | failures], acc}}
      end
    end)
  end

  defp connect_at_boot(pid, state) do
    ConnectionStateMachine.connect(pid, state.connection_options.connect_timeout + 1_000)
  catch
    :exit, reason -> {:error, {:connect, reason}}
  end

  # ApiVersions negotiation retry with exponential backoff (issue #433).
  defp retry_api_versions(error, state, retries_left) when retries_left > 0 do
    if Retry.transient_error?(error) do
      attempt = @api_versions_max_retries - retries_left
      delay = Retry.backoff_delay(attempt, @api_versions_retry_base_delay_ms)

      Logger.warning(
        "ApiVersions negotiation failed with #{inspect(error)}, " <>
          "retrying in #{delay}ms (#{retries_left - 1} retries left)"
      )

      Process.sleep(delay)
      negotiate_at_boot(state, retries_left - 1)
    else
      Logger.error("ApiVersions negotiation failed with non-retryable error: #{inspect(error)}")
      api_versions_failed!(error)
    end
  end

  defp retry_api_versions(error, _state, _retries_left) do
    Logger.error("ApiVersions negotiation failed after #{@api_versions_max_retries} attempts: #{inspect(error)}")
    api_versions_failed!(error)
  end

  @spec api_versions_failed!(term()) :: no_return()
  defp api_versions_failed!(:parse_error) do
    sleep_for_reconnect()

    raise "ApiVersions negotiation failed: unable to parse broker response after #{@api_versions_max_retries} retries. " <>
            "This may indicate network issues, broker instability, or protocol incompatibility."
  end

  defp api_versions_failed!(reason) do
    sleep_for_reconnect()

    raise "ApiVersions negotiation failed: #{inspect(reason)}. " <>
            "Check broker connectivity and network configuration."
  end

  @transport_functions [connect: 4, write: 2, read: 2, activate: 1, close: 1]

  defp validate_transport!(transport) do
    valid? =
      is_atom(transport) and Code.ensure_loaded?(transport) and
        Enum.all?(@transport_functions, fn {name, arity} -> function_exported?(transport, name, arity) end)

    if valid?, do: :ok, else: raise(ArgumentError, "invalid :transport #{inspect(transport)}")
  end

  defp validate_connection_opts!(opts) do
    valid? = Keyword.keyword?(opts) and Enum.all?(opts, &valid_connection_opt?/1)
    if valid?, do: :ok, else: raise(ArgumentError, "invalid :connection #{inspect(opts)}")
  end

  @positive_connection_opts [:connect_timeout, :idle_timeout, :send_timeout]
  @non_negative_connection_opts [:reconnect_window, :liveness_slack, :stable_period]

  defp valid_connection_opt?({key, value}) when key in @positive_connection_opts, do: is_integer(value) and value > 0

  defp valid_connection_opt?({key, value}) when key in @non_negative_connection_opts do
    is_integer(value) and value >= 0
  end

  defp valid_connection_opt?({:idle_jitter, value}), do: is_number(value) and value >= 0 and value <= 1
  defp valid_connection_opt?({:initial_correlation_id, value}), do: value in 0..2_147_483_647
  defp valid_connection_opt?({:client_id, value}), do: is_binary(value)
  defp valid_connection_opt?(_other), do: true

  @impl true
  def handle_call(:cluster_metadata, _from, state) do
    {:reply, {:ok, state.cluster_metadata}, state}
  end

  def handle_call(:correlation_id, _from, state) do
    {:reply, {:ok, state.correlation_id}, state}
  end

  def handle_call(:update_metadata, _from, state) do
    updated_state = update_metadata(state)
    {:reply, {:ok, updated_state.cluster_metadata}, updated_state}
  end

  def handle_call(
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

  def handle_call({:topic_metadata, topics, allow_topic_creation}, _from, state) do
    {topic_metadata, updated_state} = fetch_topics_metadata(state, topics, allow_topic_creation)

    {:reply, {:ok, topic_metadata}, updated_state}
  end

  def handle_call({:api_versions, opts}, _from, state) do
    case api_versions_request(opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  def handle_call({:metadata, topics, opts, _api_version}, _from, state) do
    case metadata_request(topics, opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  def handle_call({:list_offsets, [{topic, partitions_data}], opts}, _from, state) do
    case list_offset_request({topic, partitions_data}, opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  # Backward compatibility, to be deleted once we delete legacy code
  def handle_call({:offset, topic, partition, timestamp}, _from, state) do
    partition_data = %{partition_num: partition, timestamp: timestamp}

    case list_offset_request({topic, [partition_data]}, [], state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  def handle_call({:describe_groups, [consumer_group_name], opts}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group_name) do
      case describe_group_request(consumer_group_name, opts, state) do
        {:error, error} -> {:reply, {:error, error}, state}
        {result, updated_state} -> {:reply, result, updated_state}
      end
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:list_groups, node_id, opts}, _from, state) do
    case list_groups_request(node_id, opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  def handle_call({:offset_fetch, consumer_group, [{topic, partitions_data}], opts}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group) and is_binary(consumer_group) do
      case offset_fetch_request(consumer_group, {topic, partitions_data}, opts, state) do
        {:error, error} -> {:reply, {:error, error}, state}
        {result, updated_state} -> {:reply, result, updated_state}
      end
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:offset_commit, consumer_group, [{topic, partitions_data}], opts}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group) and is_binary(consumer_group) do
      case offset_commit_request(consumer_group, {topic, partitions_data}, opts, state) do
        {:error, error} -> {:reply, {:error, error}, state}
        {result, updated_state} -> {:reply, result, updated_state}
      end
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:heartbeat, consumer_group, member_id, generation_id, opts}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group) and is_binary(consumer_group) do
      case heartbeat_request(consumer_group, member_id, generation_id, opts, state) do
        {:error, error} -> {:reply, {:error, error}, state}
        {result, updated_state} -> {:reply, result, updated_state}
      end
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:join_group, consumer_group, member_id, opts}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group) and is_binary(consumer_group) do
      case join_group_request(consumer_group, member_id, opts, state) do
        {:error, error} -> {:reply, {:error, error}, state}
        {result, updated_state} -> {:reply, result, updated_state}
      end
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:leave_group, consumer_group, member_id, opts}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group) and is_binary(consumer_group) do
      case leave_group_request(consumer_group, member_id, opts, state) do
        {:error, error} -> {:reply, {:error, error}, state}
        {result, updated_state} -> {:reply, result, updated_state}
      end
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:sync_group, consumer_group, generation_id, member_id, opts}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group) and is_binary(consumer_group) do
      case sync_group_request(consumer_group, generation_id, member_id, opts, state) do
        {:error, error} -> {:reply, {:error, error}, state}
        {result, updated_state} -> {:reply, result, updated_state}
      end
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:consumer_group_heartbeat, consumer_group, opts}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group) and is_binary(consumer_group) do
      case consumer_group_heartbeat_request(consumer_group, opts, state) do
        {:error, error} -> {:reply, {:error, error}, state}
        {result, updated_state} -> {:reply, result, updated_state}
      end
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:consumer_group_describe, [consumer_group], opts}, _from, state) do
    if KafkaEx.valid_consumer_group?(consumer_group) and is_binary(consumer_group) do
      case consumer_group_describe_request(consumer_group, opts, state) do
        {:error, error} -> {:reply, {:error, error}, state}
        {result, updated_state} -> {:reply, result, updated_state}
      end
    else
      {:reply, {:error, :invalid_consumer_group}, state}
    end
  end

  def handle_call({:produce, topic, partition, messages, opts}, _from, state) do
    case produce_request(topic, partition, messages, opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  def handle_call({:fetch, topic, partition, offset, opts}, from, state) do
    case fetch_request(topic, partition, offset, opts, state) do
      {:parked, parked} -> {:noreply, park(parked, from)}
      outcome -> fetch_reply(outcome, state)
    end
  end

  def handle_call({:find_coordinator, group_id, opts}, _from, state) do
    case find_coordinator_request(group_id, opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  def handle_call({:create_topics, topics, timeout, opts}, _from, state) do
    case create_topics_request(topics, timeout, opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  def handle_call({:delete_topics, topics, timeout, opts}, _from, state) do
    case delete_topics_request(topics, timeout, opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  def handle_call({:network_request, request, node_selector}, _from, state) do
    {response, updated_state} = network_request(request, node_selector, state, :as_is)

    {:reply, response, updated_state}
  end

  def handle_call(:consumer_group, _from, state) do
    {:reply, state.consumer_group_for_auto_commit, state}
  end

  defp fetch_reply({:error, error}, state), do: {:reply, {:error, error}, %{state | parking: nil}}
  defp fetch_reply({result, %State{} = updated_state}, _state), do: {:reply, result, %{updated_state | parking: nil}}

  @max_metadata_update_retries 3

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

  defp unparked_info(:update_metadata, state) do
    {:noreply, update_metadata_with_retry(state, @max_metadata_update_retries)}
  end

  defp unparked_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, %{state | conns: Map.reject(state.conns, fn {_key, entry} -> entry.ref == ref end)}}
  end

  defp unparked_info(message, state) do
    Logger.warning("#{inspect(__MODULE__)} ignoring unexpected message: #{inspect(message)}")
    {:noreply, state}
  end

  defp update_metadata_with_retry(_state, retries_left) when retries_left <= 0 do
    raise KafkaEx.MetadataUpdateError, attempts: @max_metadata_update_retries
  end

  defp update_metadata_with_retry(state, retries_left) do
    update_metadata(state)
  rescue
    error ->
      Logger.warning("Periodic metadata update failed (#{retries_left - 1} retries left): #{inspect(error)}")
      Process.sleep(500)
      update_metadata_with_retry(state, retries_left - 1)
  end

  @impl true
  def terminate(reason, state) do
    Logger.debug("Shutting down worker #{inspect(worker_identity())}, reason: #{inspect(reason)}")
    for {_tag, entry} <- state.parked, do: Telemetry.exception_span(entry.span, entry.metadata, :exit, reason, [])
    if state.metadata_timer_ref, do: :timer.cancel(state.metadata_timer_ref), else: nil
    ClientSupervisor.stop(state.supervisor, :shutdown, 5_000)
  end

  defp worker_identity do
    case Process.info(self(), :registered_name) do
      {:registered_name, []} -> self()
      {:registered_name, name} -> name
    end
  end

  defp update_metadata(state, topics \\ []) do
    # Refresh the tracked (used) topics, not every known topic.
    tracked = MapSet.union(state.tracked_topics, MapSet.new(topics))
    state = %{state | tracked_topics: tracked}
    refresh_topics = MapSet.to_list(tracked)

    {updated_state, parsed_metadata} = retrieve_metadata(state, config_request_timeout(), refresh_topics)

    case parsed_metadata do
      nil ->
        updated_state

      %ClusterMetadata{} = new_cluster_metadata ->
        {updated_cluster_metadata, _brokers_to_close} =
          ClusterMetadata.merge_brokers(updated_state.cluster_metadata, new_cluster_metadata)

        stop_departed_connections(%{updated_state | cluster_metadata: updated_cluster_metadata})
    end
  end

  defp stop_departed_connections(state) do
    advertised = MapSet.new(State.brokers(state), &{&1.host, &1.port})

    {departed, kept} =
      Enum.split_with(state.conns, fn {{host, port, _role}, _entry} -> not MapSet.member?(advertised, {host, port}) end)

    Enum.each(departed, fn {_key, %{pid: pid, ref: ref}} ->
      Process.demonitor(ref, [:flush])
      ConnectionStateMachine.stop(pid, :metadata_update)
    end)

    %{state | conns: Map.new(kept)}
  end

  # ----------------------------------------------------------------------------------------------------
  defp describe_group_request(consumer_group_name, opts, state) do
    node_selector = NodeSelector.consumer_group(consumer_group_name)
    req_data = [{:group_names, [consumer_group_name]} | opts]

    builder = &RequestBuilder.describe_groups_request(req_data, &1)

    case builder.(state) do
      {:ok, request} -> handle_describe_group_request(request, builder, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp list_groups_request(node_id, opts, state) do
    node_selector = NodeSelector.node_id(node_id)

    builder = &RequestBuilder.list_groups_request(opts, &1)

    case builder.(state) do
      {:ok, request} -> handle_list_groups_request(request, builder, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp list_offset_request({topic, partitions_data}, opts, state) do
    [%{partition_num: partition_num}] = partitions_data
    node_selector = NodeSelector.topic_partition(topic, partition_num)
    req_data = [{:topics, [{topic, partitions_data}]} | opts]

    builder = &RequestBuilder.lists_offset_request(req_data, &1)

    case builder.(state) do
      {:ok, request} -> handle_lists_offsets_request(request, builder, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp offset_fetch_request(consumer_group, {topic, partitions_data}, opts, state) do
    node_selector = NodeSelector.consumer_group(consumer_group)
    req_data = [{:group_id, consumer_group}, {:topics, [{topic, partitions_data}]} | opts]

    builder = &RequestBuilder.offset_fetch_request(req_data, &1)

    case builder.(state) do
      {:ok, request} -> handle_offset_fetch_request(request, builder, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp offset_commit_request(consumer_group, {topic, partitions_data}, opts, state) do
    client_id = Config.client_id()
    partition_count = length(partitions_data)
    metadata = Telemetry.commit_metadata(consumer_group, client_id, topic, partition_count)

    Telemetry.span([:kafka_ex, :consumer, :commit], metadata, fn ->
      do_offset_commit_request(consumer_group, {topic, partitions_data}, opts, state, metadata)
    end)
  end

  defp do_offset_commit_request(consumer_group, {topic, partitions_data}, opts, state, metadata) do
    node_selector = NodeSelector.consumer_group(consumer_group)
    req_data = [{:group_id, consumer_group}, {:topics, [{topic, partitions_data}]} | opts]

    builder = &RequestBuilder.offset_commit_request(req_data, &1)

    with {:ok, request} <- builder.(state),
         {result, updated_state} <- handle_offset_commit_request(request, builder, node_selector, state) do
      {{result, updated_state}, metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
    end
  end

  defp heartbeat_request(consumer_group, member_id, generation_id, opts, state) do
    metadata = Telemetry.heartbeat_metadata(consumer_group, member_id, generation_id)

    Telemetry.span([:kafka_ex, :consumer, :heartbeat], metadata, fn ->
      do_heartbeat_request(consumer_group, member_id, generation_id, opts, state, metadata)
    end)
  end

  defp do_heartbeat_request(consumer_group, member_id, generation_id, opts, state, metadata) do
    node_selector = NodeSelector.consumer_group(consumer_group)
    {network_timeout, req_opts} = Keyword.pop(opts, :network_timeout)
    req_data = [{:group_id, consumer_group}, {:member_id, member_id}, {:generation_id, generation_id} | req_opts]

    builder = &RequestBuilder.heartbeat_request(req_data, &1)

    with {:ok, request} <- builder.(state),
         {result, updated_state} <- handle_heartbeat_request(request, builder, node_selector, state, network_timeout) do
      {{result, updated_state}, metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
    end
  end

  defp join_group_request(consumer_group, member_id, opts, state) do
    topics = Keyword.get(opts, :topics, [])
    metadata = Telemetry.join_group_metadata(consumer_group, member_id || "", topics)

    Telemetry.span([:kafka_ex, :consumer, :join], metadata, fn ->
      do_join_group_request(consumer_group, member_id, opts, state, metadata)
    end)
  end

  defp do_join_group_request(consumer_group, member_id, opts, state, metadata) do
    node_selector = NodeSelector.consumer_group(consumer_group)
    # :network_timeout is a control opt (derived in KafkaEx.API.join_group from
    # rebalance_timeout), not a protocol field — pop it before building req_data.
    {network_timeout, req_opts} = Keyword.pop(opts, :network_timeout)
    req_data = [{:group_id, consumer_group}, {:member_id, member_id} | req_opts]

    builder = &RequestBuilder.join_group_request(req_data, &1)

    with {:ok, request} <- builder.(state),
         {{:ok, result}, updated_state} <-
           handle_join_group_request(request, builder, node_selector, state, network_timeout) do
      stop_metadata = add_join_group_result_metadata(metadata, result)
      {{{:ok, result}, updated_state}, stop_metadata}
    else
      # KIP-394: JoinGroup V4+ requires two-step join.
      # First attempt with empty member_id returns :member_id_required
      # with an assigned member_id. Retry with that member_id.
      {{:error, %Error{error: :member_id_required, metadata: %{member_id: assigned_id}}}, updated_state} ->
        Logger.info("JoinGroup returned member_id_required (KIP-394), retrying with assigned member_id")
        do_join_group_request(consumer_group, assigned_id, opts, updated_state, metadata)

      {:error, error} ->
        {{:error, error}, metadata}

      error_result ->
        {error_result, metadata}
    end
  end

  defp add_join_group_result_metadata(metadata, result) do
    is_leader = result.leader_id == result.member_id

    metadata
    |> Map.put(:generation_id, result.generation_id)
    |> Map.put(:is_leader, is_leader)
  end

  defp leave_group_request(consumer_group, member_id, opts, state) do
    metadata = Telemetry.leave_group_metadata(consumer_group, member_id)

    Telemetry.span([:kafka_ex, :consumer, :leave], metadata, fn ->
      do_leave_group_request(consumer_group, member_id, opts, state, metadata)
    end)
  end

  defp do_leave_group_request(consumer_group, member_id, opts, state, metadata) do
    node_selector = NodeSelector.consumer_group(consumer_group)
    req_data = [{:group_id, consumer_group}, {:member_id, member_id} | opts]

    builder = &RequestBuilder.leave_group_request(req_data, &1)

    with {:ok, request} <- builder.(state),
         {result, updated_state} <- handle_leave_group_request(request, builder, node_selector, state) do
      {{result, updated_state}, metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
    end
  end

  defp sync_group_request(consumer_group, generation_id, member_id, opts, state) do
    # Leader sends non-empty group_assignment, followers send empty
    group_assignment = Keyword.get(opts, :group_assignment, [])
    is_leader = group_assignment != []
    metadata = Telemetry.sync_group_metadata(consumer_group, member_id, generation_id, is_leader)

    Telemetry.span([:kafka_ex, :consumer, :sync], metadata, fn ->
      do_sync_group_request(consumer_group, generation_id, member_id, opts, state, metadata)
    end)
  end

  defp do_sync_group_request(consumer_group, generation_id, member_id, opts, state, metadata) do
    node_selector = NodeSelector.consumer_group(consumer_group)
    # :network_timeout is a control opt (derived in KafkaEx.API.sync_group), not a
    # protocol field — pop it before building req_data.
    {network_timeout, req_opts} = Keyword.pop(opts, :network_timeout)
    req_data = [{:group_id, consumer_group}, {:generation_id, generation_id}, {:member_id, member_id} | req_opts]

    builder = &RequestBuilder.sync_group_request(req_data, &1)

    with {:ok, request} <- builder.(state),
         {{:ok, result}, updated_state} <-
           handle_sync_group_request(request, builder, node_selector, state, network_timeout) do
      assigned_partitions = count_assigned_partitions(result.partition_assignments)
      stop_metadata = Map.put(metadata, :assigned_partitions, assigned_partitions)
      {{{:ok, result}, updated_state}, stop_metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
      error_result -> {error_result, metadata}
    end
  end

  defp count_assigned_partitions(partition_assignments) do
    Enum.reduce(partition_assignments, 0, fn assignment, acc ->
      acc + length(assignment.partitions)
    end)
  end

  defp consumer_group_heartbeat_request(consumer_group, opts, state) do
    # Validate before touching opts: a missing or malformed field must be an
    # error, not an exception in this shared process (and not a telemetry
    # event for a request that is never sent).
    with :ok <- RequestBuilder.validate_consumer_group_heartbeat_opts(opts) do
      member_epoch = Keyword.fetch!(opts, :member_epoch)

      metadata =
        Telemetry.consumer_group_heartbeat_metadata(consumer_group, Keyword.get(opts, :member_id) || "", member_epoch)

      Telemetry.span([:kafka_ex, :consumer, :consumer_group_heartbeat], metadata, fn ->
        do_consumer_group_heartbeat_request(consumer_group, opts, state, metadata)
      end)
    end
  end

  defp do_consumer_group_heartbeat_request(consumer_group, opts, state, metadata) do
    node_selector = NodeSelector.consumer_group(consumer_group)
    {network_timeout, req_opts} = Keyword.pop(opts, :network_timeout)
    req_data = [{:group_id, consumer_group} | req_opts]

    case RequestBuilder.consumer_group_heartbeat_request(req_data, state) do
      {:ok, request} ->
        # A v1+ join without :member_id gets a generated id while the request is
        # built (KIP-1082). Report the id actually sent, also on failure, so a
        # caller whose join timed out can retry with the same id instead of
        # registering a second member.
        metadata = %{metadata | member_id: request.member_id}

        builder =
          &RequestBuilder.consumer_group_heartbeat_request(Keyword.put(req_data, :member_id, request.member_id), &1)

        case handle_consumer_group_heartbeat_request(request, builder, node_selector, state, network_timeout) do
          {{:ok, result}, updated_state} ->
            stop_metadata =
              Map.merge(metadata, %{
                member_id: result.member_id,
                new_member_epoch: result.member_epoch,
                assigned_partitions: ConsumerGroupHeartbeat.assigned_partition_count(result)
              })

            {{{:ok, result}, updated_state}, stop_metadata}

          error_result ->
            {error_result, metadata}
        end

      {:error, error} ->
        {{:error, error}, metadata}
    end
  end

  defp consumer_group_describe_request(consumer_group, opts, state) do
    node_selector = NodeSelector.consumer_group(consumer_group)
    req_data = [{:group_ids, [consumer_group]} | opts]

    builder = &RequestBuilder.consumer_group_describe_request(req_data, &1)

    case builder.(state) do
      {:ok, request} -> handle_consumer_group_describe_request(request, builder, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp produce_request(topic, partition, messages, opts, state) do
    with {:ok, acks} <- resolve_acks(opts) do
      metadata = Telemetry.produce_metadata(topic, partition, Config.client_id(), acks)
      start_measurements = %{message_count: length(messages)}

      Telemetry.span([:kafka_ex, :produce], Map.merge(metadata, start_measurements), fn ->
        do_produce_request(topic, partition, messages, Keyword.put(opts, :acks, acks), state, metadata)
      end)
    end
  end

  defp do_produce_request(topic, partition, messages, opts, state, metadata) do
    node_selector = NodeSelector.topic_partition(topic, partition)
    req_data = [{:topic, topic}, {:partition, partition}, {:messages, messages} | opts]

    builder = &RequestBuilder.produce_request(req_data, &1)

    with {:ok, request} <- builder.(state),
         {{:ok, result}, updated_state} <- handle_produce_request(request, builder, node_selector, state) do
      stop_metadata = add_offset_to_metadata(metadata, result)
      {{{:ok, result}, updated_state}, stop_metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
      error_result -> {error_result, metadata}
    end
  end

  # Reject a bad acks value here; unvalidated it reaches Kayrock's int16 encoder and crashes the client.
  defp resolve_acks(opts) do
    case Keyword.get(opts, :acks, Keyword.get(opts, :required_acks, -1)) do
      acks when acks in [-1, 0, 1] -> {:ok, acks}
      # Java/librdkafka spelling for "all in-sync replicas"; accept it as the -1 the wire carries.
      acks when acks in [:all, :any] -> {:ok, -1}
      _ -> {:error, :invalid_acks}
    end
  end

  defp add_offset_to_metadata(metadata, result) do
    case Map.get(result, :base_offset) do
      nil -> metadata
      offset -> Map.put(metadata, :offset, offset)
    end
  end

  defp fetch_request(topic, partition, offset, opts, state) do
    client_id = Config.client_id()
    metadata = Telemetry.fetch_metadata(topic, partition, offset, client_id)
    span = Telemetry.start_span([:kafka_ex, :fetch], metadata)
    spanned(span, metadata, %{state | parking: :first}, &do_fetch_request(topic, partition, offset, opts, &1, metadata))
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

  defp do_fetch_request(topic, partition, offset, opts, state, metadata) do
    node_selector = NodeSelector.topic_partition_replica(topic, partition)
    {network_timeout, req_opts} = Keyword.pop(opts, :network_timeout)
    req_data = [{:topic, topic}, {:partition, partition}, {:offset, offset} | req_opts]

    builder = &RequestBuilder.fetch_request(req_data, &1)

    with {:ok, request} <- builder.(state),
         {{:ok, result}, updated_state} <-
           fetch_with_leader_fallback(request, builder, node_selector, state, network_timeout) do
      filtered_result = Fetch.filter_from_offset(result, offset)
      message_count = length(Map.get(filtered_result, :records, []))
      stop_metadata = Map.put(metadata, :message_count, message_count)
      updated_state = cache_preferred_replica(updated_state, topic, partition, filtered_result)
      {{{:ok, filtered_result}, updated_state}, stop_metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
      error_result -> {error_result, metadata}
    end
  end

  # KIP-392: a replica's error is not authoritative (it may lag the leader), so the leader decides.
  defp fetch_with_leader_fallback(request, builder, selector, state, timeout) do
    case handle_fetch_request(request, builder, selector, state, timeout) do
      {{:error, error}, state_out} = result ->
        if reading_from_replica?(state_out.cluster_metadata, selector) do
          state_out = forget_preferred_replica(state_out, selector)
          after_replica_error(request, builder, selector, error, state_out, timeout)
        else
          result
        end

      result ->
        result
    end
  end

  # A single attempt keeps the caller's call budget, which covers one retry cycle.
  defp after_replica_error(request, builder, selector, error, state, timeout) do
    if Retry.data_plane_retryable?(extract_error_atom(error)) do
      {{:error, error}, state}
    else
      handle_fetch_request(request, builder, selector, state, timeout, 1)
    end
  end

  defp forget_preferred_replica(state, %NodeSelector{topic: topic, partition: partition}) do
    %{state | cluster_metadata: ClusterMetadata.put_preferred_replica(state.cluster_metadata, topic, partition, nil)}
  end

  defp reading_from_replica?(cluster_metadata, %NodeSelector{topic: topic, partition: partition} = selector) do
    ClusterMetadata.select_node(cluster_metadata, selector) !=
      ClusterMetadata.select_node(cluster_metadata, NodeSelector.topic_partition(topic, partition))
  end

  # `-1` means "stay where you are" (Java client semantics). Clearing the
  # cache here would ping-pong subsequent fetches back to the leader.
  defp cache_preferred_replica(state, _topic, _partition, %Fetch{preferred_read_replica: node_id})
       when node_id in [nil, -1] do
    state
  end

  defp cache_preferred_replica(state, topic, partition, %Fetch{preferred_read_replica: node_id})
       when is_integer(node_id) do
    %{
      state
      | cluster_metadata: ClusterMetadata.put_preferred_replica(state.cluster_metadata, topic, partition, node_id)
    }
  end

  defp cache_preferred_replica(state, _topic, _partition, _other), do: state

  defp find_coordinator_request(group_id, opts, state) do
    # FindCoordinator can be sent to any broker
    node_selector = NodeSelector.first_available()
    req_data = [{:group_id, group_id} | opts]

    builder = &RequestBuilder.find_coordinator_request(req_data, &1)

    case builder.(state) do
      {:ok, request} -> handle_find_coordinator_request(request, builder, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp create_topics_request(topics, timeout, opts, state) do
    # CreateTopics must be sent to the controller broker
    node_selector = NodeSelector.controller()
    {network_timeout, req_opts} = Keyword.pop(opts, :network_timeout)
    req_data = [{:topics, topics}, {:timeout, timeout} | req_opts]

    builder = &RequestBuilder.create_topics_request(req_data, &1)

    case builder.(state) do
      {:ok, request} -> handle_create_topics_request(request, builder, node_selector, state, network_timeout)
      {:error, error} -> {:error, error}
    end
  end

  defp delete_topics_request(topics, timeout, opts, state) do
    # DeleteTopics must be sent to the controller broker
    node_selector = NodeSelector.controller()
    {network_timeout, req_opts} = Keyword.pop(opts, :network_timeout)
    req_data = [{:topics, topics}, {:timeout, timeout} | req_opts]

    builder = &RequestBuilder.delete_topics_request(req_data, &1)

    case builder.(state) do
      {:ok, request} -> handle_delete_topics_request(request, builder, node_selector, state, network_timeout)
      {:error, error} -> {:error, error}
    end
  end

  defp api_versions_request(opts, state) do
    node_selector = NodeSelector.random()

    builder = &RequestBuilder.api_versions_request(opts, &1)

    case builder.(state) do
      {:ok, request} -> handle_api_versions_request(request, builder, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp metadata_request(topics, opts, state) do
    client_id = Config.client_id()
    topic_list = if is_list(topics), do: topics, else: [topics]
    metadata = Telemetry.metadata_update_metadata(client_id, topic_list)

    Telemetry.span([:kafka_ex, :metadata, :update], metadata, fn ->
      do_metadata_request(topics, opts, state)
    end)
  end

  defp do_metadata_request(topics, opts, state) do
    # Metadata can be fetched from any broker, use random selection
    node_selector = NodeSelector.random()
    req_data = [{:topics, topics} | opts]

    builder = &RequestBuilder.metadata_request(req_data, &1)

    case builder.(state) do
      {:ok, request} ->
        case handle_metadata_request(request, builder, node_selector, state) do
          {{:ok, cluster_metadata}, _updated_state} = result ->
            broker_count = map_size(cluster_metadata.brokers)
            topic_count = map_size(cluster_metadata.topics)
            {result, %{broker_count: broker_count, topic_count: topic_count}}

          error_result ->
            {error_result, %{}}
        end

      {:error, error} ->
        {{:error, error}, %{}}
    end
  end

  # ----------------------------------------------------------------------------------------------------
  defp handle_api_versions_request(request, builder, node_selector, state) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.api_versions_response/1,
      node_selector: node_selector
    }
    |> handle_request_with_retry(state)
  end

  defp handle_describe_group_request(request, builder, node_selector, state) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.describe_groups_response/1,
      node_selector: node_selector
    }
    |> handle_request_with_retry(state)
  end

  defp handle_list_groups_request(request, builder, node_selector, state) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.list_groups_response/1,
      node_selector: node_selector
    }
    |> handle_request_with_retry(state)
  end

  defp handle_lists_offsets_request(request, builder, node_selector, state) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.list_offsets_response/1,
      node_selector: node_selector
    }
    |> handle_request_with_retry(state)
  end

  defp handle_offset_fetch_request(request, builder, node_selector, state) do
    parse_opts = topic_name_parse_opts(state)

    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.offset_fetch_response(&1, parse_opts),
      node_selector: node_selector
    }
    |> handle_request_with_retry(state)
  end

  defp handle_offset_commit_request(request, builder, node_selector, state) do
    parse_opts = topic_name_parse_opts(state)

    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.offset_commit_response(&1, parse_opts),
      node_selector: node_selector,
      retryable?: &Retry.commit_retryable?/1
    }
    |> handle_request_with_retry(state)
  end

  # OffsetCommit/OffsetFetch v10+ responses name topics by id; resolve them
  # against the metadata the request's topic ids were taken from.
  defp topic_name_parse_opts(%State{cluster_metadata: cluster_metadata}) do
    [topic_name_resolver: &ClusterMetadata.topic_name_by_id(cluster_metadata, &1)]
  end

  defp handle_heartbeat_request(request, builder, node_selector, state, network_timeout) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.heartbeat_response/1,
      node_selector: node_selector,
      retryable?: &Retry.heartbeat_retryable?/1,
      network_timeout: network_timeout
    }
    |> handle_request_with_retry(state)
  end

  defp handle_consumer_group_heartbeat_request(request, builder, node_selector, state, network_timeout) do
    # ConsumerGroupHeartbeat responses name assigned topics by id; resolve them
    # against cluster metadata like OffsetCommit/OffsetFetch v10+.
    parse_opts = topic_name_parse_opts(state)

    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.consumer_group_heartbeat_response(&1, parse_opts),
      node_selector: node_selector,
      retryable?: &Retry.consumer_group_heartbeat_retryable?/1,
      send_once?: heartbeat_send_once(request),
      network_timeout: network_timeout,
      rediscover_coordinator?: true
    }
    |> handle_request_with_retry(state)
  end

  # Only a member heartbeat (epoch > 0) is incremental and fenced when
  # resent after the broker bumped the epoch. A join (0) and a leave (-1,
  # -2) are accepted again, so the client keeps retrying them.
  defp heartbeat_send_once(%{member_epoch: epoch}) when is_integer(epoch) and epoch > 0,
    do: &Retry.consumer_group_heartbeat_send_once?/1

  defp heartbeat_send_once(_request), do: &Retry.transport_timeout?/1

  defp handle_consumer_group_describe_request(request, builder, node_selector, state) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.consumer_group_describe_response/1,
      node_selector: node_selector
    }
    |> handle_request_with_retry(state)
  end

  defp handle_join_group_request(request, builder, node_selector, state, network_timeout) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.join_group_response/1,
      node_selector: node_selector,
      retryable?: &Retry.join_group_retryable?/1,
      network_timeout: network_timeout
    }
    |> handle_request_with_retry(state, @coordinator_max_attempts)
  end

  defp handle_leave_group_request(request, builder, node_selector, state) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.leave_group_response/1,
      node_selector: node_selector
    }
    |> handle_request_with_retry(state)
  end

  defp handle_sync_group_request(request, builder, node_selector, state, network_timeout) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.sync_group_response/1,
      node_selector: node_selector,
      retryable?: &Retry.sync_group_retryable?/1,
      network_timeout: network_timeout
    }
    |> handle_request_with_retry(state, @coordinator_max_attempts)
  end

  # acks=0: the broker sends no response, and a resend would duplicate records.
  defp handle_produce_request(%{acks: 0} = request, builder, node_selector, state) do
    %NodeSelector{topic: topic, partition: partition} = node_selector

    case network_request(request, node_selector, state, builder) do
      {{:ok, :no_response}, updated_state} ->
        {{:ok, RecordMetadata.build(topic: topic, partition: partition, base_offset: nil)}, updated_state}

      {{:error, reason}, updated_state} ->
        Logger.warning("Fire-and-forget produce to #{topic}/#{partition} failed with #{inspect(reason)}")
        {{:error, build_transport_error(reason)}, updated_state}
    end
  end

  defp handle_produce_request(request, builder, node_selector, state) do
    # Produce requests should only retry on leadership errors to avoid duplicates.
    # Timeout errors are NOT safe to retry because the message may have been written
    # but the response was lost. Use Kafka's idempotent producer for true exactly-once.
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.produce_response/1,
      node_selector: node_selector,
      retryable?: &Retry.produce_retryable?/1
    }
    |> handle_request_with_retry(state)
  end

  defp handle_fetch_request(request, builder, node_selector, state, network_timeout, attempts \\ @retry_count) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.fetch_response/1,
      node_selector: node_selector,
      network_timeout: network_timeout
    }
    |> handle_request_with_retry(state, attempts)
  end

  defp handle_find_coordinator_request(request, builder, node_selector, state) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.find_coordinator_response/1,
      node_selector: node_selector
    }
    |> handle_request_with_retry(state)
  end

  defp handle_create_topics_request(request, builder, node_selector, state, network_timeout) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.create_topics_response/1,
      node_selector: node_selector,
      network_timeout: network_timeout
    }
    |> handle_request_with_retry(state)
  end

  defp handle_delete_topics_request(request, builder, node_selector, state, network_timeout) do
    %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.delete_topics_response/1,
      node_selector: node_selector,
      network_timeout: network_timeout
    }
    |> handle_request_with_retry(state)
  end

  defp handle_metadata_request(request, builder, node_selector, state) do
    ctx = %RequestContext{
      request: request,
      builder: builder,
      parser_fn: &ResponseParser.metadata_response/1,
      node_selector: node_selector
    }

    case handle_request_with_retry(ctx, state) do
      {{:ok, cluster_metadata}, updated_state} ->
        # A targeted lookup lists only the requested topics, so merge instead of replacing the cached metadata.
        {{:ok, cluster_metadata}, merge_cluster_metadata(updated_state, cluster_metadata, request)}

      error_result ->
        error_result
    end
  end

  defp merge_cluster_metadata(state, %ClusterMetadata{} = new_metadata, request) do
    {merged, _brokers_to_close} = ClusterMetadata.merge_brokers(state.cluster_metadata, new_metadata)

    # A full request (no topics filter) lists every topic, so it replaces the
    # topic map (dropping deleted topics); a targeted one only adds or updates.
    topics =
      if all_topics_request?(request),
        do: new_metadata.topics,
        else: Map.merge(state.cluster_metadata.topics, new_metadata.topics)

    stop_departed_connections(%{state | cluster_metadata: %{merged | topics: topics}})
  end

  # Matches the built request. "All topics" is a null topic list from V1 on
  # (RequestHelpers.build_topics_list/1 sends both `nil` and `[]` as null), but
  # an empty list in V0, whose topic array is not nullable.
  defp all_topics_request?(%{topics: nil}), do: true
  defp all_topics_request?(%Kayrock.Metadata.V0.Request{topics: []}), do: true
  defp all_topics_request?(_request), do: false

  # ----------------------------------------------------------------------------------------------------
  # Request retry logic with optional retryable filter
  #
  # The `retryable?` field on the `%RequestContext{}` lets callers specify which
  # errors should trigger a retry. This is important for produce requests where
  # blind retries can cause duplicate messages. By default
  # (`Retry.data_plane_retryable?/1`), all errors are retried.
  #
  # For produce requests, the context sets `&Retry.produce_retryable?/1` to only retry leadership errors.
  # ----------------------------------------------------------------------------------------------------
  defp handle_request_with_retry(ctx, state, retry_count \\ @retry_count, last_error \\ nil)

  defp handle_request_with_retry(%RequestContext{}, state, 0, last_error) do
    {{:error, last_error}, state}
  end

  defp handle_request_with_retry(%RequestContext{} = ctx, state, retry_count, _last_error) do
    {%sent{}, outcome} = attempt_request(ctx.request, ctx.builder, ctx.node_selector, state, ctx.network_timeout)

    case outcome do
      {:refused, refused, state_out} ->
        {refused, state_out}

      {:raised, {:error, reason}, state_out} ->
        final = %{ctx | retryable?: fn _error -> false end, send_once?: fn _error -> false end}
        handle_request_error(final, state_out, retry_count, build_transport_error(reason), sent)

      {{:ok, response}, state_out} ->
        case ctx.parser_fn.(response) do
          {:ok, result} ->
            {{:ok, result}, state_out}

          {:error, [error | _]} ->
            handle_request_error(ctx, state_out, retry_count, error, sent)

          {:error, %Error{} = error} ->
            handle_request_error(ctx, state_out, retry_count, error, sent)
        end

      # The same refusal as an unwritten one, met when a reconnect lands between the await and the write.
      {{:error, :api_version_no_supported}, _state_out} = refused ->
        refused

      {{:error, reason}, state_out} ->
        handle_request_error(ctx, state_out, retry_count, build_transport_error(reason), sent)
    end
  end

  defp build_transport_error(reason) when is_atom(reason), do: Error.build(reason, %{})
  # A non-atom reason (SSL {:tls_alert, _}) would collapse to :unknown, which the fetch loop treats
  # as fatal; tag it retryable instead.
  defp build_transport_error(reason), do: Error.build(:transport_error, %{transport_reason: reason})

  defp handle_request_error(%RequestContext{} = ctx, state, retry_count, error, request_name) do
    error_atom = extract_error_atom(error)
    Logger.warning("Request #{inspect(request_name)} failed with error #{inspect(error_atom)}")

    cond do
      # Send-once only for coordinator requests (broker holds them for the rebalance/
      # session window; rejoin/commit re-issue higher up). Data-plane retries via ctx.retryable?.
      ctx.send_once?.(error_atom) and coordinator_request?(ctx.node_selector) ->
        Logger.info(
          "Coordinator request #{inspect(request_name)} failed with #{inspect(error_atom)}; " <>
            "not retrying at the client (send-once)"
        )

        {{:error, error}, act_on_error(error_atom, ctx, state)}

      # A stale topic id (OffsetCommit/OffsetFetch v10+, Metadata by id) is baked
      # into the built request, so resending `ctx.request` cannot succeed.
      # Refresh metadata and return: a caller that retries (e.g. GenConsumer's
      # commit retry) rebuilds the request from the fresh ids.
      error_atom == :unknown_topic_id ->
        {{:error, error}, act_on_error(error_atom, ctx, state)}

      ctx.retryable?.(error_atom) ->
        # A retry has written or failed an attempt, so it never parks.
        state = %{act_on_error(error_atom, ctx, state) | parking: nil}
        handle_request_with_retry(ctx, state, retry_count - 1, error)

      true ->
        Logger.info("Error #{inspect(error_atom)} is not retryable for #{inspect(request_name)}, failing immediately")
        {{:error, error}, state}
    end
  end

  defp extract_error_atom(%Error{error: error}), do: error
  defp extract_error_atom(error) when is_atom(error), do: error
  defp extract_error_atom({_group_id, error}) when is_atom(error), do: error
  defp extract_error_atom(_), do: :unknown

  defp act_on_error(error_atom, %RequestContext{} = ctx, state) do
    actions =
      ClusterMetadata.error_actions(error_atom, ctx.node_selector,
        rediscover_coordinator?: ctx.rediscover_coordinator?,
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
    default = state.allow_auto_topic_creation

    updated_state =
      update_metadata(%{state | allow_auto_topic_creation: refresh.allow_auto_topic_creation}, refresh.topics)

    %{updated_state | allow_auto_topic_creation: default}
  end

  defp run_cluster_action({:rediscover_coordinator, group}, state), do: update_consumer_group_coordinator(state, group)
  defp run_cluster_action({:drop_coordinator, group}, state), do: State.drop_consumer_group_coordinator(state, group)

  defp coordinator_request?(%NodeSelector{strategy: :consumer_group}), do: true
  defp coordinator_request?(_), do: false

  defp retrieve_metadata(state, request_timeout, topics) do
    retrieve_metadata(state, request_timeout, topics, @retry_count)
  end

  defp retrieve_metadata(state, request_timeout, topics, retry) do
    req_opts = [
      topics: topics,
      allow_auto_topic_creation: state.allow_auto_topic_creation
    ]

    builder = &RequestBuilder.metadata_request(req_opts, &1)

    with {:ok, request} <- builder.(state),
         {{:ok, response}, state_out} <-
           network_request(request, NodeSelector.first_available(), state, builder) do
      parse_metadata_response(response, state, state_out, request_timeout, topics, retry)
    else
      {:error, _} ->
        Logger.error("Unable to build metadata request.")
        {state, nil}

      {{:error, _}, state_out} ->
        Logger.error("Unable to fetch metadata from any brokers. Timeout is #{request_timeout}.")
        {state_out, nil}
    end
  end

  defp parse_metadata_response(response, state, state_out, request_timeout, topics, retry) do
    case ResponseParser.metadata_response(response) do
      {:ok, cluster_metadata} ->
        missing = MetadataLog.missing_topics(topics, cluster_metadata)
        has_new_missing? = Enum.any?(missing, &(not MapSet.member?(state.metadata_missing, &1)))

        cond do
          missing == [] ->
            {clear_metadata_missing(state_out), cluster_metadata}

          # Retry-sleep only for newly-missing topics; a known-missing one would otherwise stall every refresh.
          retry > 1 and has_new_missing? ->
            :timer.sleep(300)
            retrieve_metadata(state, request_timeout, topics, retry - 1)

          true ->
            # Merge what we got rather than discarding it (the liveness bug).
            {log_metadata_missing(state_out, missing), cluster_metadata}
        end

      {:error, _reason} ->
        Logger.error("Unable to parse metadata response from brokers.")
        {state_out, nil}
    end
  end

  defp clear_metadata_missing(%{metadata_missing: prev} = state) do
    if MapSet.size(prev) == 0 do
      state
    else
      Logger.info("Kafka metadata recovered for topics #{inspect(Enum.sort(prev))}")
      %{state | metadata_missing: MapSet.new(), metadata_missing_logged_at: nil}
    end
  end

  defp log_metadata_missing(state, missing) do
    missing_set = MapSet.new(missing)
    now = System.monotonic_time(:millisecond)

    if MetadataLog.should_log?(missing_set, state.metadata_missing, state.metadata_missing_logged_at, now) do
      Logger.warning(
        "Kafka metadata unavailable for topics #{inspect(Enum.sort(missing))} " <>
          "after #{@retry_count} attempts (topic unknown or leader not available)"
      )

      %{state | metadata_missing: missing_set, metadata_missing_logged_at: now}
    else
      %{state | metadata_missing: missing_set}
    end
  end

  defp sleep_for_reconnect do
    Process.sleep(Application.get_env(:kafka_ex, :sleep_for_reconnect, 400))
  end

  defp client_request(request, state) do
    %{request | client_id: Config.client_id(), correlation_id: state.correlation_id}
  end

  defp select_broker_with_update(state, selector) do
    case State.select_broker_entry(state, selector) do
      {:error, _} ->
        updated_state =
          selector
          |> ClusterMetadata.selection_miss_actions(allow_auto_topic_creation: state.allow_auto_topic_creation)
          |> run_cluster_actions(state)

        case State.select_broker_entry(updated_state, selector) do
          {:error, reason} ->
            # Debug, not warning: the consumer's fetch loop retries this lookup and owns the throttled log.
            Logger.debug("No broker for #{describe_selector(selector)} after metadata refresh: #{inspect(reason)}")

            {nil, updated_state}

          {:ok, _key, broker} ->
            {broker, updated_state}
        end

      {:ok, _key, broker} ->
        {broker, state}
    end
  end

  defp describe_selector(%NodeSelector{strategy: topic_partition_strategy, topic: topic, partition: partition})
       when topic_partition_strategy in [:topic_partition, :topic_partition_replica],
       do: "#{topic}/#{partition}"

  defp describe_selector(%NodeSelector{strategy: :consumer_group, consumer_group_name: group}),
    do: "consumer group #{group}"

  defp update_consumer_group_coordinator(state, consumer_group) do
    builder = &RequestBuilder.find_coordinator_request([group_id: consumer_group], &1)

    with {:ok, request} <- builder.(state),
         {{:ok, raw_response}, updated_state} <-
           network_request(request, NodeSelector.first_available(), state, builder) do
      parse_coordinator_response(raw_response, updated_state, consumer_group)
    else
      {:error, error} ->
        Logger.warning(
          "Unable to build find_coordinator request for #{inspect(consumer_group)}: Error #{inspect(error)}"
        )

        state

      {{:error, error}, updated_state} ->
        Logger.warning(
          "Unable to find consumer group coordinator for #{inspect(consumer_group)}: Error #{inspect(error)}"
        )

        updated_state
    end
  end

  defp parse_coordinator_response(raw_response, updated_state, consumer_group) do
    case ResponseParser.find_coordinator_response(raw_response) do
      {:ok, %FindCoordinatorMsg{} = result} ->
        node_id = FindCoordinatorMsg.coordinator_node_id(result)
        State.put_consumer_group_coordinator(updated_state, consumer_group, node_id)

      {:error, error} ->
        Logger.warning(
          "Unable to find consumer group coordinator for #{inspect(consumer_group)}: Error #{inspect(error)}"
        )

        updated_state
    end
  end

  # send_request/4 is send-once; budget from :request_timeout, never the implicit
  # 5000 default (a larger :request_timeout would exit the caller; #357).
  defp timeout_val(nil), do: RequestBudget.call_budget(Config.request_timeout())
  defp timeout_val(timeout) when is_integer(timeout), do: timeout

  # The per-attempt socket-recv deadline for synchronous requests that do not
  # set their own `network_timeout` (metadata, offset, heartbeat, produce ack,
  # …). Resolves via `Config.request_timeout/0` (`:request_timeout` > deprecated
  # `:sync_timeout` > default). JoinGroup/SyncGroup pass an explicit timeout.
  defp config_request_timeout(timeout \\ nil) do
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

    case targets(node_selector, state) do
      {:no_broker, state} ->
        {request, {{:error, :no_broker}, state}}

      {{:error, _} = error, state} ->
        {request, {error, state}}

      {:first_available, brokers, state} ->
        first_available({client_request(request, state), builder, node_selector, timeout}, brokers, state, nil)

      {:one, broker, state} ->
        send_to(client_request(request, state), builder, node_selector, broker, timeout, state)
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

  defp targets(%NodeSelector{strategy: :first_available}, state) do
    {known, rest} =
      state |> State.brokers() |> Enum.split_with(&Map.has_key?(state.conns, {&1.host, &1.port, :metadata}))

    {:first_available, Enum.shuffle(known) ++ Enum.shuffle(rest), state}
  end

  defp targets(%NodeSelector{strategy: strategy} = selector, state)
       when strategy in [:topic_partition, :topic_partition_replica, :consumer_group] do
    case select_broker_with_update(state, selector) do
      {nil, state} -> {:no_broker, state}
      {broker, state} -> {:one, broker, state}
    end
  end

  defp targets(selector, state) do
    case State.select_broker_entry(state, selector) do
      {:ok, _key, broker} -> {:one, broker, state}
      {:error, _} = error -> {error, state}
    end
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

    case await_negotiated(dispatch, builder, state, &await_connection(&1, selector, broker, &2)) do
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

  defp write_candidate({_key, _pid, %{request: sent}} = conn, broker, walk, state, {:open, span, _meta, _sent}) do
    walk_on(write(conn, state), restamp(span, sent), broker, sent, walk)
  end

  defp write_candidate({_key, _pid, %{request: sent}} = conn, broker, walk, state, _nothing_written) do
    span = start_request_span(sent)
    opened(span, fn -> walk_on(write(conn, state), span, broker, sent, walk) end)
  end

  defp walk_on({:written, {:error, error}, meta, state}, span, broker, sent, {walk, rest}) when error != :parse_error do
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
    entry = parked |> Map.drop([:dispatch, :state]) |> Map.merge(%{from: from, request: request, timer: timer})
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
    state = %{state | parked: parked, parking: {:resumed, entry.key, entry.pid, outcome}}
    {:reply, reply, state} = fetch_reply(entry.run.(state), state)
    GenServer.reply(entry.from, reply)
    state
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
      {:ok, frame, meta} -> {:written, deserialize(frame, dispatch.request), meta, state}
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

  defp ensure_connection(state, {host, port, role} = key) do
    case Map.fetch(state.conns, key) do
      {:ok, %{pid: pid}} ->
        {pid, state}

      :error ->
        args = %{
          host: host,
          port: port,
          role: role,
          use_ssl: state.use_ssl,
          transport: state.transport,
          secrets: secrets_fun(state),
          reconnect_backoff: state.reconnect_backoff,
          opts: connection_opts(state)
        }

        case start_connection(state.supervisor, args) do
          {:ok, pid} -> {pid, %{state | conns: Map.put(state.conns, key, %{pid: pid, ref: Process.monitor(pid)})}}
          :restarting -> {nil, state}
        end
    end
  end

  # While the client supervisor restarts the connection supervisor there is none, or a dying one, to start under.
  defp start_connection(supervisor, args) do
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

  defp connection_opts(state) do
    Keyword.merge([client_id: Config.client_id()], state.connection_opts)
  end

  defp broker_to_telemetry_info(nil), do: %{}

  defp broker_to_telemetry_info(%Broker{} = broker) do
    %{node_id: broker.node_id, host: broker.host, port: broker.port, rack: broker.rack}
  end

  defp deserialize(data, request) do
    {resp, _} = @protocol.response_deserializer(request).(data)
    {:ok, resp}
  rescue
    error ->
      Logger.error(
        "Failed to parse a response from the server: " <>
          inspect(data, limit: :infinity) <>
          " for request #{inspect(request, limit: :infinity)} " <>
          "error: #{inspect(error)}"
      )

      {:error, :parse_error}
  end

  defp fetch_topics_metadata(state, topics, allow_topic_creation) do
    allow_auto_topic_creation = state.allow_auto_topic_creation
    updated_state = update_metadata(%{state | allow_auto_topic_creation: allow_topic_creation}, topics)
    topic_metadata = State.topics_metadata(updated_state, topics)

    {topic_metadata, %{updated_state | allow_auto_topic_creation: allow_auto_topic_creation}}
  end
end
