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
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Telemetry

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.ResponseParser
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata

  alias Kayrock.ApiVersions
  alias Kayrock.ErrorCode
  alias Kayrock.FindCoordinator
  alias Kayrock.Metadata
  alias Kayrock.Request

  use GenServer

  @type args :: [KafkaEx.worker_setting() | {:allow_auto_topic_creation, boolean}]

  @doc """
  Start the server in a supervision tree
  """
  @spec start_link(args, atom) :: GenServer.on_start()
  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args, nil])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], name: name)
  end

  @doc """
  Send a Kayrock request to the appropriate broker
  Broker metadata will be updated if necessary
  """
  @spec send_request(KafkaEx.API.client(), map, KafkaEx.Client.NodeSelector.t(), pos_integer | nil) ::
          {:ok, term} | {:error, term}
  def send_request(server, request, node_selector, timeout \\ nil) do
    GenServer.call(server, {:kayrock_request, request, node_selector}, timeout_val(timeout))
  end

  require Logger

  # Default from GenServer
  @default_call_timeout 5_000
  @retry_count 3
  @sync_timeout 1_000
  @reconnect_max_retries 3
  @reconnect_delay_ms 500

  @impl true
  def init([args, name]) do
    state = State.static_init(args, name || self())

    unless KafkaEx.valid_consumer_group?(state.consumer_group_for_auto_commit) do
      raise KafkaEx.InvalidConsumerGroupError,
            state.consumer_group_for_auto_commit
    end

    brokers =
      state.bootstrap_uris
      |> Enum.with_index()
      |> Enum.into(%{}, fn {{host, port}, ix} ->
        {ix + 1,
         %Broker{
           host: host,
           port: port,
           socket: NetworkClient.create_socket(host, port, state.ssl_options, state.use_ssl, state.auth)
         }}
      end)

    state = %{state | cluster_metadata: %ClusterMetadata{brokers: brokers}}

    # Wrap remaining init in try to ensure sockets are closed on failure
    try do
      check_brokers_sockets!(brokers)

      {ok_or_err, api_versions, state} = get_api_versions(state)

      if ok_or_err == :error do
        sleep_for_reconnect()
        raise "Brokers sockets are closed"
      end

      :no_error = ErrorCode.code_to_atom(api_versions.error_code)

      initial_topics = Keyword.get(args, :initial_topics, [])

      state = State.ingest_api_versions(state, api_versions)

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
        # Close all sockets before re-raising to prevent resource leak
        close_all_sockets(state)
        reraise e, __STACKTRACE__
    end
  end

  defp close_all_sockets(state) do
    Enum.each(State.brokers(state), fn broker ->
      NetworkClient.close_socket(broker, broker.socket, :init_error)
    end)
  end

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

  def handle_call({:produce, topic, partition, messages, opts}, _from, state) do
    case produce_request(topic, partition, messages, opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
    end
  end

  def handle_call({:fetch, topic, partition, offset, opts}, _from, state) do
    case fetch_request(topic, partition, offset, opts, state) do
      {:error, error} -> {:reply, {:error, error}, state}
      {result, updated_state} -> {:reply, result, updated_state}
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

  def handle_call({:kayrock_request, request, node_selector}, _from, state) do
    {response, updated_state} = kayrock_network_request(request, node_selector, state)

    {:reply, response, updated_state}
  end

  # Simple handler for consumer group name retrieval
  def handle_call(:consumer_group, _from, state) do
    {:reply, state.consumer_group_for_auto_commit, state}
  end

  @max_metadata_update_retries 3

  @impl true
  def handle_info(:update_metadata, state) do
    {:noreply, update_metadata_with_retry(state, @max_metadata_update_retries)}
  end

  def handle_info({:tcp_closed, socket}, state) do
    state_out = close_broker_by_socket(state, socket)
    {:noreply, state_out}
  end

  def handle_info({:ssl_closed, socket}, state) do
    state_out = close_broker_by_socket(state, socket)
    {:noreply, state_out}
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
    Logger.debug("Shutting down worker #{inspect(state.worker_name)}, reason: #{inspect(reason)}")
    if state.metadata_timer_ref, do: :timer.cancel(state.metadata_timer_ref), else: nil
    Enum.each(State.brokers(state), &NetworkClient.close_socket(&1, &1.socket, :shutdown))
  end

  defp update_metadata(state, topics \\ []) do
    # make sure we update metadata about known topics
    known_topics = ClusterMetadata.known_topics(state.cluster_metadata)
    topics = Enum.uniq(known_topics ++ topics)

    {updated_state, response} = retrieve_metadata(state, config_sync_timeout(), topics)

    case response do
      nil ->
        updated_state

      _ ->
        new_cluster_metadata = ClusterMetadata.from_metadata_v1_response(response)

        {updated_cluster_metadata, brokers_to_close} =
          ClusterMetadata.merge_brokers(updated_state.cluster_metadata, new_cluster_metadata)

        :ok = Enum.each(brokers_to_close, &NetworkClient.close_socket(&1, &1.socket, :metadata_update))

        state_with_meta = %{updated_state | cluster_metadata: updated_cluster_metadata}
        updated_state = State.update_brokers(state_with_meta, &maybe_connect_broker(&1, state))
        updated_state
    end
  end

  # ----------------------------------------------------------------------------------------------------
  defp describe_group_request(consumer_group_name, opts, state) do
    node_selector = NodeSelector.consumer_group(consumer_group_name)
    req_data = [{:group_names, [consumer_group_name]} | opts]

    case RequestBuilder.describe_groups_request(req_data, state) do
      {:ok, request} -> handle_describe_group_request(request, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp list_offset_request({topic, partitions_data}, opts, state) do
    [%{partition_num: partition_num}] = partitions_data
    node_selector = NodeSelector.topic_partition(topic, partition_num)
    req_data = [{:topics, [{topic, partitions_data}]} | opts]

    case RequestBuilder.lists_offset_request(req_data, state) do
      {:ok, request} -> handle_lists_offsets_request(request, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp offset_fetch_request(consumer_group, {topic, partitions_data}, opts, state) do
    node_selector = NodeSelector.consumer_group(consumer_group)
    req_data = [{:group_id, consumer_group}, {:topics, [{topic, partitions_data}]} | opts]

    case RequestBuilder.offset_fetch_request(req_data, state) do
      {:ok, request} -> handle_offset_fetch_request(request, node_selector, state)
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

    with {:ok, request} <- RequestBuilder.offset_commit_request(req_data, state),
         {result, updated_state} <- handle_offset_commit_request(request, node_selector, state) do
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
    req_data = [{:group_id, consumer_group}, {:member_id, member_id}, {:generation_id, generation_id} | opts]

    with {:ok, request} <- RequestBuilder.heartbeat_request(req_data, state),
         {result, updated_state} <- handle_heartbeat_request(request, node_selector, state) do
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
    req_data = [{:group_id, consumer_group}, {:member_id, member_id} | opts]

    with {:ok, request} <- RequestBuilder.join_group_request(req_data, state),
         {{:ok, result}, updated_state} <- handle_join_group_request(request, node_selector, state) do
      stop_metadata = add_join_group_result_metadata(metadata, result)
      {{{:ok, result}, updated_state}, stop_metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
      error_result -> {error_result, metadata}
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

    with {:ok, request} <- RequestBuilder.leave_group_request(req_data, state),
         {result, updated_state} <- handle_leave_group_request(request, node_selector, state) do
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
    req_data = [{:group_id, consumer_group}, {:generation_id, generation_id}, {:member_id, member_id} | opts]

    with {:ok, request} <- RequestBuilder.sync_group_request(req_data, state),
         {{:ok, result}, updated_state} <- handle_sync_group_request(request, node_selector, state) do
      assigned_partitions = count_assigned_partitions(result.partition_assignments)
      stop_metadata = Map.put(metadata, :assigned_partitions, assigned_partitions)
      {{{:ok, result}, updated_state}, stop_metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
      error_result -> {error_result, metadata}
    end
  end

  defp count_assigned_partitions(partition_assignments) do
    Enum.reduce(partition_assignments, &(&2 + length(&1.partitions)))
  end

  defp produce_request(topic, partition, messages, opts, state) do
    message_count = length(messages)
    required_acks = Keyword.get(opts, :required_acks, 1)
    client_id = Config.client_id()

    metadata = Telemetry.produce_metadata(topic, partition, client_id, required_acks)
    start_measurements = %{message_count: message_count}

    Telemetry.span([:kafka_ex, :produce], Map.merge(metadata, start_measurements), fn ->
      do_produce_request(topic, partition, messages, opts, state, metadata)
    end)
  end

  defp do_produce_request(topic, partition, messages, opts, state, metadata) do
    node_selector = NodeSelector.topic_partition(topic, partition)
    req_data = [{:topic, topic}, {:partition, partition}, {:messages, messages} | opts]

    with {:ok, request} <- RequestBuilder.produce_request(req_data, state),
         {{:ok, result}, updated_state} <- handle_produce_request(request, node_selector, state) do
      stop_metadata = add_offset_to_metadata(metadata, result)
      {{{:ok, result}, updated_state}, stop_metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
      error_result -> {error_result, metadata}
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

    Telemetry.span([:kafka_ex, :fetch], metadata, fn ->
      do_fetch_request(topic, partition, offset, opts, state, metadata)
    end)
  end

  defp do_fetch_request(topic, partition, offset, opts, state, metadata) do
    node_selector = NodeSelector.topic_partition(topic, partition)
    req_data = [{:topic, topic}, {:partition, partition}, {:offset, offset} | opts]

    with {:ok, request} <- RequestBuilder.fetch_request(req_data, state),
         {{:ok, result}, updated_state} <- handle_fetch_request(request, node_selector, state) do
      message_count = length(Map.get(result, :records, []))
      stop_metadata = Map.put(metadata, :message_count, message_count)
      {{{:ok, result}, updated_state}, stop_metadata}
    else
      {:error, error} -> {{:error, error}, metadata}
      error_result -> {error_result, metadata}
    end
  end

  defp find_coordinator_request(group_id, opts, state) do
    # FindCoordinator can be sent to any broker
    node_selector = NodeSelector.first_available()
    req_data = [{:group_id, group_id} | opts]

    case RequestBuilder.find_coordinator_request(req_data, state) do
      {:ok, request} -> handle_find_coordinator_request(request, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp create_topics_request(topics, timeout, opts, state) do
    # CreateTopics must be sent to the controller broker
    node_selector = NodeSelector.controller()
    req_data = [{:topics, topics}, {:timeout, timeout} | opts]

    case RequestBuilder.create_topics_request(req_data, state) do
      {:ok, request} -> handle_create_topics_request(request, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp delete_topics_request(topics, timeout, opts, state) do
    # DeleteTopics must be sent to the controller broker
    node_selector = NodeSelector.controller()
    req_data = [{:topics, topics}, {:timeout, timeout} | opts]

    case RequestBuilder.delete_topics_request(req_data, state) do
      {:ok, request} -> handle_delete_topics_request(request, node_selector, state)
      {:error, error} -> {:error, error}
    end
  end

  defp api_versions_request(opts, state) do
    node_selector = NodeSelector.random()

    case RequestBuilder.api_versions_request(opts, state) do
      {:ok, request} -> handle_api_versions_request(request, node_selector, state)
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

    case RequestBuilder.metadata_request(req_data, state) do
      {:ok, request} ->
        case handle_metadata_request(request, node_selector, state) do
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
  defp handle_api_versions_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.api_versions_response/1, node_selector, state)
  end

  defp handle_describe_group_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.describe_groups_response/1, node_selector, state)
  end

  defp handle_lists_offsets_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.list_offsets_response/1, node_selector, state)
  end

  defp handle_offset_fetch_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.offset_fetch_response/1, node_selector, state)
  end

  defp handle_offset_commit_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.offset_commit_response/1, node_selector, state)
  end

  defp handle_heartbeat_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.heartbeat_response/1, node_selector, state)
  end

  defp handle_join_group_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.join_group_response/1, node_selector, state)
  end

  defp handle_leave_group_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.leave_group_response/1, node_selector, state)
  end

  defp handle_sync_group_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.sync_group_response/1, node_selector, state)
  end

  defp handle_produce_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.produce_response/1, node_selector, state)
  end

  defp handle_fetch_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.fetch_response/1, node_selector, state)
  end

  defp handle_find_coordinator_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.find_coordinator_response/1, node_selector, state)
  end

  defp handle_create_topics_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.create_topics_response/1, node_selector, state)
  end

  defp handle_delete_topics_request(request, node_selector, state) do
    handle_request_with_retry(request, &ResponseParser.delete_topics_response/1, node_selector, state)
  end

  defp handle_metadata_request(request, node_selector, state) do
    case handle_request_with_retry(request, &ResponseParser.metadata_response/1, node_selector, state) do
      {{:ok, cluster_metadata}, updated_state} ->
        merged_state = %{updated_state | cluster_metadata: cluster_metadata}
        {{:ok, cluster_metadata}, merged_state}

      error_result ->
        error_result
    end
  end

  # ----------------------------------------------------------------------------------------------------
  defp handle_request_with_retry(_, _, _, _, retry_count \\ @retry_count, _last_error \\ nil)

  defp handle_request_with_retry(_, _, _, state, 0, last_error) do
    {{:error, last_error}, state}
  end

  defp handle_request_with_retry(request, parser_fn, node_selector, state, retry_count, _last_error) do
    case kayrock_network_request(request, node_selector, state) do
      {{:ok, response}, state_out} ->
        case parser_fn.(response) do
          {:ok, result} ->
            {{:ok, result}, state_out}

          {:error, [error | _]} ->
            request_name = request.__struct__
            Logger.warning("Unable to send request #{inspect(request_name)}, failed with error #{inspect(error)}")
            handle_request_with_retry(request, parser_fn, node_selector, state, retry_count - 1, error)

          {:error, %Error{} = error} ->
            request_name = request.__struct__
            Logger.warning("Unable to send request #{inspect(request_name)}, failed with error #{inspect(error)}")
            handle_request_with_retry(request, parser_fn, node_selector, state, retry_count - 1, error)
        end

      {_, _state_out} ->
        request_name = request.__struct__
        Logger.warning("Unable to send request #{inspect(request_name)}, failed with error unknown")
        error = Error.build(:unknown, %{})
        handle_request_with_retry(request, parser_fn, node_selector, state, retry_count - 1, error)
    end
  end

  # ----------------------------------------------------------------------------------------------------
  defp maybe_connect_broker(broker, state) do
    case Broker.connected?(broker) do
      true ->
        broker

      false ->
        # Close existing socket if present to prevent resource leak
        NetworkClient.close_socket(broker, broker.socket, :reconnecting)

        socket = NetworkClient.create_socket(broker.host, broker.port, state.ssl_options, state.use_ssl, state.auth)
        %{broker | socket: socket}
    end
  end

  # Attempts to reconnect a broker with retries.
  # Returns updated broker struct (with socket or nil if all retries failed).
  defp reconnect_broker(broker, state, retry_count \\ 0)

  defp reconnect_broker(broker, state, retry_count) when retry_count < @reconnect_max_retries do
    Logger.info("Reconnecting to #{Broker.to_string(broker)}, attempt #{retry_count + 1}/#{@reconnect_max_retries}")

    case NetworkClient.create_socket(broker.host, broker.port, state.ssl_options, state.use_ssl, state.auth) do
      nil ->
        Process.sleep(@reconnect_delay_ms)
        reconnect_broker(broker, state, retry_count + 1)

      socket ->
        Logger.info("Successfully reconnected to #{Broker.to_string(broker)}")
        %{broker | socket: socket}
    end
  end

  defp reconnect_broker(broker, _state, _retry_count) do
    Logger.warning("Failed to reconnect to #{Broker.to_string(broker)} after #{@reconnect_max_retries} attempts")
    broker
  end

  defp retrieve_metadata(state, sync_timeout, topics) do
    retrieve_metadata(state, sync_timeout, topics, @retry_count, 0)
  end

  defp retrieve_metadata(state, _sync_timeout, topics, 0, error_code) do
    Logger.log(:error, "Metadata request for topics #{inspect(topics)} failed with error_code #{inspect(error_code)}")
    {state, nil}
  end

  defp retrieve_metadata(state, sync_timeout, topics, retry, _error_code) do
    # default to version 4 of the metadata protocol because this one treats an
    # empty list of topics as 'no topics'.  note this limits us to Kafka 0.11+
    api_version = State.max_supported_api_version(state, :metadata, 4)

    metadata_request = %{
      Metadata.get_request_struct(api_version)
      | topics: topics,
        allow_auto_topic_creation: state.allow_auto_topic_creation
    }

    {{ok_or_err, response}, state_out} =
      kayrock_network_request(metadata_request, NodeSelector.first_available(), state)

    case ok_or_err do
      :ok ->
        case Enum.find(response.topic_metadata, &(&1.error_code == ErrorCode.atom_to_code!(:leader_not_available))) do
          nil ->
            {state_out, response}

          topic_metadata ->
            :timer.sleep(300)
            retrieve_metadata(state, sync_timeout, topics, retry - 1, topic_metadata.error_code)
        end

      _ ->
        Logger.error("Unable to fetch metadata from any brokers. Timeout is #{sync_timeout}.")
        {state_out, nil}
    end
  end

  defp sleep_for_reconnect do
    Process.sleep(Application.get_env(:kafka_ex, :sleep_for_reconnect, 400))
  end

  defp check_brokers_sockets!(brokers) do
    any_socket_opened =
      brokers
      |> Enum.map(fn {_, %Broker{socket: socket}} -> !is_nil(socket) end)
      |> Enum.reduce(&(&1 || &2))

    if !any_socket_opened do
      sleep_for_reconnect()
      raise "Brokers sockets are not opened"
    end
  end

  defp client_request(request, state) do
    %{request | client_id: Config.client_id(), correlation_id: state.correlation_id}
  end

  # select a broker, updating state if necessary (e.g., metadata or consumer group)
  # returns {broker, maybe_updated_state} - broker will be nil in case of
  # failure. Ensures the returned broker is connected, attempting reconnection if needed.
  defp select_broker_with_update(state, selector, state_updater) do
    case State.select_broker(state, selector) do
      {:error, _} ->
        updated_state = state_updater.(state)

        case State.select_broker(updated_state, selector) do
          {:error, _} -> {nil, updated_state}
          {:ok, broker} -> ensure_broker_connected(broker, updated_state)
        end

      {:ok, broker} ->
        ensure_broker_connected(broker, state)
    end
  end

  # Ensures broker is connected, reconnecting if necessary.
  # Returns {broker, updated_state} where broker may have a new socket,
  # or nil if reconnection failed.
  defp ensure_broker_connected(broker, state) do
    if Broker.connected?(broker) do
      {broker, state}
    else
      reconnected_broker = reconnect_broker(broker, state)

      if Broker.connected?(reconnected_broker) do
        # Update the broker in state with new socket
        updated_state = update_broker_in_state(state, reconnected_broker)
        {reconnected_broker, updated_state}
      else
        {nil, state}
      end
    end
  end

  defp update_broker_in_state(state, broker) do
    State.update_brokers(state, fn b ->
      if b.node_id == broker.node_id, do: broker, else: b
    end)
  end

  @max_reconnect_attempts 3
  defp ensure_any_broker_connected(state) do
    brokers = State.brokers(state)
    connected = Enum.filter(brokers, &Broker.connected?/1)

    if Enum.empty?(connected) do
      brokers_to_try = Enum.take(brokers, @max_reconnect_attempts)
      reconnect_any_broker(brokers_to_try, state)
    else
      {state, connected}
    end
  end

  defp reconnect_any_broker([], state), do: {state, []}

  defp reconnect_any_broker([broker | rest], state) do
    reconnected = reconnect_broker(broker, state)

    if Broker.connected?(reconnected) do
      updated_state = update_broker_in_state(state, reconnected)
      {updated_state, [reconnected]}
    else
      reconnect_any_broker(rest, state)
    end
  end

  defp broker_for_partition_with_update(state, topic, partition) do
    node = NodeSelector.topic_partition(topic, partition)
    select_broker_with_update(state, node, &update_metadata(&1, [topic]))
  end

  defp broker_for_consumer_group_with_update(state, consumer_group) do
    node = NodeSelector.consumer_group(consumer_group)
    select_broker_with_update(state, node, &update_consumer_group_coordinator(&1, consumer_group))
  end

  defp update_consumer_group_coordinator(state, consumer_group) do
    request = %FindCoordinator.V1.Request{coordinator_key: consumer_group, coordinator_type: 0}
    {response, updated_state} = kayrock_network_request(request, NodeSelector.first_available(), state)

    case response do
      {:ok, %FindCoordinator.V1.Response{error_code: 0, coordinator: coordinator}} ->
        State.put_consumer_group_coordinator(updated_state, consumer_group, coordinator.node_id)

      {:ok, %FindCoordinator.V1.Response{error_code: error_code}} ->
        error_code = ErrorCode.code_to_atom(error_code)
        Logger.warning("Unable to find consumer group coordinator for #{inspect(consumer_group)}: Error #{error_code}")
        updated_state

      {:error, error} ->
        Logger.warning(
          "Unable to find consumer group coordinator for #{inspect(consumer_group)}: Error #{inspect(error)}"
        )

        updated_state
    end
  end

  defp first_broker_response(request, brokers, timeout) do
    first_broker_response(request, Enum.shuffle(brokers), timeout, nil)
  end

  defp first_broker_response(_request, [], _timeout, last_error) do
    {last_error || {:error, :no_connected_broker}, nil}
  end

  defp first_broker_response(request, [broker | rest], timeout, _last_error) do
    case try_broker(broker, request, timeout) do
      nil -> first_broker_response(request, rest, timeout, {:error, :broker_failed})
      {:error, _} = error -> first_broker_response(request, rest, timeout, error)
      response -> {response, broker}
    end
  end

  defp try_broker(broker, request, timeout) do
    case NetworkClient.send_sync_request(broker, request, timeout) do
      {:error, :not_connected} ->
        Logger.debug("#{Broker.to_string(broker)} not connected, skipping")
        nil

      {:error, error} ->
        Logger.warning("Network call to #{Broker.to_string(broker)} failed: #{inspect(error)}")
        nil

      response ->
        response
    end
  end

  defp timeout_val(nil), do: Application.get_env(:kafka_ex, :sync_timeout, @default_call_timeout)
  defp timeout_val(timeout) when is_integer(timeout), do: timeout

  defp config_sync_timeout(timeout \\ nil) do
    timeout || Application.get_env(:kafka_ex, :sync_timeout, @sync_timeout)
  end

  defp get_api_versions(state, request_version \\ 0) do
    request = ApiVersions.get_request_struct(request_version)
    {{ok_or_error, response}, state_out} = kayrock_network_request(request, NodeSelector.first_available(), state)
    {ok_or_error, response, state_out}
  end

  defp kayrock_network_request(request, node_selector, state, network_timeout \\ nil) do
    synchronous = if Map.get(request, :acks) == 0, do: false, else: true
    network_timeout = config_sync_timeout(network_timeout)
    {send_request, updated_state} = get_send_request_function(node_selector, state, network_timeout, synchronous)

    case send_request do
      :no_broker ->
        {{:error, :no_broker}, updated_state}

      {:error, _} = error ->
        {error, updated_state}

      _ ->
        request = client_request(request, updated_state)
        response = run_client_request(request, send_request, synchronous)
        {response, State.increment_correlation_id(updated_state)}
    end
  end

  defp run_client_request(
         %{client_id: client_id, correlation_id: correlation_id} = client_request,
         send_request,
         synchronous
       )
       when not is_nil(client_id) and not is_nil(correlation_id) do
    # Start with empty broker info - will be populated after send
    start_metadata = Telemetry.request_metadata(client_request, %{})

    Telemetry.span([:kafka_ex, :request], start_metadata, fn ->
      do_run_client_request(client_request, send_request, synchronous, start_metadata)
    end)
  end

  defp do_run_client_request(client_request, send_request, synchronous, start_metadata) do
    wire_request = Request.serialize(client_request)
    bytes_sent = IO.iodata_length(wire_request)

    {result, bytes_received, broker_info} =
      case send_request.(wire_request) do
        {{:error, reason}, broker} ->
          {{:error, reason}, 0, broker_to_telemetry_info(broker)}

        {data, broker} when synchronous ->
          {deserialize(data, client_request), byte_size(data), broker_to_telemetry_info(broker)}

        {data, broker} ->
          {data, byte_size(data), broker_to_telemetry_info(broker)}
      end

    additional_stop_metadata = %{bytes_sent: bytes_sent, bytes_received: bytes_received, broker: broker_info}
    stop_metadata = Map.merge(start_metadata, additional_stop_metadata)
    {result, stop_metadata}
  end

  defp get_send_request_function(%NodeSelector{strategy: :first_available}, state, network_timeout, _synchronous) do
    {updated_state, connected_brokers} = ensure_any_broker_connected(state)

    if Enum.empty?(connected_brokers) do
      {:no_broker, updated_state}
    else
      {fn wire_request -> first_broker_response(wire_request, connected_brokers, network_timeout) end, updated_state}
    end
  end

  defp get_send_request_function(
         %NodeSelector{strategy: :topic_partition, topic: topic, partition: partition},
         state,
         network_timeout,
         synchronous
       ) do
    {broker, updated_state} = broker_for_partition_with_update(state, topic, partition)

    if broker do
      if synchronous do
        {send_sync_request_fn(broker, network_timeout), updated_state}
      else
        {send_async_request_fn(broker), updated_state}
      end
    else
      {:no_broker, updated_state}
    end
  end

  defp get_send_request_function(
         %NodeSelector{
           strategy: :consumer_group,
           consumer_group_name: consumer_group
         },
         state,
         network_timeout,
         _synchronous
       ) do
    {broker, updated_state} = broker_for_consumer_group_with_update(state, consumer_group)

    if broker do
      {send_sync_request_fn(broker, network_timeout), updated_state}
    else
      {:no_broker, updated_state}
    end
  end

  defp get_send_request_function(%NodeSelector{} = node_selector, state, network_timeout, _synchronous) do
    case State.select_broker(state, node_selector) do
      {:ok, broker} ->
        {connected_broker, updated_state} = ensure_broker_connected(broker, state)

        if connected_broker do
          {send_sync_request_fn(connected_broker, network_timeout), updated_state}
        else
          {:no_broker, updated_state}
        end

      {:error, _} = error ->
        {error, state}
    end
  end

  defp send_sync_request_fn(broker, network_timeout) do
    fn wire_request ->
      {NetworkClient.send_sync_request(broker, wire_request, network_timeout), broker}
    end
  end

  defp send_async_request_fn(broker) do
    fn wire_request ->
      {NetworkClient.send_async_request(broker, wire_request), broker}
    end
  end

  defp broker_to_telemetry_info(nil), do: %{}
  defp broker_to_telemetry_info(broker), do: %{node_id: broker.node_id, host: broker.host, port: broker.port}

  defp deserialize(data, request) do
    {resp, _} = Request.response_deserializer(request).(data)
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

  defp close_broker_by_socket(state, socket, reason \\ :remote_closed) do
    State.update_brokers(state, fn broker ->
      if Broker.has_socket?(broker, socket) do
        Logger.debug("#{Broker.to_string(broker)} closed connection")
        # Socket is already closed (received :tcp_closed/:ssl_closed), just emit telemetry
        NetworkClient.close_socket(broker, socket, reason)
        Broker.put_socket(broker, nil)
      else
        broker
      end
    end)
  end
end
