defmodule KafkaEx.Server0P8P2 do
  @moduledoc """
  Implements kafkaEx.Server behaviors for kafka >= 0.8.2 < 0.9.0 API.
  """

  # these functions aren't implemented for 0.8.2
  @dialyzer [
    {:nowarn_function, kafka_server_heartbeat: 3},
    {:nowarn_function, kafka_server_sync_group: 3},
    {:nowarn_function, kafka_server_join_group: 3},
    {:nowarn_function, kafka_server_leave_group: 3}
  ]

  use KafkaEx.Server
  alias KafkaEx.ConsumerGroupRequiredError
  alias KafkaEx.InvalidConsumerGroupError
  alias KafkaEx.Protocol.ConsumerMetadata
  alias KafkaEx.Protocol.ConsumerMetadata.Response, as: ConsumerMetadataResponse
  alias KafkaEx.Protocol.Fetch
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Protocol.OffsetFetch
  alias KafkaEx.Protocol.OffsetCommit
  alias KafkaEx.Server.State
  alias KafkaEx.NetworkClient

  @consumer_group_update_interval 30_000

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args])
  end
  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], [name: name])
  end

  def kafka_server_init([args]) do
    kafka_server_init([args, self()])
  end

  def kafka_server_init([args, name]) do
    uris = Keyword.get(args, :uris, [])
    metadata_update_interval = Keyword.get(args, :metadata_update_interval, @metadata_update_interval)
    consumer_group_update_interval = Keyword.get(args, :consumer_group_update_interval, @consumer_group_update_interval)

    # this should have already been validated, but it's possible someone could
    # try to short-circuit the start call
    consumer_group = Keyword.get(args, :consumer_group)
    unless KafkaEx.valid_consumer_group?(consumer_group) do
      raise InvalidConsumerGroupError, consumer_group
    end

    brokers = Enum.map(uris, fn({host, port}) -> %Broker{host: host, port: port, socket: NetworkClient.create_socket(host, port)} end)
    {correlation_id, metadata} = retrieve_metadata(brokers, 0, config_sync_timeout())
    state = %State{metadata: metadata, brokers: brokers, correlation_id: correlation_id, consumer_group: consumer_group, metadata_update_interval: metadata_update_interval, consumer_group_update_interval: consumer_group_update_interval, worker_name: name}
    # Get the initial "real" broker list and start a regular refresh cycle.
    state = update_metadata(state)
    {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

    state =
      if consumer_group?(state) do
        # If we are using consumer groups then initialize the state and start the update cycle
        {_, updated_state} = update_consumer_metadata(state)
        {:ok, _} = :timer.send_interval(state.consumer_group_update_interval, :update_consumer_metadata)
        updated_state
      else
        state
      end

    {:ok, state}
  end

  def kafka_server_consumer_group(state) do
    {:reply, state.consumer_group, state}
  end

  def kafka_server_fetch(fetch_request, state) do
    true = consumer_group_if_auto_commit?(fetch_request.auto_commit, state)
    {response, state} = fetch(fetch_request, state)

    {:reply, response, state}
  end

  def kafka_server_offset_fetch(offset_fetch, state) do
    unless consumer_group?(state) do
      raise ConsumerGroupRequiredError, offset_fetch
    end

    {broker, state} = broker_for_consumer_group_with_update(state)

    # if the request is for a specific consumer group, use that
    # otherwise use the worker's consumer group
    consumer_group = offset_fetch.consumer_group || state.consumer_group
    offset_fetch = %{offset_fetch | consumer_group: consumer_group}

    offset_fetch_request = OffsetFetch.create_request(state.correlation_id, @client_id, offset_fetch)

    {response, state} = case broker do
      nil    ->
        Logger.log(:error, "Coordinator for topic #{offset_fetch.topic} is not available")
        {:topic_not_found, state}
      _ ->
        response = broker
          |> NetworkClient.send_sync_request(offset_fetch_request, config_sync_timeout())
          |> OffsetFetch.parse_response
        {response, %{state | correlation_id: state.correlation_id + 1}}
    end

    {:reply, response, state}
  end

  def kafka_server_offset_commit(offset_commit_request, state) do
    {response, state} = offset_commit(state, offset_commit_request)

    {:reply, response, state}
  end

  def kafka_server_consumer_group_metadata(state) do
    {consumer_metadata, state} = update_consumer_metadata(state)
    {:reply, consumer_metadata, state}
  end

  def kafka_server_update_consumer_metadata(state) do
    unless consumer_group?(state) do
      raise ConsumerGroupRequiredError, "consumer metadata update"
    end

    {_, state} = update_consumer_metadata(state)
    {:noreply, state}
  end

  def kafka_server_join_group(_, _, _state), do: raise "Join Group is not supported in 0.8.0 version of kafka"
  def kafka_server_sync_group(_, _, _state), do: raise "Sync Group is not supported in 0.8.0 version of kafka"
  def kafka_server_leave_group(_, _, _state), do: raise "Leave Group is not supported in 0.8.0 version of Kafka"
  def kafka_server_heartbeat(_, _, _state), do: raise "Heartbeat is not supported in 0.8.0 version of kafka"
  defp update_consumer_metadata(state), do: update_consumer_metadata(state, @retry_count, 0)

  defp update_consumer_metadata(%State{consumer_group: consumer_group} = state, 0, error_code) do
    Logger.log(:error, "Fetching consumer_group #{consumer_group} metadata failed with error_code #{inspect error_code}")
    {%ConsumerMetadataResponse{error_code: error_code}, state}
  end

  defp update_consumer_metadata(%State{consumer_group: consumer_group, correlation_id: correlation_id} = state, retry, _error_code) do
    response = correlation_id
      |> ConsumerMetadata.create_request(@client_id, consumer_group)
      |> first_broker_response(state)
      |> ConsumerMetadata.parse_response

    case response.error_code do
      :no_error -> {response, %{state | consumer_metadata: response, correlation_id: state.correlation_id + 1}}
      _ -> :timer.sleep(400)
        update_consumer_metadata(%{state | correlation_id: state.correlation_id + 1}, retry - 1, response.error_code)
    end
  end

  defp fetch(fetch_request, state) do
    true = consumer_group_if_auto_commit?(fetch_request.auto_commit, state)
    fetch_data = Fetch.create_request(%FetchRequest{
      fetch_request |
      client_id: @client_id,
      correlation_id: state.correlation_id,
    })
    {broker, state} = case MetadataResponse.broker_for_topic(state.metadata, state.brokers, fetch_request.topic, fetch_request.partition) do
      nil    ->
        updated_state = update_metadata(state)
        {MetadataResponse.broker_for_topic(updated_state.metadata, updated_state.brokers, fetch_request.topic, fetch_request.partition), updated_state}
      broker -> {broker, state}
    end

    case broker do
      nil ->
        Logger.log(:error, "Leader for topic #{fetch_request.topic} is not available")
        {:topic_not_found, state}
      _ ->
        response = NetworkClient.send_sync_request(broker, fetch_data, config_sync_timeout())
        case response do
          nil -> {response, state}
          _ ->
            response = Fetch.parse_response(response)
            state = %{state | correlation_id: state.correlation_id + 1}
            last_offset = response |> hd |> Map.get(:partitions) |> hd |> Map.get(:last_offset)
            if last_offset != nil && fetch_request.auto_commit do
              offset_commit_request = %OffsetCommit.Request{
                topic: fetch_request.topic,
                offset: last_offset,
                partition: fetch_request.partition,
                consumer_group: state.consumer_group}
              {_, state} = offset_commit(state, offset_commit_request)
              {response, state}
            else
              {response, state}
            end
        end
    end
  end

  defp offset_commit(state, offset_commit_request) do
    {broker, state} = broker_for_consumer_group_with_update(state, true)

    # if the request has a specific consumer group, use that
    # otherwise use the worker's consumer group
    consumer_group = offset_commit_request.consumer_group || state.consumer_group
    offset_commit_request = %{offset_commit_request | consumer_group: consumer_group}

    offset_commit_request_payload = OffsetCommit.create_request(state.correlation_id, @client_id, offset_commit_request)
    response = broker
      |> NetworkClient.send_sync_request(offset_commit_request_payload, config_sync_timeout())
      |> OffsetCommit.parse_response

    {response, %{state | correlation_id: state.correlation_id + 1}}
  end

  defp broker_for_consumer_group(state) do
    ConsumerMetadataResponse.broker_for_consumer_group(state.brokers, state.consumer_metadata)
  end

  # refactored from two versions, one that used the first broker as valid answer, hence
  # the optional extra flag to do that. Wraps broker_for_consumer_group with an update
  # call if no broker was found.
  defp broker_for_consumer_group_with_update(state, use_first_as_default \\ false) do
    case broker_for_consumer_group(state) do
      nil ->
        {_, updated_state} = update_consumer_metadata(state)
        default_broker = if use_first_as_default, do: hd(state.brokers), else: nil
        {broker_for_consumer_group(updated_state) || default_broker, updated_state}
      broker ->
        {broker, state}
    end
  end

  # note within the genserver state, we've already validated the
  # consumer group, so it can only be either :no_consumer_group or a
  # valid binary consumer group name
  def consumer_group?(%State{consumer_group: :no_consumer_group}), do: false
  def consumer_group?(_), do: true

  def consumer_group_if_auto_commit?(true, state) do
    consumer_group?(state)
  end
  def consumer_group_if_auto_commit?(false, _state) do
    true
  end

  defp first_broker_response(request, state) do
    first_broker_response(request, state.brokers, config_sync_timeout())
  end
end
