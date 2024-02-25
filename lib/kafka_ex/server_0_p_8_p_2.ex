defmodule KafkaEx.Server0P8P2 do
  @moduledoc """
  Implements KafkaEx.Server behaviors for kafka >= 0.8.2 < 0.9.0 API.
  """

  # these functions aren't implemented for 0.8.2
  @dialyzer [
    {:nowarn_function, kafka_server_heartbeat: 3},
    {:nowarn_function, kafka_server_sync_group: 3},
    {:nowarn_function, kafka_server_join_group: 3},
    {:nowarn_function, kafka_server_leave_group: 3},
    {:nowarn_function, kafka_server_create_topics: 3},
    {:nowarn_function, kafka_server_delete_topics: 3},
    {:nowarn_function, kafka_server_api_versions: 1}
  ]

  use KafkaEx.Server
  alias KafkaEx.Config
  alias KafkaEx.ConsumerGroupRequiredError
  alias KafkaEx.InvalidConsumerGroupError
  alias KafkaEx.Protocol.ConsumerMetadata
  alias KafkaEx.Protocol.ConsumerMetadata.Response, as: ConsumerMetadataResponse
  alias KafkaEx.Protocol.Fetch
  alias KafkaEx.Protocol.Metadata.Broker
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
    GenServer.start_link(__MODULE__, [args, name], name: name)
  end

  def kafka_server_init([args]) do
    kafka_server_init([args, self()])
  end

  def kafka_server_init([args, name]) do
    uris = Keyword.get(args, :uris, [])

    metadata_update_interval =
      Keyword.get(args, :metadata_update_interval, @metadata_update_interval)

    consumer_group_update_interval =
      Keyword.get(
        args,
        :consumer_group_update_interval,
        @consumer_group_update_interval
      )

    # this should have already been validated, but it's possible someone could
    # try to short-circuit the start call
    consumer_group = Keyword.get(args, :consumer_group)

    unless KafkaEx.valid_consumer_group?(consumer_group) do
      raise InvalidConsumerGroupError, consumer_group
    end

    brokers =
      Enum.map(uris, fn {host, port} ->
        %Broker{
          host: host,
          port: port,
          socket: NetworkClient.create_socket(host, port)
        }
      end)

    check_brokers_sockets!(brokers)

    {correlation_id, metadata} =
      try do
        retrieve_metadata(brokers, 0, config_sync_timeout())
      rescue
        e ->
          sleep_for_reconnect()
          Kernel.reraise(e, __STACKTRACE__)
      end

    state = %State{
      metadata: metadata,
      brokers: brokers,
      correlation_id: correlation_id,
      consumer_group: consumer_group,
      metadata_update_interval: metadata_update_interval,
      consumer_group_update_interval: consumer_group_update_interval,
      worker_name: name,
      api_versions: [:unsupported]
    }

    # Get the initial "real" broker list and start a regular refresh cycle.
    state = update_metadata(state)

    {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

    if consumer_group?(state) do
      # If we are using consumer groups then initialize the state and start the update cycle
      {_, updated_state} = update_consumer_metadata(state)

      {:ok, _} =
        :timer.send_interval(
          state.consumer_group_update_interval,
          :update_consumer_metadata
        )

      {:ok, updated_state}
    else
      {:ok, state}
    end
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

    offset_fetch_request =
      OffsetFetch.create_request(
        state.correlation_id,
        Config.client_id(),
        offset_fetch
      )

    {response, state} =
      case broker do
        nil ->
          Logger.log(
            :error,
            "Coordinator for topic #{offset_fetch.topic} is not available"
          )

          {:topic_not_found, state}

        _ ->
          response =
            broker
            |> NetworkClient.send_sync_request(
              offset_fetch_request,
              config_sync_timeout()
            )
            |> case do
              {:error, reason} -> {:error, reason}
              response -> OffsetFetch.parse_response(response)
            end

          {response, increment_state_correlation_id(state)}
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

  def kafka_server_join_group(_, _, _state),
    do: raise("Join Group is not supported in 0.8.2 version of kafka")

  def kafka_server_sync_group(_, _, _state),
    do: raise("Sync Group is not supported in 0.8.2 version of kafka")

  def kafka_server_leave_group(_, _, _state),
    do: raise("Leave Group is not supported in 0.8.2 version of Kafka")

  def kafka_server_heartbeat(_, _, _state),
    do: raise("Heartbeat is not supported in 0.8.2 version of kafka")

  def kafka_server_api_versions(_state),
    do: raise("ApiVersions is not supported in 0.8.2 version of kafka")

  def kafka_server_create_topics(_, _, _state),
    do: raise("CreateTopic is not supported in 0.8.2 version of kafka")

  def kafka_server_delete_topics(_, _, _state),
    do: raise("DeleteTopic is not supported in 0.8.2 version of kafka")

  defp update_consumer_metadata(state),
    do: update_consumer_metadata(state, @retry_count, 0)

  def update_consumer_metadata(
        %State{consumer_group: consumer_group} = state,
        0,
        error_code
      ) do
    Logger.log(
      :error,
      "Fetching consumer_group #{consumer_group} metadata failed with error_code #{inspect(error_code)}"
    )

    {%ConsumerMetadataResponse{error_code: error_code}, state}
  end

  def update_consumer_metadata(
        %State{consumer_group: consumer_group, correlation_id: correlation_id} = state,
        retry,
        _error_code
      ) do
    response =
      correlation_id
      |> ConsumerMetadata.create_request(Config.client_id(), consumer_group)
      |> first_broker_response(state)
      |> ConsumerMetadata.parse_response()

    case response.error_code do
      :no_error ->
        {response,
         %{
           state
           | consumer_metadata: response,
             correlation_id: state.correlation_id + 1
         }}

      _ ->
        :timer.sleep(400)

        update_consumer_metadata(
          increment_state_correlation_id(state),
          retry - 1,
          response.error_code
        )
    end
  end

  defp fetch(request, state) do
    true = consumer_group_if_auto_commit?(request.auto_commit, state)

    case network_request(request, Fetch, state) do
      {{:error, error}, state_out} ->
        {error, state_out}

      {response, state_out} ->
        last_offset =
          case response do
            [%{partitions: [%{last_offset: last_offset} | _]} | _] ->
              last_offset

            [] ->
              Logger.log(
                :error,
                "Not able to retrieve the last offset, the kafka server is probably throttling your requests"
              )

              nil
          end

        if last_offset != nil && request.auto_commit do
          offset_commit_request = %OffsetCommit.Request{
            topic: request.topic,
            offset: last_offset,
            partition: request.partition,
            consumer_group: state_out.consumer_group
          }

          {_, state} = offset_commit(state_out, offset_commit_request)
          {response, state}
        else
          {response, state_out}
        end
    end
  end

  defp offset_commit(state, offset_commit_request) do
    {broker, state} = broker_for_consumer_group_with_update(state, true)

    # if the request has a specific consumer group, use that
    # otherwise use the worker's consumer group
    consumer_group = offset_commit_request.consumer_group || state.consumer_group

    offset_commit_request = %{
      offset_commit_request
      | consumer_group: consumer_group
    }

    offset_commit_request_payload =
      OffsetCommit.create_request(
        state.correlation_id,
        Config.client_id(),
        offset_commit_request
      )

    response =
      broker
      |> NetworkClient.send_sync_request(
        offset_commit_request_payload,
        config_sync_timeout()
      )
      |> case do
        {:error, reason} -> {:error, reason}
        response -> OffsetCommit.parse_response(response)
      end

    {response, increment_state_correlation_id(state)}
  end

  defp broker_for_consumer_group(state) do
    ConsumerMetadataResponse.broker_for_consumer_group(
      state.brokers,
      state.consumer_metadata
    )
  end

  # refactored from two versions, one that used the first broker as valid answer, hence
  # the optional extra flag to do that. Wraps broker_for_consumer_group with an update
  # call if no broker was found.
  defp broker_for_consumer_group_with_update(
         state,
         use_first_as_default \\ false
       ) do
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

  def consumer_group_if_auto_commit?(true, state), do: consumer_group?(state)
  def consumer_group_if_auto_commit?(false, _state), do: true

  defp first_broker_response(request, state) do
    first_broker_response(request, state.brokers, config_sync_timeout())
  end
end
