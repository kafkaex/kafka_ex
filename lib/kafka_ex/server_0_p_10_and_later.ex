defmodule KafkaEx.Server0P10AndLater do
  @moduledoc """
  Implements kafkaEx.Server behaviors for kafka 0.10.1 API.
  """
  use KafkaEx.Server
  alias KafkaEx.Protocol.CreateTopics
  alias KafkaEx.Protocol.DeleteTopics
  alias KafkaEx.Protocol.ApiVersions
  alias KafkaEx.Server0P8P2
  alias KafkaEx.Server0P9P0

  alias KafkaEx.InvalidConsumerGroupError
  alias KafkaEx.Protocol.ConsumerMetadata
  alias KafkaEx.Protocol.ConsumerMetadata.Response, as: ConsumerMetadataResponse
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Server.State

  require Logger

  @consumer_group_update_interval 30_000

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], name: name)
  end

  # The functions below are all defined in KafkaEx.Server0P8P2
  defdelegate kafka_server_consumer_group(state), to: Server0P8P2
  defdelegate kafka_server_fetch(fetch_request, state), to: Server0P8P2
  defdelegate kafka_server_offset_fetch(offset_fetch, state), to: Server0P8P2

  defdelegate kafka_server_offset_commit(offset_commit_request, state),
    to: Server0P8P2

  defdelegate kafka_server_consumer_group_metadata(state), to: Server0P8P2
  defdelegate kafka_server_update_consumer_metadata(state), to: Server0P8P2

  # The functions below are all defined in KafkaEx.Server0P9P0
  defdelegate kafka_server_join_group(request, network_timeout, state_in),
    to: Server0P9P0

  defdelegate kafka_server_sync_group(request, network_timeout, state_in),
    to: Server0P9P0

  defdelegate kafka_server_leave_group(request, network_timeout, state_in),
    to: Server0P9P0

  defdelegate kafka_server_heartbeat(request, network_timeout, state_in),
    to: Server0P9P0

  defdelegate consumer_group?(state), to: Server0P9P0

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

    use_ssl = Keyword.get(args, :use_ssl, false)
    ssl_options = Keyword.get(args, :ssl_options, [])

    brokers =
      Enum.map(uris, fn {host, port} ->
        %Broker{
          host: host,
          port: port,
          socket: NetworkClient.create_socket(host, port, ssl_options, use_ssl)
        }
      end)

    {_,
     %KafkaEx.Protocol.ApiVersions.Response{
       api_versions: api_versions,
       error_code: :no_error
     }, state} = kafka_api_versions(%State{brokers: brokers})

    api_versions = KafkaEx.ApiVersions.api_versions_map(api_versions)

    {correlation_id, metadata} =
      retrieve_metadata(
        brokers,
        state.correlation_id,
        config_sync_timeout(),
        [],
        api_versions
      )

    state = %State{
      metadata: metadata,
      brokers: brokers,
      correlation_id: correlation_id,
      consumer_group: consumer_group,
      metadata_update_interval: metadata_update_interval,
      consumer_group_update_interval: consumer_group_update_interval,
      worker_name: name,
      ssl_options: ssl_options,
      use_ssl: use_ssl,
      api_versions: api_versions
    }

    # Get the initial "real" broker list and start a regular refresh cycle.
    state = update_metadata(state)

    {:ok, _} =
      :timer.send_interval(state.metadata_update_interval, :update_metadata)

    state =
      if consumer_group?(state) do
        # If we are using consumer groups then initialize the state and start the update cycle
        {_, updated_state} = update_consumer_metadata(state)

        {:ok, _} =
          :timer.send_interval(
            state.consumer_group_update_interval,
            :update_consumer_metadata
          )

        updated_state
      else
        state
      end

    {:ok, state}
  end

  def kafka_server_metadata(topic, state) do
    {correlation_id, metadata} =
      retrieve_metadata(
        state.brokers,
        state.correlation_id,
        config_sync_timeout(),
        topic,
        state.api_versions
      )

    updated_state = %{
      state
      | metadata: metadata,
        correlation_id: correlation_id
    }

    {:reply, metadata, updated_state}
  end

  def kafka_server_update_metadata(state) do
    {:noreply, update_metadata(state)}
  end

  def kafka_api_versions(state) do
    response =
      state.correlation_id
      |> ApiVersions.create_request(@client_id)
      |> first_broker_response(state)
      |> ApiVersions.parse_response()

    {:reply, response, %{state | correlation_id: state.correlation_id + 1}}
  end

  def kafka_delete_topics(topics, network_timeout, state) do
    api_version =
      case DeleteTopics.api_version(state.api_versions) do
        {:ok, api_version} ->
          api_version

        _ ->
          raise "DeleteTopic is not supported in this version of Kafka, or the versions supported by the client do not match the ones supported by the server."
      end

    main_request =
      DeleteTopics.create_request(
        state.correlation_id,
        @client_id,
        %DeleteTopics.Request{
          topics: topics,
          timeout: config_sync_timeout(network_timeout)
        },
        api_version
      )

    broker = state.brokers |> Enum.find(& &1.is_controller)

    {response, state} =
      case broker do
        nil ->
          Logger.log(:error, "Coordinator for topic is not available")
          {:topic_not_found, state}

        _ ->
          response =
            broker
            |> NetworkClient.send_sync_request(
              main_request,
              config_sync_timeout()
            )
            |> case do
              {:error, reason} -> {:error, reason}
              response -> DeleteTopics.parse_response(response, api_version)
            end

          {response, %{state | correlation_id: state.correlation_id + 1}}
      end

    {:reply, response, state}
  end

  def kafka_create_topics(requests, network_timeout, state) do
    api_version =
      case CreateTopics.api_version(state.api_versions) do
        {:ok, api_version} ->
          api_version

        _ ->
          raise "CreateTopic is not supported in this version of Kafka, or the versions supported by the client do not match the ones supported by the server."
      end

    create_topics_request = %CreateTopics.Request{
      create_topic_requests: requests,
      timeout: config_sync_timeout(network_timeout)
    }

    main_request =
      CreateTopics.create_request(
        state.correlation_id,
        @client_id,
        create_topics_request,
        api_version
      )

    broker = state.brokers |> Enum.find(& &1.is_controller)

    {response, state} =
      case broker do
        nil ->
          Logger.log(:error, "Coordinator for topic is not available")
          {:topic_not_found, state}

        _ ->
          response =
            broker
            |> NetworkClient.send_sync_request(
              main_request,
              config_sync_timeout()
            )
            |> case do
              {:error, reason} -> {:error, reason}
              response -> CreateTopics.parse_response(response, api_version)
            end

          {response, %{state | correlation_id: state.correlation_id + 1}}
      end

    {:reply, response, state}
  end

  defp update_consumer_metadata(state),
    do: update_consumer_metadata(state, @retry_count, 0)

  defp update_consumer_metadata(
         %State{consumer_group: consumer_group} = state,
         0,
         error_code
       ) do
    Logger.log(
      :error,
      "Fetching consumer_group #{consumer_group} metadata failed with error_code #{
        inspect(error_code)
      }"
    )

    {%ConsumerMetadataResponse{error_code: error_code}, state}
  end

  defp update_consumer_metadata(
         %State{consumer_group: consumer_group, correlation_id: correlation_id} =
           state,
         retry,
         _error_code
       ) do
    response =
      correlation_id
      |> ConsumerMetadata.create_request(@client_id, consumer_group)
      |> first_broker_response(state)
      |> ConsumerMetadata.parse_response()

    case response.error_code do
      :no_error ->
        {
          response,
          %{
            state
            | consumer_metadata: response,
              correlation_id: state.correlation_id + 1
          }
        }

      _ ->
        :timer.sleep(400)

        update_consumer_metadata(
          %{state | correlation_id: state.correlation_id + 1},
          retry - 1,
          response.error_code
        )
    end
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
  def broker_for_consumer_group_with_update(
        state,
        use_first_as_default \\ false
      ) do
    case broker_for_consumer_group(state) do
      nil ->
        {_, updated_state} = update_consumer_metadata(state)

        default_broker =
          if use_first_as_default, do: hd(state.brokers), else: nil

        {broker_for_consumer_group(updated_state) || default_broker,
         updated_state}

      broker ->
        {broker, state}
    end
  end

  defp first_broker_response(request, state) do
    first_broker_response(request, state.brokers, config_sync_timeout())
  end
end
