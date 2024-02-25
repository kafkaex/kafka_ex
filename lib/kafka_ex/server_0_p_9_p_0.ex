defmodule KafkaEx.Server0P9P0 do
  @moduledoc """
    Implements KafkaEx.Server behaviors for Kafka >= 0.9.0 < 0.10.1 API.

  """

  # these functions aren't implemented for 0.9.0
  @dialyzer [
    {:nowarn_function, kafka_server_create_topics: 3},
    {:nowarn_function, kafka_server_delete_topics: 3},
    {:nowarn_function, kafka_server_api_versions: 1}
  ]

  use KafkaEx.Server
  alias KafkaEx.Config
  alias KafkaEx.ConsumerGroupRequiredError
  alias KafkaEx.InvalidConsumerGroupError
  alias KafkaEx.Protocol.ConsumerMetadata.Response, as: ConsumerMetadataResponse
  alias KafkaEx.Protocol.Heartbeat
  alias KafkaEx.Protocol.JoinGroup
  alias KafkaEx.Protocol.LeaveGroup
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.SyncGroup
  alias KafkaEx.Server.State
  alias KafkaEx.NetworkClient
  alias KafkaEx.Server0P8P2

  require Logger

  @consumer_group_update_interval 30_000

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], name: name)
  end

  # The functions below are all defined in KafkaEx.Server0P8P2 and their
  # implementation is exactly same across both versions of Kafka.

  defdelegate kafka_server_consumer_group(state), to: Server0P8P2
  defdelegate kafka_server_fetch(fetch_request, state), to: Server0P8P2
  defdelegate kafka_server_offset_fetch(offset_fetch, state), to: Server0P8P2

  defdelegate kafka_server_offset_commit(offset_commit_request, state),
    to: Server0P8P2

  defdelegate kafka_server_consumer_group_metadata(state), to: Server0P8P2
  defdelegate kafka_server_update_consumer_metadata(state), to: Server0P8P2
  defdelegate update_consumer_metadata(state, num, error_code), to: Server0P8P2

  def kafka_server_api_versions(_state),
    do: raise("ApiVersions is not supported in 0.9.0 version of kafka")

  def kafka_server_create_topics(_, _, _state),
    do: raise("CreateTopic is not supported in 0.9.0 version of Kafka")

  def kafka_server_delete_topics(_, _, _state),
    do: raise("DeleteTopic is not supported in 0.9.0 version of Kafka")

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
      ssl_options: ssl_options,
      use_ssl: use_ssl,
      api_versions: [:unsupported]
    }

    # Get the initial "real" broker list and start a regular refresh cycle.
    state = update_metadata(state)

    {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

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

  def kafka_server_join_group(request, network_timeout, state_in) do
    {response, state_out} =
      consumer_group_sync_request(
        request,
        JoinGroup,
        network_timeout,
        state_in
      )

    {:reply, response, state_out}
  end

  def kafka_server_sync_group(request, network_timeout, state_in) do
    {response, state_out} =
      consumer_group_sync_request(
        request,
        SyncGroup,
        network_timeout,
        state_in
      )

    {:reply, response, state_out}
  end

  def kafka_server_leave_group(request, network_timeout, state_in) do
    {response, state_out} =
      consumer_group_sync_request(
        request,
        LeaveGroup,
        network_timeout,
        state_in
      )

    {:reply, response, state_out}
  end

  def kafka_server_heartbeat(request, network_timeout, state_in) do
    {response, state_out} =
      consumer_group_sync_request(
        request,
        Heartbeat,
        network_timeout,
        state_in
      )

    {:reply, response, state_out}
  end

  defp consumer_group_sync_request(
         request,
         protocol_module,
         network_timeout,
         state
       ) do
    unless consumer_group?(state) do
      raise ConsumerGroupRequiredError, request
    end

    {broker, state} = broker_for_consumer_group_with_update(state)

    state_out = increment_state_correlation_id(state)

    sync_timeout = config_sync_timeout(network_timeout)

    wire_request =
      protocol_module.create_request(
        state.correlation_id,
        Config.client_id(),
        request
      )

    wire_response =
      NetworkClient.send_sync_request(
        broker,
        wire_request,
        sync_timeout
      )

    case wire_response do
      {:error, reason} ->
        {{:error, reason}, state_out}

      _ ->
        response = protocol_module.parse_response(wire_response)

        if response.error_code == :not_coordinator_for_consumer do
          {_, updated_state_out} = update_consumer_metadata(state_out)

          consumer_group_sync_request(
            request,
            protocol_module,
            network_timeout,
            updated_state_out
          )
        else
          {response, state_out}
        end
    end
  end

  defp update_consumer_metadata(state),
    do: update_consumer_metadata(state, @retry_count, 0)

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

        default_broker = if use_first_as_default, do: hd(state.brokers), else: nil

        {broker_for_consumer_group(updated_state) || default_broker, updated_state}

      broker ->
        {broker, state}
    end
  end

  # note within the GenServer state, we've already validated the
  # consumer group, so it can only be either :no_consumer_group or a
  # valid binary consumer group name
  def consumer_group?(%State{consumer_group: :no_consumer_group}), do: false
  def consumer_group?(_), do: true
end
