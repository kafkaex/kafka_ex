defmodule KafkaEx.Server0P8P0 do
  @moduledoc """
  Implements kafkaEx.Server behaviors for kafka >= 0.8.0 < 0.8.2 API.
  """

  # these functions aren't implemented for 0.8.0
  @dialyzer [
    {:nowarn_function, kafka_server_heartbeat: 3},
    {:nowarn_function, kafka_server_sync_group: 3},
    {:nowarn_function, kafka_server_join_group: 3},
    {:nowarn_function, kafka_server_leave_group: 3},
    {:nowarn_function, kafka_server_update_consumer_metadata: 1},
    {:nowarn_function, kafka_server_consumer_group_metadata: 1},
    {:nowarn_function, kafka_server_consumer_group: 1},
    {:nowarn_function, kafka_server_offset_commit: 2},
    {:nowarn_function, kafka_server_offset_fetch: 2}
  ]

  use KafkaEx.Server
  alias KafkaEx.Protocol.Fetch
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Protocol.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Server.State
  alias KafkaEx.NetworkClient

  def kafka_server_init([args]) do
    kafka_server_init([args, self()])
  end

  def kafka_server_init([args, name]) do
    uris = Keyword.get(args, :uris, [])
    metadata_update_interval = Keyword.get(args, :metadata_update_interval, @metadata_update_interval)
    brokers = Enum.map(uris, fn({host, port}) -> %Broker{host: host, port: port, socket: NetworkClient.create_socket(host, port)} end)
    {correlation_id, metadata} = retrieve_metadata(brokers, 0, config_sync_timeout())
    state = %State{metadata: metadata, brokers: brokers, correlation_id: correlation_id, metadata_update_interval: metadata_update_interval, worker_name: name}
    # Get the initial "real" broker list and start a regular refresh cycle.
    state = update_metadata(state)
    {:ok, _} = :timer.send_interval(state.metadata_update_interval, :update_metadata)

    {:ok, state}
  end

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], [name: name])
  end

  def kafka_server_fetch(fetch_request, state) do
    {response, state} = fetch(fetch_request, state)

    {:reply, response, state}
  end

  def kafka_server_offset_fetch(_, _state), do: raise "Offset Fetch is not supported in 0.8.0 version of kafka"
  def kafka_server_offset_commit(_, _state), do: raise "Offset Commit is not supported in 0.8.0 version of kafka"
  def kafka_server_consumer_group(_state), do: raise "Consumer Group is not supported in 0.8.0 version of kafka"
  def kafka_server_consumer_group_metadata(_state), do: raise "Consumer Group Metadata is not supported in 0.8.0 version of kafka"
  def kafka_server_join_group(_, _, _state), do: raise "Join Group is not supported in 0.8.0 version of kafka"
  def kafka_server_sync_group(_, _, _state), do: raise "Sync Group is not supported in 0.8.0 version of kafka"
  def kafka_server_leave_group(_, _, _state), do: raise "Leave Group is not supported in 0.8.0 version of Kafka"
  def kafka_server_heartbeat(_, _, _state), do: raise "Heartbeat is not supported in 0.8.0 version of kafka"
  def kafka_server_update_consumer_metadata(_state), do: raise "Consumer Group Metadata is not supported in 0.8.0 version of kafka"

  defp fetch(fetch_request, state) do
    fetch_data = Fetch.create_request(%FetchRequest{
      fetch_request |
      client_id: @client_id,
      correlation_id: state.correlation_id,
    })
    {broker, state} = case MetadataResponse.broker_for_topic(state.metadata, state.brokers, fetch_request.topic, fetch_request.partition) do
      nil    ->
        updated_state = update_metadata(state)
        {MetadataResponse.broker_for_topic(state.metadata, state.brokers, fetch_request.topic, fetch_request.partition), updated_state}
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
            {response, %{state | correlation_id: state.correlation_id + 1}}
        end
    end
  end
end
