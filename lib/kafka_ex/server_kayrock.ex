defmodule KafkaEx.ServerKayrock do
  @moduledoc """
  Implements a KafkaEx.Server using Kayrock

  This should work with any version of Kafka >= 0.11.0
  """

  use KafkaEx.Server

  alias KafkaEx.Protocol
  alias KafkaEx.Protocol.Metadata.Broker
  alias KafkaEx.Server0P10AndLater
  alias KafkaEx.Server0P8P2
  alias KafkaEx.Server0P9P0
  alias KafkaEx.Server.State

  require Logger

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], name: name)
  end

  def broker_call(pid, request, partition_selector \\ :any) do
    GenServer.call(pid, {:kayrock_request, request, partition_selector})
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

  defdelegate kafka_server_metadata(topic, state), to: Server0P10AndLater

  defdelegate kafka_server_update_metadata(state), to: Server0P10AndLater

  defdelegate kafka_server_delete_topics(topics, network_timeout, state),
    to: Server0P10AndLater

  defdelegate kafka_server_create_topics(requests, network_timeout, state),
    to: Server0P10AndLater

  def consumer_group?(_state), do: false

  def kafka_server_init([args]) do
    kafka_server_init([args, self()])
  end

  def kafka_server_init([args, name]) do
    uris = Keyword.get(args, :uris, [])

    metadata_update_interval =
      Keyword.get(args, :metadata_update_interval, @metadata_update_interval)

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

    {_,
     %{
       api_versions: api_versions,
       error_code: error_code
     }, state} = kafka_server_api_versions(%State{brokers: brokers})

    if error_code == :no_response do
      sleep_for_reconnect()
      raise "Brokers sockets are closed"
    end

    :no_error = error_code

    api_versions = KafkaEx.ApiVersions.api_versions_map(api_versions)

    {correlation_id, metadata} =
      try do
        retrieve_metadata(
          brokers,
          state.correlation_id,
          config_sync_timeout(),
          [],
          api_versions
        )
      rescue
        e ->
          sleep_for_reconnect()
          Kernel.reraise(e, System.stacktrace())
      end

    # in kayrock we manage the consumer group elsewhere
    consumer_group = :no_consumer_group

    state = %State{
      metadata: metadata,
      brokers: brokers,
      correlation_id: correlation_id,
      consumer_group: consumer_group,
      metadata_update_interval: metadata_update_interval,
      consumer_group_update_interval: nil,
      worker_name: name,
      ssl_options: ssl_options,
      use_ssl: use_ssl,
      api_versions: api_versions
    }

    # Get the initial "real" broker list and start a regular refresh cycle.
    state = update_metadata(state)

    {:ok, _} =
      :timer.send_interval(state.metadata_update_interval, :update_metadata)

    {:ok, state}
  end

  def kafka_server_api_versions(state) do
    request = %Kayrock.ApiVersions.V0.Request{}

    {{:ok, response}, state_out} = kayrock_network_request(request, :any, state)

    # map to the KafkaEx version of this message
    api_versions = %Protocol.ApiVersions.Response{
      error_code: Kayrock.ErrorCode.code_to_atom(response.error_code),
      api_versions:
        Enum.map(
          response.api_versions,
          fn api_version ->
            %Protocol.ApiVersions.ApiVersion{
              api_key: api_version.api_key,
              min_version: api_version.min_version,
              max_version: api_version.max_version
            }
          end
        )
    }

    {:reply, api_versions,
     %{state_out | correlation_id: state_out.correlation_id + 1}}
  end

  def handle_call(
        {:kayrock_request, request, partition_selector},
        _from,
        state
      ) do
    {response, state_out} =
      kayrock_network_request(request, partition_selector, state)

    {:reply, response, state_out}
  end

  defp kayrock_network_request(request, node_selector, state) do
    {sender, updated_state} = get_sender(node_selector, state)

    wire_request =
      request
      |> client_request(updated_state)
      |> Kayrock.Request.serialize()

    response =
      case(sender.(wire_request)) do
        {:error, reason} -> {:error, reason}
        data -> {:ok, deserialize(data, request)}
      end

    state_out = %{updated_state | correlation_id: state.correlation_id + 1}
    {response, state_out}
  end

  defp get_sender(:any, state) do
    {fn wire_request ->
       first_broker_response(wire_request, state.brokers, config_sync_timeout())
     end, state}
  end

  defp get_sender({:partition, topic, partition}, state) do
    {broker, updated_state} =
      broker_for_partition_with_update(
        state,
        topic,
        partition
      )

    {fn wire_request ->
       NetworkClient.send_sync_request(
         broker,
         wire_request,
         config_sync_timeout()
       )
     end, updated_state}
  end

  defp deserialize(data, request) do
    try do
      deserializer = Kayrock.Request.response_deserializer(request)
      {resp, _} = deserializer.(data)
      resp
    rescue
      _ ->
        Logger.error(
          "Failed to parse a response from the server: #{inspect(data)} " <>
            "for request #{inspect(request)}"
        )

        Kernel.reraise(
          "Parse error during #{inspect(request)} response deserializer. " <>
            "Couldn't parse: #{inspect(data)}",
          System.stacktrace()
        )
    end
  end
end
