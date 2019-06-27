defmodule KafkaEx.ServerKayrock do
  @moduledoc """
  Implements a KafkaEx.Server using Kayrock

  This should work with any version of Kafka >= 0.11.0
  """

  use KafkaEx.Server

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

  defdelegate kafka_server_api_versions(state), to: Server0P10AndLater

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
     %KafkaEx.Protocol.ApiVersions.Response{
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
    case node_selector do
      :any ->
        wire_request =
          request
          |> client_request(state)
          |> Kayrock.Request.serialize()

        response =
          case(
            first_broker_response(
              wire_request,
              state.brokers,
              config_sync_timeout()
            )
          ) do
            {:error, reason} ->
              {:error, reason}

            data ->
              deserializer = Kayrock.Request.response_deserializer(request)
              {resp, _} = deserializer.(data)
              {:ok, resp}
          end

        state = %{state | correlation_id: state.correlation_id + 1}
        {response, state}

      {:partition, topic, partition} ->
        {broker, updated_state} =
          broker_for_partition_with_update(
            state,
            topic,
            partition
          )

        wire_request =
          request
          |> client_request(updated_state)
          |> Kayrock.Request.serialize()

        response =
          case(
            broker
            |> NetworkClient.send_sync_request(
              wire_request,
              config_sync_timeout()
            )
          ) do
            {:error, reason} ->
              {:error, reason}

            data ->
              deserializer = Kayrock.Request.response_deserializer(request)
              {resp, _} = deserializer.(data)
              {:ok, resp}
          end

        state = %{state | correlation_id: state.correlation_id + 1}
        {response, state}
    end
  end

  defp never_callederson(request, module, state) do
    {broker, updated_state} =
      broker_for_partition_with_update(
        state,
        request.topic,
        request.partition
      )

    case broker do
      nil ->
        Logger.error(fn ->
          "network_request: leader for topic #{request.topic}/#{
            request.partition
          } is not available"
        end)

        {{:error, :topic_not_found}, updated_state}

      _ ->
        wire_request =
          request
          |> client_request(updated_state)
          |> module.create_request

        response =
          broker
          |> NetworkClient.send_sync_request(
            wire_request,
            config_sync_timeout()
          )
          |> case do
            {:error, reason} ->
              {:error, reason}

            response ->
              try do
                module.parse_response(response)
              rescue
                _ ->
                  Logger.error(
                    "Failed to parse a response from the server: #{
                      inspect(response)
                    }"
                  )

                  Kernel.reraise(
                    "Parse error during #{inspect(module)}.parse_response. Couldn't parse: #{
                      inspect(response)
                    }",
                    System.stacktrace()
                  )
              end
          end

        state_out = State.increment_correlation_id(updated_state)
        {response, state_out}
    end
  end
end
