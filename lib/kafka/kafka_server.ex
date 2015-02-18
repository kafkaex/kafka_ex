defmodule Kafka.Server do
  @type datetime() :: {{pos_integer, pos_integer, pos_integer}, {pos_integer, pos_integer, pos_integer}}

  ###Public Api
  @client_id "kafka_ex"

  def start_link(uris, name \\ __MODULE__) do
    GenServer.start_link(__MODULE__, uris, [name: name])
  end

  def metadata(topic \\ "", name \\ __MODULE__) do
    GenServer.call(name, {:metadata, topic})
  end

  def latest_offset(topic, partition), do: offset(topic, partition, :latest)

  def earliest_offset(topic, partition), do: offset(topic, partition, :earliest)

  @spec offset(binary, number, datetime|atom) :: map
  def offset(topic, partition, time) do
    GenServer.call(__MODULE__, {:offset, topic, partition, time})
  end

  def fetch(topic, partition, offset, wait_time \\ 10, min_bytes \\ 1, max_bytes \\ 1_000_000) do
    GenServer.call(__MODULE__, {:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes})
  end

  def produce(topic, partition, value, key \\ nil, required_acks \\ 0, timeout \\ 100) do
    GenServer.call(__MODULE__, {:produce, topic, partition, value, key, required_acks, timeout})
  end

  ### GenServer Callbacks
  use GenServer

  def init(uris) do
    socket_map = Kafka.Connection.connect_brokers(uris)
    send(self, :metadata)
    {:ok, {0, %{}, socket_map}}
  end

  def handle_call({:produce, topic, partition, value, key, required_acks, timeout}, _from, {correlation_id, metadata, socket_map} = state) do
    data = Kafka.Protocol.Produce.create_request(correlation_id, @client_id, topic, partition, value, key, required_acks, timeout)
    metadata = topic_metadata(state, topic)
    socket = get_socket_for_broker(socket_map, metadata, topic, partition)
    send_data(socket, data)
    parsed_response = case required_acks do
      0 -> :ok
      _ -> handle_produce_response(timeout)
    end
    {:reply, parsed_response, {correlation_id + 1, metadata, socket_map}}
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes}, _from, {correlation_id, metadata, socket_map} = state) do
    data = Kafka.Protocol.Fetch.create_request(correlation_id, @client_id, topic, partition, offset, wait_time, min_bytes, max_bytes)
    metadata = topic_metadata(state, topic)
    socket = get_socket_for_broker(socket_map, metadata, topic, partition)
    send_data(socket, data)
    {:reply, handle_fetch_response, {correlation_id + 1, metadata, socket_map}}
  end

  def handle_call({:offset, topic, partition, time}, _from, {correlation_id, metadata, socket_map} = state) do
    data = Kafka.Protocol.Offset.create_request(correlation_id, @client_id, topic, partition, time)
    metadata = topic_metadata(state, topic)
    socket = get_socket_for_broker(socket_map, metadata, topic, partition)
    send_data(socket, data)
    {:reply, handle_offset_response, {correlation_id + 1, metadata, socket_map}}
  end

  def handle_call({:metadata, topic}, _from, {correlation_id, metadata, socket_map} = state) do
    data = topic_metadata(state, topic)
    {:reply, data, {correlation_id + 1, metadata, socket_map}}
  end

  def handle_info(:metadata, {correlation_id, _, socket_map} = state) do
    {:noreply, {correlation_id + 1, topic_metadata(state, ""), socket_map}}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, {_, _, socket_map}) do
    Map.values(socket_map) |> Enum.each(&Kafka.Connection.close/1)
  end

  defp topic_metadata({correlation_id, _metadata, socket_map}, topic) do
    data = Kafka.Protocol.Metadata.create_request(correlation_id, @client_id, topic)
    Map.values(socket_map) |> send_metadata_request(data)
    handle_metadata_response
  end

  defp send_metadata_request([socket|rest], data) when length(rest) == 0 do
    case send_data(socket, data) do
      {:error, _} -> raise "Cannot send metadata request"
      _ -> :ok
    end
  end

  defp send_metadata_request([socket|rest], data) do
    case send_data(socket, data) do
      {:error, _} -> send_metadata_request(rest, data)
      _ -> :ok
    end
  end

  defp get_socket_for_broker(socket_map, metadata, topic, partition) when metadata == %{} do
    get_socket_for_broker(socket_map, metadata(""), topic, partition)
  end

  defp get_socket_for_broker(socket_map, metadata, topic, partition) do
    brokers = Map.get(metadata, :brokers, %{})
    leader = Map.get(metadata, :topics, %{}) |> Map.get(topic, %{}) |> Map.get(:partitions, %{}) |> Map.get(partition, %{}) |> Map.get(:leader) #handle when there is no leader or no partition
    socket_map[brokers[leader]]
  end

  defp send_data(socket, data) do
    Kafka.Connection.send(data, socket)
  end

  defp handle_produce_response(timeout) do
    receive do
      {:tcp, _, data} -> Kafka.Protocol.Produce.parse_response(data)
    after
      (timeout + 1000) -> :timeout
    end
  end

  defp handle_fetch_response do
    receive do
      {:tcp, _, data} -> Kafka.Protocol.Fetch.parse_response(data)
    end
  end

  defp handle_offset_response do
    receive do
      {:tcp, _, data} -> Kafka.Protocol.Offset.parse_response(data)
    end
  end

  defp handle_metadata_response do
    receive do
      {:tcp, _, data} -> Kafka.Protocol.Metadata.parse_response(data)
    after
      5000 -> :timeout
    end
  end
end
