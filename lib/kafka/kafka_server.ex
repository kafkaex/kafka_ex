defmodule Kafka.Server do
  @type datetime() :: {{pos_integer, pos_integer, pos_integer}, {pos_integer, pos_integer, pos_integer}}

  ###Public Api
  @client_id "kafka_ex"

  def start_link(uris, name \\ __MODULE__) do
    GenServer.start_link(__MODULE__, uris, [name: name])
  end

  def metadata do
    GenServer.call(__MODULE__, :metadata)
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
    {host_port, socket} = Kafka.Connection.connect_brokers(uris)
    socket_map = Map.put(%{}, host_port, socket)
    {:reply, {:ok, metadata}, {correlation_id, _, socket_map}} = handle_call(:metadata, self(), {1, nil, socket_map})
    {:ok, {correlation_id, metadata, socket_map}}
  end

  def handle_call({:produce, topic, partition, value, key, required_acks, timeout}, _from, {correlation_id, metadata, socket_map} = state) do
    data = Kafka.Protocol.Produce.create_request(correlation_id, @client_id, topic, partition, value, key, required_acks, timeout)
    get_socket_for_broker(socket_map, metadata, topic, partition)
    |> send_data(data, state)
    |> handle_produce_response(required_acks, timeout, state)
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes}, _from, {correlation_id, metadata, socket_map} = state) do
    data = Kafka.Protocol.Fetch.create_request(correlation_id, @client_id, topic, partition, offset, wait_time, min_bytes, max_bytes)
    get_socket_for_broker(socket_map, metadata, topic, partition)
    |> send_data(data, state)
    |> handle_fetch_response(state)
  end

  def handle_call({:offset, topic, partition, time}, _from, {correlation_id, metadata, socket_map} = state) do
    data = Kafka.Protocol.Offset.create_request(correlation_id, @client_id, topic, partition, time)
    get_socket_for_broker(socket_map, metadata, topic, partition)
    |> send_data(data, state)
    |> handle_offset_response(state)
  end

  def handle_call(:metadata, _from, {correlation_id, _metadata, socket_map} = state) do
    data = Kafka.Protocol.Metadata.create_request(correlation_id, @client_id)
    {List.first(Map.values(socket_map)), socket_map}
    |> send_data(data, state)
    |> handle_metadata_response(state)
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, {_, socket}) do
    Kafka.Connection.close(socket)
  end

  defp get_socket_for_broker(socket_map, metadata, topic, partition) do
    get_broker_for_topic_partition(metadata, topic, partition)
    |> get_cached_socket(socket_map)
    |> create_and_cache_socket
  end

  defp get_broker_for_topic_partition(metadata, topic, partition) do
    metadata.topics[topic]
    |> get_partition_data(partition)
    |> get_leader(metadata)
  end

  defp get_cached_socket(nil, socket_map) do
    {nil, socket_map}
  end

  defp get_cached_socket(broker, socket_map) do
    {socket_map[broker], broker, socket_map}
  end

  defp create_and_cache_socket({nil, socket_map}) do
    {nil, socket_map}
  end

  defp create_and_cache_socket({nil, broker, socket_map}) do
    {_, socket} = Kafka.Connection.connect_brokers(broker)
    socket_map = Map.put(socket_map, broker, socket)
    {socket, socket_map}
  end

  defp create_and_cache_socket({socket, _broker, socket_map}) do
    {socket, socket_map}
  end

  defp get_partition_data(nil, _partition) do
    nil
  end

  defp get_partition_data(topic_data, partition) do
    topic_data.partitions[partition]
  end

  defp get_leader(nil, _metadata) do
    nil
  end

  defp get_leader(partition_data, metadata) do
    metadata.brokers[partition_data.leader]
  end

  defp send_data({nil, socket_map}, _data, _state) do
    {nil, socket_map}
  end

  defp send_data({socket, socket_map}, data, _state) do
    case Kafka.Connection.send(data, socket) do
      :ok -> socket_map
      {:error, reason} -> {:error, reason, socket_map}
    end
  end

  defp handle_produce_response({nil, socket_map}, _required_acks, _timeout, {correlation_id, metadata, _socket_map}) do
    {:reply, {:error, "Unknown topic or partition"}, {correlation_id, metadata, socket_map}}
  end

  defp handle_produce_response(socket_map, _required_acks, timeout, {correlation_id, metadata, _socket_map}) do
    receive do
      {:tcp, _, data} ->
        parsed = Kafka.Protocol.Produce.parse_response(data)
        {:reply, parsed, {correlation_id + 1, metadata, socket_map}}
    after
      (timeout + 1000) -> {:reply, :timeout, {correlation_id + 1, metadata, socket_map}}
    end
  end

  defp handle_produce_response(socket_map, 0, _timeout, {correlation_id, metadata, _socket_map}) do
    {:reply, :ok, {correlation_id + 1, metadata, socket_map}}
  end

  defp handle_produce_response({:error, reason, socket_map}, _acks, _timeout, {correlation_id, metadata, _socket_map}) do
    {:reply, {:error, reason}, {correlation_id, metadata, socket_map}}
  end

  defp handle_fetch_response({nil, socket_map}, {correlation_id, metadata, _socket_map}) do
    {:reply, {:error, "Unknown topic or partition"}, {correlation_id, metadata, socket_map}}
  end

  defp handle_fetch_response(socket_map, {correlation_id, metadata, _socket_map}) do
    receive do
      {:tcp, _, data} ->
        parsed = Kafka.Protocol.Fetch.parse_response(data)
        {:reply, parsed, {correlation_id + 1, metadata, socket_map}}
    end
  end

  defp handle_fetch_response({:error, reason, socket_map}, {correlation_id, metadata, _socket_map}) do
    {:reply, {:error, reason}, {correlation_id, metadata, socket_map}}
  end

  defp handle_offset_response({nil, socket_map}, {correlation_id, metadata, _socket_map}) do
    {:reply, {:error, "Unknown topic or partition"}, {correlation_id, metadata, socket_map}}
  end

  defp handle_offset_response(socket_map, {correlation_id, metadata, _socket_map}) do
    receive do
      {:tcp, _, data} ->
        parsed = Kafka.Protocol.Offset.parse_response(data)
        {:reply, parsed, {correlation_id + 1, metadata, socket_map}}
    end
  end

  defp handle_offset_response({:error, reason, socket_map}, {correlation_id, metadata, _socket_map}) do
    {:reply, {:error, reason}, {correlation_id, metadata, socket_map}}
  end

  defp handle_metadata_response({nil, socket_map}, {correlation_id, metadata, _socket_map}) do
    {:reply, {:error, "Unknown topic or partition"}, {correlation_id, metadata, socket_map}}
  end

  defp handle_metadata_response(socket_map, {correlation_id, metadata, _socket_map}) do
    receive do
      {:tcp, _, data} ->
        parsed = Kafka.Protocol.Metadata.parse_response(data)
        {:reply, parsed, {correlation_id + 1, metadata, socket_map}}
    end
  end

  defp handle_metadata_response({:error, reason, socket_map}, {correlation_id, metadata, _socket_map}) do
    {:reply, {:error, reason}, {correlation_id, metadata, socket_map}}
  end
end
