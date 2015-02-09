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

  defp get_socket_for_broker(socket_map, metadata, topic, partition) do
    socket = nil
    broker = get_broker_for_topic_partition(metadata, topic, partition)
    if broker do
      socket = socket_map[broker]
      if socket == nil do
        {_, socket} = Kafka.Connection.connect_brokers(broker)
        socket_map = Map.put(socket_map, broker, socket)
      end
    end
    {socket, socket_map}
  end

  defp get_broker_for_topic_partition(metadata, topic, partition) do
    broker = nil
    topic_data = metadata.topics[topic]
    if topic_data do
      partition_data = topic_data.partitions[partition]
      if partition_data do
        leader = partition_data.leader
        broker = metadata.brokers[leader]
      end
    end
    broker
  end

  def handle_call({:produce, topic, partition, value, key, required_acks, timeout}, _from, {correlation_id, metadata, socket_map}) do
    data = Kafka.Protocol.Produce.create_request(correlation_id, @client_id, topic, partition, value, key, required_acks, timeout)
    {socket, socket_map} = get_socket_for_broker(socket_map, metadata, topic, partition)
    if socket do
      Kafka.Connection.send(data, socket)
    end

    cond do
      socket == nil      -> {:reply, {:error, "Unknown topic or partition"}, {correlation_id, metadata, socket_map}}

      required_acks == 0 -> {:reply, :ok, {correlation_id + 1, metadata, socket_map}}

      true ->
        receive do
          {:tcp, _, data} ->
            parsed = Kafka.Protocol.Produce.parse_response(data)
            {:reply, parsed, {correlation_id + 1, metadata, socket_map}}
        after
          (timeout + 1000) -> {:reply, :timeout, {correlation_id + 1, metadata, socket_map}}
        end
    end
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes}, _from, {correlation_id, metadata, socket_map}) do
    data = Kafka.Protocol.Fetch.create_request(correlation_id, @client_id, topic, partition, offset, wait_time, min_bytes, max_bytes)
    {socket, socket_map} = get_socket_for_broker(socket_map, metadata, topic, partition)

    if socket do
      Kafka.Connection.send(data, socket)

      receive do
        {:tcp, _, data} ->
          parsed = Kafka.Protocol.Fetch.parse_response(data)
          {:reply, parsed, {correlation_id + 1, metadata, socket_map}}
      end
    else
      {:reply, {:error, "Unknown topic or partition"}, {correlation_id, metadata, socket_map}}
    end
  end

  def handle_call({:offset, topic, partition, time}, _from, {correlation_id, metadata, socket_map}) do
    data = Kafka.Protocol.Offset.create_request(correlation_id, @client_id, topic, partition, time)
    {socket, socket_map} = get_socket_for_broker(socket_map, metadata, topic, partition)

    if socket do
      Kafka.Connection.send(data, socket)

      receive do
        {:tcp, _, data} ->
          parsed = Kafka.Protocol.Offset.parse_response(data)
          {:reply, parsed, {correlation_id + 1, metadata, socket_map}}
      end
    else
      {:reply, {:error, "Unknown topic or partition"}, {correlation_id, metadata, socket_map}}
    end
  end

  def handle_call(:metadata, _from, {correlation_id, metadata, socket_map}) do
    data = Kafka.Protocol.Metadata.create_request(correlation_id, @client_id)
    socket = List.first(Map.values(socket_map))
    Kafka.Connection.send(data, socket)

    receive do
      {:tcp, _, data} ->
        parsed = Kafka.Protocol.Metadata.parse_response(data)
        {:reply, parsed, {correlation_id + 1, metadata, socket_map}}
    end
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, {_, socket}) do
    Kafka.Connection.close(socket)
  end
end
