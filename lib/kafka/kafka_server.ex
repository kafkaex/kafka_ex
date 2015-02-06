defmodule Kafka.Server do
  @type datetime() :: {{pos_integer, pos_integer, pos_integer}, {pos_integer, pos_integer, pos_integer}}

  ###Public Api
  @client_id "kafka_ex"

  def start_link(uris) do
    GenServer.start_link(__MODULE__, uris, [name: __MODULE__])
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
    socket = Kafka.Connection.connect_brokers(uris)
    {:ok, {1, socket}}
  end

  def handle_call({:produce, topic, partition, value, key, required_acks, timeout}, _from, {correlation_id, socket}) do
    data = Kafka.Protocol.Produce.create_request(correlation_id, @client_id, topic, partition, value, key, required_acks, timeout)
    Kafka.Connection.send(data, socket)

    if required_acks == 0 do
      {:reply, :ok, {correlation_id + 1, socket}}
    else
      receive do
        {:tcp, _, data} ->
          parsed = Kafka.Protocol.Produce.parse_response(data)
          {:reply, parsed, {correlation_id + 1, socket}}
      after
        (timeout + 1000) -> {:reply, :timeout, {correlation_id + 1, socket}}
      end
    end
  end

  def handle_call({:fetch, topic, partition, offset, wait_time, min_bytes, max_bytes}, _from, {correlation_id, socket}) do
    data = Kafka.Protocol.Fetch.create_request(correlation_id, @client_id, topic, partition, offset, wait_time, min_bytes, max_bytes)
    Kafka.Connection.send(data, socket)

    receive do
      {:tcp, _, data} ->
        parsed = Kafka.Protocol.Fetch.parse_response(data)
        {:reply, parsed, {correlation_id + 1, socket}}
    end
  end

  def handle_call({:offset, topic, partition, time}, _from, {correlation_id, socket}) do
    data = Kafka.Protocol.Offset.create_request(correlation_id, @client_id, topic, partition, time)
    Kafka.Connection.send(data, socket)

    receive do
      {:tcp, _, data} ->
        parsed = Kafka.Protocol.Offset.parse_response(data)
        {:reply, parsed, {correlation_id + 1, socket}}
    end
  end

  def handle_call(:metadata, _from, {correlation_id, socket}) do
    data = Kafka.Protocol.Metadata.create_request(correlation_id, @client_id)
    Kafka.Connection.send(data, socket)

    receive do
      {:tcp, _, data} ->
        parsed = Kafka.Protocol.Metadata.parse_response(data)
        {:reply, parsed, {correlation_id + 1, socket}}
    end
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, {_, socket}) do
    Kafka.Connection.close(socket)
  end
end
