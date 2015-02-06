defmodule Kafka.Server do
  ###Public Api
  @client_id "kafka_ex"
  def start(uris) do
    GenServer.start_link(__MODULE__, uris, [name: __MODULE__])
  end

  def metadata do
    GenServer.call(__MODULE__, :metadata)
  end

  ### GenServer Callbacks
  use GenServer

  def init(uris) do
    socket = Kafka.Connection.connect_brokers(uris)
    {:ok, {1, socket}}
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

  def handle_call({:produce, data}, {correlation_id, socket}) do
    Kafka.Connection.send(data, socket) 

    receive do
      {:tcp, _, data} -> {:reply, data, {correlation_id + 1, socket}}
    end
  end

  def handle_cast({:produce, data}, {_, socket} = state) do
    Kakfa.Connection.send(data, socket) 
    {:noreply, state}
  end

  def handle_info({:tcp, _, data}, {correlation_id, socket}) do
    #do something with data 
    data
    {:noreply, {correlation_id + 1, socket}}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, {_, socket}) do
    Kafka.Connection.close(socket)
  end
end
