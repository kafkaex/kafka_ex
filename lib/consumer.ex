# Kafka consumer code
#
# When new is called, it contacts Kafka and retrieves the metadata
#
# When subscribe is called, one GenServer is created per broker, with each GenServer
# storing the list of subscribed topics in an Agent. It also spawns a "fetcher" process to send
# fetch requests and retrieve responses, which uses the Agent to get updates to the topic list.
#
# One EventManager is created per topic, so that the spawned process can send events on a
# topic-by-topic basis. Each callback function is wrapped in a handler, so that when the
# fetcher process receives data for a topic, it can generate an event which then sends the
# data to all the callbacks subscribed to that topic
#
defmodule Kafka.Consumer do
  def new(broker_list, client_id) do
    Kafka.Metadata.new(broker_list, client_id)
    |> create_consumer(broker_list, client_id)
  end

  defp create_consumer({:ok, metadata}, broker_list, client_id) do
    {:ok, %{broker_list: broker_list, client_id: client_id, metadata: metadata}}
  end

  defp create_consumer(error, _, _) do
    error
  end

  defp update_metadata({:ok, metadata}, consumer) do
    {:ok, Map.put(consumer, :metadata, metadata)}
  end

  defp update_metadata({:error, message}, consumer) do
    {:error, message, consumer}
  end

  defp start({:ok, consumer}, topic, offset, partition, callback) do
    Kafka.Metadata.get_brokers(consumer.metadata, consumer.client_id, topic, partition)
    |> handle_brokers(topic, partition, offset, callback, consumer)
  end

  defp start(error, _topic, _offset, _partition, _callback) do
    error
  end

  defp handle_brokers({:ok, metadata, brokers}, topic, partition, offset, callback, consumer) do
    Enum.each(brokers,
              fn({broker, topics_and_partitions}) ->
                case find_or_create_server(broker) do
                  {:ok, server}     ->
                    GenServer.call(server, {:subscribe, topics_and_partitions, offset, callback})
                  {:error, message} ->
                    IO.puts("Error starting server for broker #{broker}: #{message}")
                end
              end)
    {:ok, consumer}
  end

  defp handle_brokers({:ok, nil}, topic, partition) do
    {:error, "No brokers found for #{topic}, partition #{partition}"}
  end

  defp handle_brokers(error, topic, partition) do
    error
  end

  def subscribe(consumer, topic, offset \\ :earliest, partition \\ :all, callback) do
    Kafka.Metadata.update(consumer.metadata, consumer.client_id)
    |> update_metadata(consumer)
    |> start(topic, offset, partition, callback)
  end

  defp find_or_create_server(broker) do
    name = String.to_atom(Enum.join([broker.host, broker.port], ":"))
    pid = Process.whereis(name)
    pid && {:ok, pid} || create_server(broker, name)
  end

  defp create_server(broker, name) do
    GenServer.start_link(Kafka.Consumer.Server, broker)
    |> register_server(name)
  end

  defp register_server({:ok, server}, name) do
    Process.register(server, name)
    {:ok, server}
  end

  defp register_server(error, _) do
    error
  end

  defmodule EventHandler do
    use GenEvent

    def init(callback) do
      {:ok, callback}
    end

    def handle_event(data, callback) do
      callback.(data)
    end
  end

  defmodule Server do
    use GenServer

    def init(broker, client_id) do
      try do
        Kafka.Connection.connect(broker.host, broker.port, client_id)
        |> initialize_state(broker)
      catch _, exception ->
        IO.inspect exception
      end
    end

    defp initialize_state({:ok, connection}, _) do
      {:ok, %{connection: connection, topics: %{}, agent: nil, fetcher: nil}}
    end

    defp initialize_state({:error, message}, _broker) do
      {:stop, message}
    end

    def create_fetch_request(topics, offset) do
      << 1 :: 16, 0 :: 16, 10000 :: 32, 1 :: 32 >>
    end

    def parse_fetch_response(data) do
    end

    def fetch(connection, agent, offset) do
      topics = Agent.get(agent, &(&1))
      unless Enum.empty?(topics) do
        {connection, data} = Kafka.Connection.send(connection, create_fetch_request(topics, offset))
        parse_fetch_response(data)
        fetch(connection, agent, offset)
      end
    end

    defp event_manager(topic) do
      name = String.to_atom(topic)
      mgr = Process.whereis(name)
      unless mgr do
        {:ok, mgr} = GenEvent.start(name: name)
      end
      mgr
    end

    defp add_handlers(topics_and_partitions, callback) do
      Enum.each(Map.keys(topics_and_partitions),
        fn(topic) -> GenEvent.add_handler(event_manager(topic), Kafka.Consumer.EventHandler, callback) end)
    end

    defp start_agent(state) do
      Agent.start_link(fn -> state.topics end)
    end

    defp set_agent({:ok, agent}, state) do
      {:ok, %{state | agent: agent}}
    end

    defp set_agent({:error, message}, state) do
      {:error, message, state}
    end

    defp update_agent(state) do
      if state.agent do
        {Agent.update(state.agent, fn(_) -> state.topics end), state}
      else
        start_agent(state)
        |> set_agent(state)
      end
    end

    defp spawn_fetcher({:ok, state}, offset) do
      unless state.fetcher && Process.alive?(state.fetcher) do
        fetcher = spawn(fn -> Kafka.Consumer.Server.fetch(state.connection, state.agent, offset) end)
        state = %{state | fetcher: fetcher}
      end
      {:noreply, state}
    end

    defp spawn_fetcher({:error, reason, state}, _offset) do
      {:stop, reason, state}
    end

    def handle_call({:subscribe, topics_and_partitions, offset, callback}, _, state) do
      IO.inspect topics_and_partitions
      IO.inspect offset
      try do
        add_handlers(topics_and_partitions, callback)
        state = Map.put(state, :topics, Map.merge(state.topics, topics_and_partitions))
        |> update_agent
        |> spawn_fetcher(offset)
      catch _, exception ->
        IO.inspect exception
      end
    end

    def handle_call({:unsubscribe, topics}, _, state) do
      {:noreply, Enum.reduce(topics, state, fn(topic) -> Map.delete(topic) end)}
    end
  end
end
