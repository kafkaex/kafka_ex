defmodule Kafka.Consumer do
  use GenServer

  def new({broker_list, client_id}) do
    %{broker_list: broker_list, client_id: client_id}
  end

  def subscribe(consumer, topic_list, callback) do
    metadata = Kafka.Metadata.get(consumer.broker_list, consumer.client_id)
    Enum.each(Kafka.Metadata.get_brokers(metadata, consumer.client_id, topic_list),
              fn({broker, topics_and_partitions}) ->
                case find_or_create_server(broker) do
                  {:ok, server}     ->
                    GenServer.call(server, {:subscribe, topics_and_partitions, callback})
                  {:error, message} ->
                    IO.puts("Error starting server for broker #{broker}: #{message}")
                end
              end)
  end

  defp find_or_create_server(broker) do
    name = String.to_atom(Enum.join([broker.host, broker.port], ":"))
    pid = Process.whereis(name)
    pid && {:ok, pid} || create_server(broker, name)
  end

  defp create_server(broker, name) do
    case GenServer.start_link(Kafka.Consumer.Server, broker) do
      {:ok, pid} ->
        Process.register(pid, name)
        {:ok, pid}
      error      -> error
    end
  end

  defmodule EventHandler do
    use GenEvent

    def init(topic) do
      Process.register(self, String.to_atom(topic))
      {:ok, []}
    end

    def handle_call({:register, callback}, state) do
      {:ok, nil, state ++ [callback]}
    end

    def handle_event(data, callbacks) do
      Enum.each(callbacks, fn(callback) -> callback.(data) end)
    end
  end

  defmodule Server do
    def init(broker) do
      case Kafka.Connection.connect(broker.host, broker.port) do
        {:ok, connection} -> {:ok, %{connection: connection, topics: %{}, agent: nil, fetcher: nil}}
        {:error, message} ->
          IO.puts("Error connection to #{broker.host}:#{broker.port}: #{message}")
          {:stop, message}
      end
    end

    def create_fetch_request(topics) do
    end

    def parse_and_send_data(data) do
    end

    def fetch(connection, agent) do
      topics = Agent.get(agent, &(&1))
      unless Enum.empty?(topics) do
        {connection, data} = Kafka.Connection.send(connection, create_fetch_request(topics))
        parse_and_send_data(data)
        fetch(connection, agent)
      end
    end

    def handle_call({:subscribe, topics_and_partitions, callback}, _, state) do
      state = %{state | topics: Map.merge(state.topics, topics_and_partitions)}
      if state.agent do
        Agent.update(state.agent, fn(_) -> state.topics end)
      else
        agent = Agent.start_link(fn -> state.topics end)
        state = %{state | agent: agent}
      end
      unless state.pid && Process.alive?(state.pid) do
        fetcher = spawn(fn -> Kafka.Consumer.Server.fetch(state.connection, agent) end)
        state = %{state | fetcher: fetcher}
      end
      {:noreply, state}
    end

    def handle_call({:unsubscribe, topics}, _, state) do
      {:noreply, Enum.reduce(topics, state, fn(topic) -> Map.delete(topic) end)}
    end

    defp do_subscribe(topic, %{brokers: broker_map, topics: topic_map} = state) do
      state
    end

    def start(broker_list, client_id) do
      GenServer.start(__MODULE__, {broker_list, client_id})
    end
  end
end
