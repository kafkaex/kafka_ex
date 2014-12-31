defmodule Kafka.Consumer do
  use GenServer

  def init({broker_list, client_id}) do
    {:ok, pid} = Agent.start_link fn -> 1 end
    {:ok, %{broker_list: broker_list, client_id: client_id, correlation_id: pid}}
  end

  def correlation_id(state) do
    Agent.get_and_update(state.correlation_id, fn(x) -> {x, x+1} end)
  end

  def handle_call({:add_topics, topic_list}, from, %{broker_list: broker_list, client_id: client_id} = state) do
    case Kafka.Connection.connect(broker_list) do
      {:ok, connection} ->
        {broker_map, topic_map} =
          Kafka.Metadata.get_metadata(connection, correlation_id(state), client_id)
        handle_call({:add_topics, topic_list},
                     from,
                     %{brokers: broker_map, topics: topic_map})

      error             -> {:reply, error, state}
    end
  end

  def handle_call({:add_topics, topic_list}, _, state) do
    {:reply, :ok, Enum.reduce(topic_list, state, &subscribe/2)}
  end

  defp subscribe(topic, %{brokers: broker_map, topics: topic_map} = state) do
    state
  end
end
