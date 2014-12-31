defmodule Kafka.Consumer do
  use GenServer

  def init({broker_list, client_id}) do
    {:ok, %{broker_list: broker_list, client_id: client_id, correlation_id: 1}}
  end

  def handle_call({:add_topics, topic_list}, from, %{broker_list: broker_list, client_id: client_id} = state) do
    case Kafka.Connection.connect(broker_list) do
      {:ok, connection} ->
        {broker_map, topic_map} =
          Kafka.Metadata.get_metadata(connection, state.correlation_id, client_id)
        Kafka.Connection.close(connection)
        handle_call({:add_topics, topic_list},
                     from,
                     %{brokers: broker_map, topics: topic_map})

      error             -> {:reply, error, state}
    end
  end

  def handle_call({:add_topics, topic_list}, _, state) do
    IO.inspect({:reply, :ok, Enum.reduce(topic_list, state, &subscribe/2)})
  end

  defp subscribe(topic, %{brokers: broker_map, topics: topic_map} = state) do
    state
  end
end
