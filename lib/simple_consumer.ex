defmodule Kafka.SimpleConsumer do
  def new(broker_list, client_id, topic, partition) do
    Kafka.Metadata.new(broker_list, client_id)
    |> Kafka.Util.get_broker(topic, partition)
    |> Kafka.Util.connect(client_id)
    |> create_consumer(topic, partition)
  end

  def fetch(consumer, offset, wait_time \\ 10, min_bytes \\ 1, max_bytes \\ 1_000_000)

  def fetch({:ok, consumer}, offset, wait_time, min_bytes, max_bytes) do
    fetch({:ok, :cached, consumer.metadata}, consumer, offset, wait_time, min_bytes, max_bytes)
  end

  def fetch({:ok, :cached, metadata}, consumer, offset, wait_time, min_bytes, max_bytes) do
    Kafka.Protocol.Fetch.create_request(consumer.connection, consumer.topic, consumer.partition, offset, wait_time, min_bytes, max_bytes)
    |> Kafka.Connection.send_and_return_response(consumer.connection)
    |> parse_response(consumer)
  end

  def fetch({:ok, :updated, metadata}, consumer, offset, wait_time, min_bytes, max_bytes) do
    rebalance(metadata, consumer)
    |> fetch(offset, wait_time, min_bytes, max_bytes)
  end

  def fetch({:error, reason}, _offset, _wait_time, _min_bytes, _max_bytes) do
    {:error, reason}
  end

  def fetch(consumer, offset, wait_time, min_bytes, max_bytes) do
    Kafka.Metadata.update(consumer.metadata)
    |> fetch(consumer, offset, wait_time, min_bytes, max_bytes)
  end

  defp rebalance(metadata, consumer) do
    case Kafka.Metadata.get_broker(metadata, consumer.topic, consumer.partition) do
      {:ok, broker, metadata} ->
        cond do
          broker != consumer.broker ->
            Kafka.Util.connect({:ok, broker, metadata}, consumer.connection.client_id)
            |> update_consumer(consumer, consumer.topic, consumer.partition)

          true -> {:ok, consumer}
        end

        {:error, reason} -> {:error, reason}
    end
  end

  defp parse_response({:ok, connection, data}, consumer) do
    %{consumer | connection: connection}
    Kafka.Protocol.Fetch.parse_response(consumer, data)
  end

  defp parse_response({:error, reason}, _) do
    {:error, reason}
  end

  defp create_consumer({:ok, connection, metadata, broker}, topic, partition) do
    {:ok, %{:connection => connection, :broker => broker, :metadata => metadata, :topic => topic, :partition => partition}}
  end

  defp create_consumer({:error, reason, nil}, _topic, _partition) do
    {:error, reason}
  end

  defp create_consumer({:error, reason, metadata}, _topic, _partition) do
    {:error, reason, metadata}
  end

  defp update_consumer({:ok, connection, metadata, broker}, consumer, topic, partition) do
    Kafka.Connection.close(consumer.connection)
    {:ok, %{consumer | :metadata => metadata, :broker => broker, :connection => connection}}
  end

  defp update_consumer({:error, reason, _}, _topic, _partition) do
    {:error, reason}
  end
end
