defmodule Kafka.SimpleConsumer do
  def new(broker_list, client_id, topic, partition) do
    Kafka.Metadata.new(broker_list, client_id)
    |> get_broker(topic, partition)
    |> connect(client_id)
    |> create_consumer(topic, partition)
  end

  def fetch(consumer, offset, wait_time \\ 10, min_bytes \\ 1, max_bytes \\ 1_000_000)

  def fetch({:ok, consumer}, offset, wait_time, min_bytes, max_bytes) do
    fetch({:ok, :cached, consumer.metadata}, consumer, offset, wait_time, min_bytes, max_bytes)
  end

  def fetch({:ok, :cached, metadata}, consumer, offset, wait_time, min_bytes, max_bytes) do
    case Kafka.Connection.send(consumer.connection,
      Kafka.Protocol.Fetch.create_request(consumer.connection, consumer.topic, consumer.partition, offset, wait_time, min_bytes, max_bytes)) do
        {:ok, _, response} -> {:ok, consumer, response}
        {:error, reason}   -> {:error, reason}
    end
  end

  def fetch({:ok, :updated, metadata}, consumer, offset, wait_time, min_bytes, max_bytes) do
    case Kafka.Metadata.get_broker(metadata, consumer.topic, consumer.partition) do
      {:ok, broker, metadata} ->
        cond do
          broker != consumer.broker ->
            connect({:ok, broker, metadata}, consumer.connection.client_id)
            |> update_consumer(consumer, consumer.topic, consumer.partition)
            |> fetch(offset, wait_time, min_bytes, max_bytes)

          true ->
            fetch({:ok, :cached, metadata}, consumer, offset, wait_time, min_bytes, max_bytes)
        end

        {:error, reason} -> {:error, reason}
    end
  end

  def fetch({:error, reason}, _offset, _wait_time, _min_bytes, _max_bytes) do
    {:error, reason}
  end

  def fetch(consumer, offset, wait_time, min_bytes, max_bytes) do
    Kafka.Metadata.update(consumer.metadata)
    |> fetch(consumer, offset, wait_time, min_bytes, max_bytes)
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

  defp connect({:error, reason, metadata}, client_id) do
    {:error, reason, metadata}
  end

  defp connect({:ok, broker, metadata}, client_id) do
    IO.puts("Calling Kafka.Connection.connect")
    case Kafka.Connection.connect(broker, client_id) do
      {:ok, connection} -> {:ok, connection, metadata, broker}
      {:error, reason}  -> {:error, reason, metadata}
    end
  end

  defp get_broker({:ok, metadata}, topic, partition) do
    Kafka.Metadata.get_broker(metadata, topic, partition)
  end

  defp get_broker({:error, reason}, _topic, _partition) do
    {:error, reason, nil}
  end
end
