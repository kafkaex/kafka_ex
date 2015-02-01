defmodule Kafka.Producer do
  def new(broker_list, client_id, topic, partition) do
    Kafka.Metadata.new(broker_list, client_id)
    |> Kafka.Util.get_broker(topic, partition)
    |> Kafka.Util.connect(client_id)
    |> create_producer(topic, partition)
  end

  defp create_producer({:ok, connection, metadata, broker}, topic, partition) do
    {:ok, %{:connection => connection, :broker => broker, :metadata => metadata, :topic => topic, :partition => partition}}
  end

  defp create_producer({:error, reason, nil}, _topic, _partition) do
    {:error, reason}
  end

  defp create_producer({:error, reason, metadata}, _topic, _partition) do
    {:error, reason, metadata}
  end

  def produce(producer, value, key \\ nil, required_acks \\ 0, timeout \\ 100) do
    Kafka.Metadata.update(producer.metadata)
    |> produce(producer, value, key, required_acks, timeout)
  end

  def produce({:ok, :updated, metadata}, producer, value, key, required_acks, timeout) do
    case rebalance(metadata, producer) do
      {:ok, producer} -> produce({:ok, :cached, metadata}, producer, value, key, required_acks, timeout)
      {:error, reason} -> {:error, reason}
    end
  end

  def produce({:ok, :cached, metadata}, producer, value, key, 0, timeout) do
    Kafka.Protocol.Produce.create_request(producer.connection, producer.topic, producer.partition, value, key, 0, timeout)
    |> Kafka.Connection.send(producer.connection)
    |> create_result(producer)
  end

  def produce({:ok, :cached, metadata}, producer, value, key, required_acks, timeout) do
    Kafka.Protocol.Produce.create_request(producer.connection, producer.topic, producer.partition, value, key, required_acks, timeout)
    |> Kafka.Connection.send_and_return_response(producer.connection)
    |> parse_response(producer)
  end

  defp create_result({:ok, connection}, producer) do
    {:ok, %{producer | :connection => connection}}
  end

  defp create_result({:error, reason}, producer) do
    {:error, reason, producer}
  end

  defp rebalance(metadata, producer) do
    case Kafka.Metadata.get_broker(metadata, producer.topic, producer.partition) do
      {:ok, broker, metadata} ->
        cond do
          broker != producer.broker ->
            Kafka.Util.connect({:ok, broker, metadata}, producer.connection.client_id)
            |> update_producer(producer, producer.topic, producer.partition)

          true -> {:ok, producer}
        end

        {:error, reason} -> {:error, reason}
    end
  end

  defp update_producer({:ok, connection, metadata, broker}, producer, topic, partition) do
    Kafka.Connection.close(producer.connection)
    {:ok, %{producer | :metadata => metadata, :broker => broker, :connection => connection}}
  end

  defp update_producer({:error, reason, _}, _topic, _partition) do
    {:error, reason}
  end

  defp parse_response({:ok, connection, data}, producer) do
    %{producer | connection: connection}
    Kafka.Protocol.Produce.parse_response(producer, data)
  end

  defp parse_response({:error, reason}, _) do
    {:error, reason}
  end
end
