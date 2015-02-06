defmodule Kafka.SimpleConsumer do
  def new(broker_list, client_id, topic, partition) do
    Kafka.Metadata.new(broker_list, client_id)
    |> Kafka.Util.get_broker(topic, partition)
    |> Kafka.Util.connect(client_id)
    |> create_consumer(topic, partition)
  end

  def fetch(consumer, offset, wait_time \\ 10, min_bytes \\ 1, max_bytes \\ 1_000_000) do
    case get_offset(consumer, offset) do
      {:ok, offset} -> 
        Kafka.Metadata.update(consumer.metadata)
        |> fetch_with_update(consumer, offset, wait_time, min_bytes, max_bytes)

      {:error, reason} -> {:error, reason}
    end
  end

  defp get_offset(consumer, offset) do
    case offset do
      :latest   -> lookup_offset(consumer, -1)
      :earliest -> lookup_offset(consumer, -2)
      _         -> {:ok, offset}
    end
  end

  defp lookup_offset(consumer, _value) do
    Kafka.Protocol.Offset.create_request(consumer.connection, consumer.topic, consumer.partition, -1)
    |> Kafka.Connection.send_and_return_response(consumer.connection)
    |> parse_offset_response(consumer)
  end

  defp parse_offset_response({:ok, connection, data}, consumer) do
    consumer = %{consumer | connection: connection}
    case Kafka.Protocol.Offset.parse_response(consumer, data) do
      {:ok, response, _connection} -> {:ok, List.first(response[consumer.topic][consumer.partition].offsets)}
      {:error, reason}            -> {:error, reason}
    end
  end

  defp parse_offset_response({:error, reason}, _) do
    {:error, reason}
  end

  defp fetch_with_update({:ok, :cached, _metadata}, consumer, offset, wait_time, min_bytes, max_bytes) do
    Kafka.Protocol.Fetch.create_request(consumer.connection, consumer.topic, consumer.partition, offset, wait_time, min_bytes, max_bytes)
    |> Kafka.Connection.send_and_return_response(consumer.connection)
    |> parse_response(consumer)
  end

  defp fetch_with_update({:ok, :updated, metadata}, consumer, offset, wait_time, min_bytes, max_bytes) do
    rebalance(metadata, consumer)
    |> fetch_with_rebalance(offset, wait_time, min_bytes, max_bytes)
  end

  defp fetch_with_update({:error, reason}, _consumer, _offset, _wait_time, _min_bytes, _max_bytes) do
    {:error, reason}
  end

  defp fetch_with_rebalance({:ok, consumer}, offset, wait_time, min_bytes, max_bytes) do
    fetch_with_update({:ok, :cached, consumer.metadata}, consumer, offset, wait_time, min_bytes, max_bytes)
  end

  defp rebalance(metadata, consumer) do
    case Kafka.Metadata.get_broker(metadata, consumer.topic, consumer.partition) do
      {:ok, broker, metadata} ->
        cond do
          broker != consumer.broker ->
            Kafka.Util.connect({:ok, broker, metadata}, consumer.connection.client_id)
            |> update_consumer(consumer)

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

  defp update_consumer({:ok, connection, metadata, broker}, consumer) do
    Kafka.Connection.close(consumer.connection)
    {:ok, %{consumer | :metadata => metadata, :broker => broker, :connection => connection}}
  end

  defp update_consumer({:error, reason, _}, _consumer) do
    {:error, reason}
  end
end
