defmodule Kafka.Metadata do
  def new(broker_list, client_id) do
    Kafka.Connection.connect(broker_list, client_id)
    |> get
  end

  defp _get_brokers({:ok, metadata}, topic, partition) do
    {:ok, metadata, get_brokers_for_topic(metadata, %{}, topic, partition)}
  end

  defp _get_brokers({:error, reason}, _, _) do
    {:error, reason}
  end

  def get_brokers(metadata, topic, partition) do
    update(metadata)
    |> _get_brokers(topic, partition)
  end

  defp get_brokers_for_topic(metadata, map, topic, partition) do
    Enum.reduce(metadata.topics[topic][:partitions], map, fn({partition_id, partition_map}, acc) ->
      if partition == partition_id || partition == :all do
        broker = metadata.brokers[partition_map[:leader]]
        if Map.has_key?(acc, broker) do
          if Map.has_key?(acc[broker], topic) do
            Map.put(acc, broker, Map.put(acc[broker], topic, acc[broker][topic] ++ [partition_id]))
          else
            Map.put(acc, broker, Map.put(acc[broker], topic, [partition_id]))
          end
        else
          Map.put(acc, broker, Map.put(%{}, topic, [partition_id]))
        end
      else
        acc
      end
    end)
  end

  defp get({:ok, connection}) do
    Kafka.Connection.send(connection, Kafka.Protocol.Metadata.create_request(connection))
    |> parse_response
  end

  defp get({:error, message}) do
    {:error, "Error connecting to Kafka: #{message}", %{}}
  end

  def update(metadata) do
    if Kafka.Helper.get_timestamp - metadata.timestamp >= 5 * 60 do
      get({:ok, metadata.connection})
    else
      {:ok, metadata}
    end
  end

  defp parse_response({:ok, connection, metadata}) do
    Kafka.Protocol.Metadata.parse_response(connection, metadata)
  end

  defp parse_response(error) do
    error
  end
end
