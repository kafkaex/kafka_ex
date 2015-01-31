defmodule Kafka.Metadata do
  def new(broker_list, client_id) do
    Kafka.Connection.connect(broker_list, client_id)
    |> get_metadata
  end

  defp get_metadata({:ok, connection}) do
    Kafka.Protocol.Metadata.create_request(connection)
    |> Kafka.Connection.send_and_return_response(connection)
    |> parse_response
  end

  defp get_metadata({:error, message}) do
    {:error, "Error connecting to Kafka: #{message}"}
  end

  def update(metadata) do
    if Kafka.Helper.get_timestamp - metadata.timestamp >= 5 * 60 do
      case get_metadata({:ok, metadata.connection}) do
        {:ok, metadata}  -> {:ok, :updated, metadata}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, :cached, metadata}
    end
  end

  defp parse_response({:ok, connection, metadata}) do
    Kafka.Protocol.Metadata.parse_response(connection, metadata)
  end

  defp parse_response(error) do
    error
  end

  defp get_broker_from_metadata({:ok, _, metadata}, topic, partition) do
    get_brokers_for_topic(metadata, topic, partition)
  end

  defp get_broker_from_metadata({:error, reason}, _, _) do
    {:error, reason}
  end

  def get_broker(metadata, topic, partition) do
    update(metadata)
    |> get_broker_from_metadata(topic, partition)
  end

  defp get_leader_for_topic_partition(metadata, topic, partition) do
    metadata.topics[topic][:partitions][partition][:leader]
  end

  defp get_brokers_for_topic(metadata, topic, partition) do
    get_leader_for_topic_partition(metadata, topic, partition)
    |> get_broker_for_broker_id(metadata)
  end

  defp get_broker_for_broker_id(nil, metadata) do
    {:error, "No leader found for topic and partition", metadata}
  end

  defp get_broker_for_broker_id(broker_id, metadata) do
    case metadata.brokers[broker_id] do
      nil      -> {:error, "no broker found for broker id #{broker_id}", metadata}
      broker   -> {:ok, broker, metadata}
    end
  end
end
