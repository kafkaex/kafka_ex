defmodule KafkaEx.Protocol.Produce do
  def create_request(correlation_id, client_id, topic, partition, value, key, required_acks, timeout) do
    message_set = KafkaEx.Util.create_message_set(value, key)
    KafkaEx.Protocol.create_request(:produce, correlation_id, client_id) <>
      << required_acks :: 16, timeout :: 32, 1 :: 32, byte_size(topic) :: 16, topic :: binary, 1 :: 32, partition :: 32, byte_size(message_set) :: 32 >> <>
      message_set
  end

  def parse_response(<< _correlation_id :: 32, num_topics :: 32, rest :: binary >>) do
    parse_topics(%{}, num_topics, rest)
    |> generate_result
  end

  defp generate_result({:ok, response_map, _rest}) do
    {:ok, response_map}
  end

  defp generate_result({:error, message, data}) do
    {:error, message, data}
  end

  defp parse_topics(map, 0, data) do
    {:ok, map, data}
  end

  defp parse_topics(map, num_topics, << topic_size :: 16, topic_name :: size(topic_size)-binary, num_partitions :: 32, rest :: binary >>) do
    case parse_partitions(%{}, num_partitions, rest) do
      {:ok, partition_map, rest} -> parse_topics(Map.put(map, topic_name, partition_map), num_topics - 1, rest)
      {:error, message, data}    -> {:error, message, data}
    end
  end

  defp parse_topics(_map, _num, data) do
    {:error, "Error parsing topic or number of partitions in produce response", data}
  end

  defp parse_partitions(map, 0, rest) do
    {:ok, map, rest}
  end

  defp parse_partitions(map, num_partitions, << partition :: 32, error_code :: 16, offset :: 64, rest :: binary >>) do
    parse_partitions(Map.put(map, partition, %{:error_code => error_code, :offset => offset}), num_partitions-1, rest)
  end

  defp parse_partitions(_map, _num, data) do
    {:error, "Error parsing partition data in fetch response", data}
  end
end
