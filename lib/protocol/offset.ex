defmodule Kafka.Protocol.Offset do
  def create_request(connection, topic, partition, time) do
    message = Kafka.Protocol.create_request(:offset, connection) <>
      << -1 :: 32, 1 :: 32, byte_size(topic) :: 16, topic :: binary, 1 :: 32, partition :: 32, parse_time(time) :: 64, 1 :: 32>>
  end

  defp parse_time(:latest) do
    -1
  end

  defp parse_time(:earliest) do
    -2
  end

  defp parse_time(time) do
    time
  end

  def parse_response(connection, << _correlation_id :: 32, num_topics :: 32, rest :: binary >>) do
    parse_topics(%{}, num_topics, rest)
    |> generate_result(connection)
  end

  def parse_response(_connection, data) do
    {:error, "Error parsing num_topics in offset response", data}
  end

  defp generate_result({:ok, response_map, _rest}, connection) do
    {:ok, response_map, connection}
  end

  defp generate_result({:error, message, data}, connection) do
    {:error, message, data, connection}
  end

  defp parse_topics(map, 0, rest) do
    {:ok, map, rest}
  end

  defp parse_topics(map, num_topics, << topic_size :: 16, topic :: size(topic_size)-binary, num_partitions :: 32, rest :: binary >>) do
    case parse_partitions(%{}, num_partitions, rest) do
      {:ok, partition_map, rest} -> parse_topics(Map.put(map, topic, partition_map), num_topics-1, rest)
      {:error, message}          -> {:error, message}
    end
  end

  defp parse_topics(_map, _num, data) do
    {:error, "Error parsing topic or number of partitions in offset response", data}
  end

  defp parse_partitions(map, 0, rest) do
    {:ok, map, rest}
  end

  defp parse_partitions(map, num_partitions, << partition :: 32, error_code :: 16, num_offsets :: 32, rest :: binary >>) do
    case parse_offsets([], num_offsets, rest) do
      {:ok, offsets, rest} -> parse_partitions(Map.put(map, partition, %{:error_code => error_code, :offsets => offsets}), num_partitions-1, rest)
      {:error, message}    -> {:error, message}
    end
  end

  defp parse_partitions(_map, _num, data) do
    {:error, "Error parsing partition data in offset response", data}
  end

  defp parse_offsets(list, 0, rest) do
    {:ok, list, rest}
  end

  defp parse_offsets(list, num_partitions, << offset :: 64, rest :: binary >>) do
    parse_offsets(Enum.concat(list, [offset]), num_partitions-1, rest)
  end

  defp parse_offsets(_list, _num, data) do
    {:error, "Error parsing offsets", data}
  end
end
