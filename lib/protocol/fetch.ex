defmodule Kafka.Protocol.Fetch do
  def create_request(connection, topic, partition, offset, wait_time, min_bytes, max_bytes) do
    Kafka.Protocol.create_request(:fetch, connection) <>
      << -1 :: 32, wait_time :: 32, min_bytes :: 32, byte_size(topic) :: 16, topic :: binary,
         partition :: 32, offset :: 64, max_bytes :: 32 >>
  end

  def parse_response(connection, << _correlation_id :: 32, num_topics :: 32, rest :: binary>>) do
    parse_topics(%{}, num_topics, rest)
    |> generate_result(connection)
  end

  def parse_response(_connection, data) do
    {:error, "Error parsing num_topics in fetch response", data}
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
    {:error, "Error parsing topic or number of partitions in fetch response", data}
  end

  defp parse_partitions(map, 0, rest) do
    {:ok, map, rest}
  end

  defp parse_partitions(map, num_partitions, << partition :: 32, error_code :: 16, hw_mark_offset :: 64, msg_set_size :: 32, msg_set_data :: size(msg_set_size)-binary, rest :: binary >>) do
    case parse_message_set([], msg_set_data) do
      {:ok, message_set} ->
        parse_partitions(Map.put(map, partition, %{:error_code => error_code, :hw_mark_offset => hw_mark_offset, :message_set => message_set}), num_partitions-1, rest)
      {:error, message}        -> {:error, message}
    end
  end

  defp parse_partitions(_map, _num, data) do
    {:error, "Error parsing partition data in fetch response", data}
  end

  defp parse_message_set(list, << >>) do
    {:ok, list}
  end

  defp parse_message_set(list, << offset :: 64, msg_size :: 32, msg_data :: size(msg_size)-binary, rest :: binary >>) do
    case parse_message(msg_data) do
      {:ok, message} -> parse_message_set(Enum.concat(list, [Map.put(message, :offset, offset)]), rest)
      {:error, error_message} -> {:error, error_message}
    end
  end

  defp parse_message_set(_map, data) do
    {:error, "Error parsing message set in fetch response", data}
  end

  defp parse_message(<< crc :: 32, _magic :: 8, attributes :: 8, rest :: binary>>) do
    parse_key(crc, attributes, rest)
  end

  defp parse_key(crc, attributes, << -1 :: 32-signed, rest :: binary >>) do
    parse_value(crc, attributes, nil, rest)
  end

  defp parse_key(crc, attributes, << key_size :: 32, key :: size(key_size)-binary, rest :: binary >>) do
    parse_value(crc, attributes, key, rest)
  end

  defp parse_key(_crc, _attributes, data) do
    {:error, "Error parsing key from message in fetch response", data}
  end

  defp parse_value(crc, attributes, key, << -1 :: 32-signed >>) do
    {:ok, %{:crc => crc, :attributes => attributes, :key => key, :value => nil}}
  end

  defp parse_value(crc, attributes, key, << value_size :: 32, value :: size(value_size)-binary >>) do
    {:ok, %{:crc => crc, :attributes => attributes, :key => key, :value => value}}
  end

  defp parse_value(_crc, _attributes, _key, data) do
    {:error, "Error parsing value from message in fetch response", data}
  end
end
