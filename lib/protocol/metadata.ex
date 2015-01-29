defmodule Kafka.Protocol.Metadata do
  def create_request(connection) do
    Kafka.Protocol.create_request(:metadata, connection) <> << 0 :: 32 >>
  end

  def parse_response(connection, << _correlation_id :: 32, num_brokers :: 32, rest :: binary >>) do
    timestamp = Kafka.Helper.get_timestamp
    parse_broker_list(%{}, num_brokers, rest)
    |> parse_topic_metadata_list
    |> generate_result(timestamp, connection)
  end

  def parse_response(_connection, data) do
    {:error, "Error parsing number of brokers from metadata response", data}
  end

  defp generate_result({:ok, broker_map, topic_map, rest}, timestamp, connection) do
    {:ok, %{:brokers => broker_map, :topics => topic_map, :timestamp => timestamp, :connection => connection}}
  end

  defp generate_result({:error, message, data}, _ts, connection) do
    {:error, message, data, connection}
  end

  defp parse_broker_list(map, 0, rest) do
    {:ok, map, rest}
  end

  defp parse_broker_list(map, num_brokers, << node_id :: 32, host_len :: 16, host :: size(host_len)-binary, port :: 32, rest :: binary >>) do
    parse_broker_list(Map.put(map, node_id, %{:host => host, :port => port}), num_brokers-1, rest)
  end

  defp parse_broker_list(_map, _brokers, data) do
    {:error, "Error parsing broker list from metadata response", data}
  end

  defp parse_topic_metadata_list({:ok, broker_map, << num_topic_metadata :: 32, rest :: binary >>}) do
    parse_topic_metadata(%{}, broker_map, num_topic_metadata, rest)
  end

  defp parse_topic_metadata_list({:error, message, data}, _map) do
    {:error, message, data}
  end

  defp parse_topic_metadata(map, broker_map, 0, rest) do
    {:ok, broker_map, map, rest}
  end

  defp parse_topic_metadata(map, broker_map, num_topic_metadata, << error_code :: 16, topic_len :: 16, topic :: size(topic_len)-binary, num_partitions :: 32, rest :: binary >>) do
    case parse_partition_metadata(%{}, num_partitions, rest) do
      {:ok, partition_map, rest} ->
        parse_topic_metadata(Map.put(map, topic, error_code: error_code, partitions: partition_map), broker_map, num_topic_metadata-1, rest)
      {:error, message, data} -> {:error, message, data}
    end
  end

  defp parse_topic_metadata(_map, _num, data) do
    {:error, "Error parsing topic metadata from metadata response", data}
  end

  defp parse_partition_metadata(map, 0, rest) do
    {:ok, map, rest}
  end

  defp parse_partition_metadata(map, num_partitions, << error_code :: 16, id :: 32, leader :: 32, rest :: binary >>) do
    case parse_replicas_and_isrs(rest) do
      {:ok, replicas, isrs, rest} ->
        parse_partition_metadata(Map.put(map, id, %{:error_code => error_code, :leader => leader, :replicas => replicas, :isrs => isrs}), num_partitions-1, rest)
      {:error, message, data} -> {:error, message, data}
    end
  end

  defp parse_replicas_and_isrs(<< num_replicas :: 32, rest :: binary >>) do
    parse_int32_array([], num_replicas, rest)
    |> parse_isrs
  end

  defp parse_isrs({:ok, replicas, << num_isrs :: 32, rest ::binary >>}) do
    case parse_int32_array([], num_isrs, rest) do
      {:ok, isrs, rest} -> {:ok, replicas, isrs, rest}
      {:error, message, data} -> {:error, message, data}
    end
  end

  defp parse_isrs({:error, message, data}) do
    {:error, message, data}
  end

  defp parse_int32_array(array, 0, rest) do
    {:ok, array, rest}
  end

  defp parse_int32_array(array, num, << value :: 32, rest :: binary >>) do
    parse_int32_array(Enum.concat(array, [value]), num-1, rest)
  end

  defp parse_int32_array(_array, _num, data) do
    {:error, "Error parsing int32 array from metadata response", data}
  end
end
