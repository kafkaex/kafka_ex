defmodule KafkaEx.Protocol.Metadata do
  def create_request(correlation_id, client_id, topic \\ "")

  def create_request(correlation_id, client_id, "") do
    KafkaEx.Protocol.create_request(:metadata, correlation_id, client_id) <> << 0 :: 32 >>
  end

  def create_request(correlation_id, client_id, topic) do
    KafkaEx.Protocol.create_request(:metadata, correlation_id, client_id) <> << 1 :: 32 >> <> << byte_size(topic) :: 16 >> <> topic
  end

  def parse_response(<< _correlation_id :: 32, num_brokers :: 32, rest :: binary >>) do
    parse_broker_list(%{}, num_brokers, rest)
    |> parse_topic_metadata_list
    |> generate_result
  end

  defp generate_result({broker_map, topic_map, _rest}) do
    %{:brokers => broker_map, :topics => topic_map}
  end

  defp parse_broker_list(map, 0, rest) do
    {map, rest}
  end

  defp parse_broker_list(map, num_brokers, << node_id :: 32, host_len :: 16, host :: size(host_len)-binary, port :: 32, rest :: binary >>) do
    parse_broker_list(Map.put(map, node_id, {host, port}), num_brokers-1, rest)
  end

  defp parse_topic_metadata_list({broker_map, << num_topic_metadata :: 32, rest :: binary >>}) do
    parse_topic_metadata(%{}, broker_map, num_topic_metadata, rest)
  end

  defp parse_topic_metadata(map, broker_map, 0, rest) do
    {broker_map, map, rest}
  end

  defp parse_topic_metadata(map, broker_map, num_topic_metadata, << error_code :: 16, topic_len :: 16, topic :: size(topic_len)-binary, num_partitions :: 32, rest :: binary >>) do
    case parse_partition_metadata(%{}, num_partitions, rest) do
      {partition_map, rest} ->
        parse_topic_metadata(Map.put(map, topic, %{:error_code => error_code, :partitions => partition_map}), broker_map, num_topic_metadata-1, rest)
    end
  end

  defp parse_partition_metadata(map, 0, rest) do
    {map, rest}
  end

  defp parse_partition_metadata(map, num_partitions, << error_code :: 16, id :: 32-signed, leader :: 32-signed, rest :: binary >>) do
    {replicas, rest} =  parse_replicas(rest)
    {isrs, rest} =  parse_isrs(rest)
    parse_partition_metadata(Map.put(map, id, %{:error_code => error_code, :leader => leader, :replicas => replicas, :isrs => isrs}), num_partitions-1, rest)
  end

  defp parse_replicas(<< num_replicas :: 32, rest :: binary >>) do
    parse_int32_array(num_replicas, rest)
  end

  defp parse_isrs(<< num_isrs :: 32, rest ::binary >>) do
    parse_int32_array([], num_isrs, rest)
  end

  defp parse_int32_array(array, 0, rest) do
    {Enum.reverse(array), rest}
  end

  defp parse_int32_array(array \\ [], num, << value :: 32, rest :: binary >>) do
    parse_int32_array([value|array], num-1, rest)
  end
end
