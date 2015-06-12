defmodule KafkaEx.Protocol.Fetch do
  defmodule Request do
    defstruct replica_id: -1, max_wait_time: 0, min_bytes: 0, topic_name: "", partition: 0, fetch_offset: 0, max_bytes: 0
    @type t :: %Request{replica_id: integer, max_wait_time: integer, topic_name: binary, partition: integer, fetch_offset: integer, max_bytes: integer}
  end

  defmodule Response do
    defstruct topic: "", partitions: []
    @type t :: %Response{topic: binary, partitions: list} 
  end

  def create_request(correlation_id, client_id, topic, partition, offset, wait_time, min_bytes, max_bytes) do
    KafkaEx.Protocol.create_request(:fetch, correlation_id, client_id) <>
      << -1 :: 32, wait_time :: 32, min_bytes :: 32, 1 :: 32, byte_size(topic) :: 16, topic :: binary,
         1 :: 32, partition :: 32, offset :: 64, max_bytes :: 32 >>
  end

  def parse_response(<< _correlation_id :: 32, topics_size :: 32, rest :: binary>>) do
    parse_topics(topics_size, rest)
  end

  defp parse_topics(0, _), do: []

  defp parse_topics(topics_size, << topic_size :: 16, topic :: size(topic_size)-binary, partitions_size :: 32, rest :: binary >>) do
    {partitions, topics_data} = parse_partitions(partitions_size, rest, [])
    [%Response{topic: topic, partitions: partitions} | parse_topics(topics_size - 1, topics_data)]
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(partitions_size, << partition :: 32, error_code :: 16, hw_mark_offset :: 64,
  msg_set_size :: 32, msg_set_data :: size(msg_set_size)-binary, rest :: binary >>, partitions) do
    parse_partitions(partitions_size - 1, rest, [%{partition: partition, error_code: error_code, hw_mark_offset: hw_mark_offset, message_set: elem(KafkaEx.Util.parse_message_set([], msg_set_data), 1)} | partitions])
  end
end
