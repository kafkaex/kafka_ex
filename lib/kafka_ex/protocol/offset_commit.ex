defmodule KafkaEx.Protocol.OffsetCommit do
  def create_request(correlation_id, client_id, consumer_group, topic, partition, offset, metadata) do
    KafkaEx.Protocol.create_request(:offset_commit, correlation_id, client_id) <> << byte_size(consumer_group) :: 16, consumer_group :: binary, 1 :: 32, byte_size(topic) :: 16, topic :: binary, 1 :: 32, partition :: 32, offset :: 64, byte_size(metadata) :: 16, metadata :: binary >>
  end

  def parse_response(<< _correlation_id :: 32, topics_count :: 32, topics_data :: binary >>) do
    parse_topics(topics_count, topics_data)
  end

  defp parse_topics(0, _), do: []

  defp parse_topics(topic_count, << topic_size :: 16, topic :: size(topic_size)-binary, partitions_count :: 32, rest :: binary >>) do
    {partitions, topics_data} = parse_partitions(partitions_count, rest, [])
    [%{topic: topic, partitions: partitions} | parse_topics(topic_count - 1, topics_data)]
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(partitions_count, << partition :: 32, _error_code :: 16, rest :: binary >>, partitions) do
    #do something with error_code
    parse_partitions(partitions_count - 1, rest, [partition | partitions])
  end
end
