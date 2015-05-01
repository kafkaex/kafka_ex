defmodule KafkaEx.Protocol.OffsetFetch do
  def create_request(correlation_id, client_id, consumer_group, topic, partition) do
    KafkaEx.Protocol.create_request(:offset_fetch, correlation_id, client_id) <> << byte_size(consumer_group) :: 16, consumer_group :: binary, 1 :: 32, byte_size(topic) :: 16, topic :: binary, 1 :: 32, partition :: 32 >>
  end

  #<< corr_id ::32, topic_counts :: 32, topic_size :: 16, topic :: size(topic_size)-binary, partitions_count :: 32, partition :: 32, offset :: 64, metadata_size :: 16, metadata :: size(metadata_size)-binary, error_code :: 16, rest :: binary >>

  def parse_response(<< _correlation_id :: 32, topics_count :: 32, topics_data :: binary >>) do
    parse_topics(topics_count, topics_data)
  end

  defp parse_topics(0, _), do: []

  defp parse_topics(topic_count, << topic_size :: 16, topic :: size(topic_size)-binary, partitions_count :: 32, rest :: binary >>) do
    {partitions, topics_data} = parse_partitions(partitions_count, rest, [])
    [Map.put(%{}, topic, partitions) | parse_topics(topic_count - 1, topics_data)]
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(partitions_count, << partition :: 32, offset :: 64, metadata_size :: 16, metadata :: size(metadata_size)-binary, _error_code :: 16, rest :: binary >>, partitions) do
    #do something with error_code
    parse_partitions(partitions_count - 1, rest, [%{partition: partition, offset: offset, metadata: metadata} | partitions])
  end
end
