defmodule KafkaEx.Protocol.OffsetFetch do
  defmodule Request do
    defstruct consumer_group: "kafka_ex", topic: "", partition: 0
    @type t :: %Request{consumer_group: binary, topic: binary, partition: integer}
  end

  defmodule Response do
    defstruct topic: "", partitions: []
    @type t :: %Response{topic: binary, partitions: list}
  end

  def create_request(correlation_id, client_id, offset_fetch_request) do
    KafkaEx.Protocol.create_request(:offset_fetch, correlation_id, client_id) <> << byte_size(offset_fetch_request.consumer_group) :: 16, offset_fetch_request.consumer_group :: binary, 1 :: 32, byte_size(offset_fetch_request.topic) :: 16, offset_fetch_request.topic :: binary, 1 :: 32, offset_fetch_request.partition :: 32 >>
  end

  def parse_response(<< _correlation_id :: 32, topics_count :: 32, topics_data :: binary >>) do
    parse_topics(topics_count, topics_data)
  end

  defp parse_topics(0, _), do: []

  defp parse_topics(topic_count, << topic_size :: 16, topic :: size(topic_size)-binary, partitions_count :: 32, rest :: binary >>) do
    {partitions, topics_data} = parse_partitions(partitions_count, rest, [])
    [%Response{topic: topic, partitions: partitions} | parse_topics(topic_count - 1, topics_data)]
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(partitions_count, << partition :: 32, offset :: 64, metadata_size :: 16, metadata :: size(metadata_size)-binary, _error_code :: 16, rest :: binary >>, partitions) do
    #do something with error_code
    parse_partitions(partitions_count - 1, rest, [%{partition: partition, offset: offset, metadata: metadata} | partitions])
  end
end
