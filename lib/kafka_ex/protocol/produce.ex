defmodule KafkaEx.Protocol.Produce do
  defmodule Request do
    defstruct required_acks: 0, timeout: 0, topic: "", partition: 0, data: ""
    @type t :: %Request{required_acks: binary, timeout: integer, topic: binary, partition: integer, data: binary}
  end

  defmodule Response do
    defstruct topic: "", partitions: []
    @type t :: %Response{topic: binary, partitions: list} 
  end

  def create_request(correlation_id, client_id, topic, partition, value, key, required_acks, timeout) do
    message_set = KafkaEx.Util.create_message_set(value, key)
    KafkaEx.Protocol.create_request(:produce, correlation_id, client_id) <>
      << required_acks :: 16, timeout :: 32, 1 :: 32, byte_size(topic) :: 16, topic :: binary, 1 :: 32, partition :: 32, byte_size(message_set) :: 32 >> <>
      message_set
  end

  def parse_response(<< _correlation_id :: 32, num_topics :: 32, rest :: binary >>), do: parse_topics(num_topics, rest)

  def parse_response(unknown), do: unknown

  defp parse_topics(0, _), do: []

  defp parse_topics(topics_size, << topic_size :: 16, topic :: size(topic_size)-binary, partitions_size :: 32, rest :: binary >>) do
    {partitions, topics_data} = parse_partitions(partitions_size, rest, [])
    [%Response{topic: topic, partitions: partitions} | parse_topics(topics_size - 1, topics_data)]
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(partitions_size, << partition :: 32, error_code :: 16, offset :: 64, rest :: binary >>, partitions) do
    parse_partitions(partitions_size-1, rest, [%{partition: partition, error_code: error_code, offset: offset} | partitions])
  end
end
