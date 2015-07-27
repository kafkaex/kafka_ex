defmodule KafkaEx.Protocol.OffsetFetch do
  defmodule Request do
    defstruct consumer_group: "kafka_ex", topic: "", partition: 0
    @type t :: %Request{consumer_group: binary, topic: binary, partition: integer}
  end

  defmodule Response do
    defstruct topic: "", partitions: []
    @type t :: %Response{topic: binary, partitions: list}
    
    def last_offset(:topic_not_found) do
      0
    end

    def last_offset(offset_fetch_data) do
      case offset_fetch_data do
        [] -> 0
        _  -> partitions = offset_fetch_data |> hd |> Map.get(:partitions, [])
          case partitions do
            [] -> 0
            _  -> partitions |> hd |> Map.get(:offset, 0)
          end
      end
    end
  end

  def create_request(correlation_id, client_id, offset_fetch_request) do
    KafkaEx.Protocol.create_request(:offset_fetch, correlation_id, client_id) <> << byte_size(offset_fetch_request.consumer_group) :: 16-signed, offset_fetch_request.consumer_group :: binary, 1 :: 32-signed, byte_size(offset_fetch_request.topic) :: 16-signed, offset_fetch_request.topic :: binary, 1 :: 32-signed, offset_fetch_request.partition :: 32 >>
  end

  def parse_response(<< _correlation_id :: 32-signed, topics_size :: 32-signed, topics_data :: binary >>) do
    parse_topics(topics_size, topics_data)
  end

  defp parse_topics(0, _), do: []

  defp parse_topics(topics_size, << topic_size :: 16-signed, topic :: size(topic_size)-binary, partitions_size :: 32-signed, rest :: binary >>) do
    {partitions, topics_data} = parse_partitions(partitions_size, rest, [])
    [%Response{topic: topic, partitions: partitions} | parse_topics(topics_size - 1, topics_data)]
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(partitions_size, << partition :: 32-signed, offset :: 64-signed, metadata_size :: 16-signed, metadata :: size(metadata_size)-binary, error_code :: 16-signed, rest :: binary >>, partitions) do
    parse_partitions(partitions_size - 1, rest, [%{partition: partition, offset: offset, metadata: metadata, error_code: error_code} | partitions])
  end
end
