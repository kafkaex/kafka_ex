defmodule KafkaEx.Protocol.Offset do
  defmodule Request do
    defstruct replica_id: -1, topic_name: nil, partition: nil, time: -1, max_number_of_offsets: 1
    @type t :: %Request{replica_id: integer, topic_name: binary, partition: integer, time: integer, max_number_of_offsets: integer}
  end

  defmodule Response do
    defstruct topic: nil, partition_offsets: []
    @type t :: %Response{topic: binary, partition_offsets: list}
  end

  def create_request(correlation_id, client_id, topic, partition, time) do
    KafkaEx.Protocol.create_request(:offset, correlation_id, client_id) <>
      << -1 :: 32-signed, 1 :: 32-signed, byte_size(topic) :: 16-signed, topic :: binary, 1 :: 32-signed, partition :: 32-signed, parse_time(time) :: 64, 1 :: 32>>
  end

  def parse_response(<< _correlation_id :: 32-signed, num_topics :: 32-signed, rest :: binary >>), do: parse_topics(num_topics, rest)

  defp parse_time(:latest), do: -1

  defp parse_time(:earliest), do: -2

  @spec parse_time(:calendar.datetime) :: integer
  defp parse_time(time) do
    current_time_in_seconds = time |> :calendar.datetime_to_gregorian_seconds
    unix_epoch_in_seconds = {{1970,1,1},{0,0,0}} |> :calendar.datetime_to_gregorian_seconds
    (current_time_in_seconds - unix_epoch_in_seconds) * 1000
  end

  defp parse_topics(0, _), do: []

  defp parse_topics(topics_size, << topic_size :: 16-signed, topic :: size(topic_size)-binary, partitions_size :: 32-signed, rest :: binary >>) do
    {partitions, topics_data} = parse_partitions(partitions_size, rest)
    [%Response{topic: topic, partition_offsets: partitions} | parse_topics(topics_size - 1, topics_data)]
  end

  defp parse_partitions(partitions_size, rest, partitions \\ [])

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(partitions_size, << partition :: 32-signed, error_code :: 16-signed, offsets_size :: 32-signed, rest :: binary >>, partitions) do
    {offsets, rest} = parse_offsets(offsets_size, rest)
    parse_partitions(partitions_size-1, rest, [%{partition: partition, error_code: error_code, offset: offsets} | partitions])
  end

  defp parse_offsets(offsets_size, rest, offsets \\ [])

  defp parse_offsets(0, rest, offsets), do: {Enum.reverse(offsets), rest}

  defp parse_offsets(offsets_size, << offset :: 64-signed, rest :: binary >>, offsets) do
    parse_offsets(offsets_size-1, rest, [offset|offsets])
  end
end
