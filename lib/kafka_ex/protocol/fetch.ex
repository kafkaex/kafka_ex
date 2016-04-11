defmodule KafkaEx.Protocol.Fetch do
  alias KafkaEx.Protocol

  defmodule Response do
    defstruct topic: nil, partitions: []
    @type t :: %Response{topic: binary, partitions: list} 
  end

  defmodule Message do
    defstruct attributes: 0, crc: nil, offset: nil, key: nil, value: nil
    @type t :: %Message{attributes: integer, crc: integer, offset: integer, key: binary, value: binary}
  end

  def create_request(correlation_id, client_id, topic, partition, offset, wait_time, min_bytes, max_bytes) do
    KafkaEx.Protocol.create_request(:fetch, correlation_id, client_id) <>
      << -1 :: 32 - signed, wait_time :: 32 - signed, min_bytes :: 32 - signed, 1 :: 32 - signed, byte_size(topic) :: 16 - signed, topic :: binary,
         1 :: 32 - signed, partition :: 32 - signed, offset :: 64, max_bytes :: 32 >>
  end

  def parse_response(<< _correlation_id :: 32 - signed, topics_size :: 32 - signed, rest :: binary>>) do
    parse_topics(topics_size, rest)
  end

  defp parse_topics(0, _), do: []
  defp parse_topics(topics_size, << topic_size :: 16 - signed, topic :: size(topic_size) - binary, partitions_size :: 32 - signed, rest :: binary >>) do
    {partitions, topics_data} = parse_partitions(partitions_size, rest, [])
    [%Response{topic: topic, partitions: partitions} | parse_topics(topics_size - 1, topics_data)]
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}
  defp parse_partitions(partitions_size, << partition :: 32 - signed, error_code :: 16 - signed, hw_mark_offset :: 64 - signed,
  msg_set_size :: 32 - signed, msg_set_data :: size(msg_set_size) - binary, rest :: binary >>, partitions) do
    {:ok, message_set, last_offset} = parse_message_set([], msg_set_data)
    parse_partitions(partitions_size - 1, rest, [%{partition: partition, error_code: Protocol.error(error_code), hw_mark_offset: hw_mark_offset, message_set: message_set, last_offset: last_offset} | partitions])
  end

  defp parse_message_set([], << >>) do
    {:ok, [], nil}
  end
  defp parse_message_set(list, << offset :: 64, msg_size :: 32, msg_data :: size(msg_size) - binary, rest :: binary >>) do
    {:ok, message} = parse_message(%Message{offset: offset}, msg_data)
    parse_message_set(append_messages(message,  list), rest)
  end
  defp parse_message_set([last|_] = list, _) do
    {:ok, Enum.reverse(list), last.offset}
  end

  # handles the single message case and the batch (compression) case
  defp append_messages([], list) do
    list
  end
  defp append_messages([message | messages], list) do
    append_messages(messages, [message | list])
  end
  defp append_messages(message, list) do
    [message | list]
  end

  defp parse_message(message = %Message{}, << crc :: 32, _magic :: 8, attributes :: 8, rest :: binary>>) do
    maybe_decompress(%{message | crc: crc, attributes: attributes}, rest)
  end

  defp maybe_decompress(message = %Message{attributes: 0}, rest) do
    parse_key(message, rest)
  end 
  defp maybe_decompress(%Message{attributes: attributes}, rest) do
    << -1 :: 32 - signed, value_size :: 32, value :: size(value_size) - binary >> = rest
    decompressed = KafkaEx.Compression.decompress(attributes, value)
    {:ok, msg_set, _offset} = parse_message_set([], decompressed)
    {:ok, msg_set}
  end

  defp parse_key(message = %Message{}, << -1 :: 32 - signed, rest :: binary >>) do
    parse_value(%{message | key: nil}, rest)
  end
  defp parse_key(message = %Message{}, << key_size :: 32, key :: size(key_size) - binary, rest :: binary >>) do
    parse_value(%{message | key: key}, rest)
  end

  defp parse_value(message = %Message{}, << -1 :: 32 - signed >>) do
    {:ok, %{message | value: nil}}
  end
  defp parse_value(message = %Message{}, << value_size :: 32, value :: size(value_size) - binary >>) do
    {:ok, %{message | value: value}}
  end
end
