defmodule KafkaEx.Util do
  @snappy_attribute 2

  def current_timestamp do
    {mega, seconds, micros} = :os.timestamp
    mega * 1_000_000_000 + seconds * 1_000 + round(micros / 1_000)
  end

  def parse_message_set([], << >>) do
    {:ok, [], nil}
  end

  def parse_message_set([last|_] = list, << >>) do
    {:ok, Enum.reverse(list), last.offset}
  end

  def parse_message_set(list, << offset :: 64, msg_size :: 32, msg_data :: size(msg_size)-binary, rest :: binary >>) do
    {:ok, message} = parse_message(msg_data)
    parse_message_set(append_messages(set_offsets(message, offset),  list), rest)
  end

  def parse_message_set([], _) do
    {:ok, [], nil}
  end

  def parse_message_set([last|_] = list, _) do
    {:ok, Enum.reverse(list), last.offset}
  end

  # compressed batches give us the offset of the LAST message in the batch
  # it is up to us to correctly assign the intermediate offsets
  def set_offsets(messages, offset) when is_list(messages) do
    [messages, (0..length(messages)-1) |> Enum.to_list |> Enum.reverse]
    |> List.zip
    |> Enum.map(fn({m, o}) -> set_offsets(m, offset - o) end)
  end
  def set_offsets(message, offset) do
    Map.put(message, :offset, offset)
  end

  # handles the single message case and the batch (compression) case
  def append_messages([], list) do
    list
  end
  def append_messages([message | messages], list) do
    append_messages(messages, [message | list])
  end
  def append_messages(message, list) do
    [message | list]
  end

  def parse_message(<< crc :: 32, _magic :: 8, attributes :: 8, rest :: binary>>) do
    parse_message_value(crc, attributes, rest)
  end

  def parse_message_value(_crc, @snappy_attribute, rest) do
    << -1 :: 32-signed, value_size :: 32, value :: size(value_size)-binary >> = rest
    decompress_snappy(value)
  end
  def parse_message_value(crc, attributes, rest) do
    parse_key(crc, attributes, rest)
  end 

  def parse_key(crc, attributes, << -1 :: 32-signed, rest :: binary >>) do
    parse_value(crc, attributes, nil, rest)
  end

  def parse_key(crc, attributes, << key_size :: 32, key :: size(key_size)-binary, rest :: binary >>) do
    parse_value(crc, attributes, key, rest)
  end

  def parse_value(crc, attributes, key, << -1 :: 32-signed >>) do
    {:ok, %{:crc => crc, :attributes => attributes, :key => key, :value => nil}}
  end

  def parse_value(crc, attributes, key, << value_size :: 32, value :: size(value_size)-binary >>) do
    {:ok, %{:crc => crc, :attributes => attributes, :key => key, :value => value}}
  end

  def decompress_snappy(<< _snappy_header :: 64, _snappy_version_info :: 64, _size :: 32, value :: binary >>) do
    {:ok, decompressed} = :snappy.decompress(value)
    parse_decompressed(decompressed, [])
  end
  def decompress(value, _) do
    value
  end

  def parse_decompressed(<< offset :: 64, size :: 32, rest :: binary >>, msgs) do
    << message :: size(size)-binary, rest :: binary >> = rest
    {:ok, message} = parse_message(message)
    parse_decompressed(rest, [Map.put(message, :offset, offset) | msgs])
  end
  def parse_decompressed(<<>>, msgs) do
    {:ok, Enum.reverse(msgs)}
  end

end
