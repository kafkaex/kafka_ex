defmodule KafkaEx.Util do
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
    parse_message_set([Map.put(message, :offset, offset)|list], rest)
  end

  def parse_message_set([], _) do
    {:ok, [], nil}
  end

  def parse_message_set([last|_] = list, _) do
    {:ok, Enum.reverse(list), last.offset}
  end

  def parse_message(<< crc :: 32, _magic :: 8, attributes :: 8, rest :: binary>>) do
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
end
