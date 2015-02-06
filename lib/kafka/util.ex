defmodule Kafka.Util do
  def create_message_set(value, key \\ nil) do
    message = create_message(value, key)
    << 0 :: 64, byte_size(message) :: 32 >> <> message
  end

  def create_message(value, key \\ nil) do
    sub = << 0 :: 8, 0 :: 8 >> <> bytes(key) <> bytes(value)
    crc = :erlang.crc32(sub)
    << crc :: 32 >> <> sub
  end

  def bytes(data) do
    cond do
      is_nil(data) -> << -1 :: 32 >>
      true ->
        size = byte_size(data)
        case size do
          0 -> << -1 :: 32 >>
          _ -> << size :: 32, data :: binary >>
        end
    end
  end

  def parse_message_set(list, << >>) do
    {:ok, list}
  end

  def parse_message_set(list, << offset :: 64, msg_size :: 32, msg_data :: size(msg_size)-binary, rest :: binary >>) do
    case parse_message(msg_data) do
      {:ok, message} -> parse_message_set(Enum.concat(list, [Map.put(message, :offset, offset)]), rest)
      {:error, error_message} -> {:error, error_message}
    end
  end

  def parse_message_set(_map, data) do
    {:error, "Error parsing message set in fetch response", data}
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

  def parse_key(_crc, _attributes, data) do
    {:error, "Error parsing key from message in fetch response", data}
  end

  def parse_value(crc, attributes, key, << -1 :: 32-signed >>) do
    {:ok, %{:crc => crc, :attributes => attributes, :key => key, :value => nil}}
  end

  def parse_value(crc, attributes, key, << value_size :: 32, value :: size(value_size)-binary >>) do
    {:ok, %{:crc => crc, :attributes => attributes, :key => key, :value => value}}
  end

  def parse_value(_crc, _attributes, _key, data) do
    {:error, "Error parsing value from message in fetch response", data}
  end
end
