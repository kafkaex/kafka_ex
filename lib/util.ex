defmodule Kafka.Util do
  def get_broker({:ok, metadata}, topic, partition) do
    Kafka.Metadata.get_broker(metadata, topic, partition)
  end

  def get_broker({:error, reason}, _topic, _partition) do
    {:error, reason, nil}
  end

  def connect({:error, reason, metadata}, client_id) do
    {:error, reason, metadata}
  end

  def connect({:ok, broker, metadata}, client_id) do
    case Kafka.Connection.connect(broker, client_id) do
      {:ok, connection} -> {:ok, connection, metadata, broker}
      {:error, reason}  -> {:error, reason, metadata}
    end
  end

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
end
