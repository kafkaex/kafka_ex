defmodule KafkaEx.Protocol.Fetch do
  alias KafkaEx.Protocol
  alias KafkaEx.Compression
  import KafkaEx.Protocol.Common

  @moduledoc """
  Implementation of the Kafka Fetch request and response APIs
  """

  defmodule Request do
    @moduledoc false
    defstruct correlation_id: nil,
              client_id: nil,
              topic: nil,
              partition: nil,
              offset: nil,
              wait_time: nil,
              min_bytes: nil,
              max_bytes: nil,
              auto_commit: nil

    @type t :: %Request{
            correlation_id: integer,
            client_id: binary,
            topic: binary,
            partition: integer,
            offset: integer,
            wait_time: integer,
            min_bytes: integer,
            max_bytes: integer
          }
  end

  defmodule Response do
    @moduledoc false
    defstruct topic: nil, partitions: []
    @type t :: %Response{topic: binary, partitions: list}

    @spec partition_messages(list, binary, integer) :: map
    def partition_messages(responses, topic, partition) do
      response = Enum.find(responses, &(&1.topic == topic)) || %Response{}
      Enum.find(response.partitions, &(&1.partition == partition))
    end
  end

  defmodule Message do
    @moduledoc false
    defstruct attributes: 0, crc: nil, offset: nil, key: nil, value: nil

    @type t :: %Message{
            attributes: integer,
            crc: integer,
            offset: integer,
            key: binary,
            value: binary
          }
  end

  @spec create_request(Request.t()) :: binary
  def create_request(fetch_request) do
    KafkaEx.Protocol.create_request(
      :fetch,
      fetch_request.correlation_id,
      fetch_request.client_id
    ) <>
      <<
        -1::32-signed,
        fetch_request.wait_time::32-signed,
        fetch_request.min_bytes::32-signed,
        1::32-signed,
        byte_size(fetch_request.topic)::16-signed,
        fetch_request.topic::binary,
        1::32-signed,
        fetch_request.partition::32-signed,
        fetch_request.offset::64,
        fetch_request.max_bytes::32
      >>
  end

  def parse_response(
        <<_correlation_id::32-signed, topics_size::32-signed, rest::binary>>
      ) do
    parse_topics(topics_size, rest, __MODULE__)
  end

  def parse_partitions(0, rest, partitions), do: {partitions, rest}

  def parse_partitions(
        partitions_size,
        <<partition::32-signed, error_code::16-signed,
          hw_mark_offset::64-signed, msg_set_size::32-signed,
          msg_set_data::size(msg_set_size)-binary, rest::binary>>,
        partitions
      ) do
    {:ok, message_set, last_offset} = parse_message_set([], msg_set_data)

    parse_partitions(partitions_size - 1, rest, [
      %{
        partition: partition,
        error_code: Protocol.error(error_code),
        hw_mark_offset: hw_mark_offset,
        message_set: message_set,
        last_offset: last_offset
      }
      | partitions
    ])
  end

  defp parse_message_set([], <<>>) do
    {:ok, [], nil}
  end

  defp parse_message_set(
         list,
         <<offset::64, msg_size::32, msg_data::size(msg_size)-binary,
           rest::binary>>
       ) do
    {:ok, message} = parse_message(%Message{offset: offset}, msg_data)
    parse_message_set(append_messages(message, list), rest)
  end

  defp parse_message_set([last | _] = list, _) do
    {:ok, Enum.reverse(list), last.offset}
  end

  defp parse_message_set(
         _,
         <<offset::64, msg_size::32, partial_message_data::binary>>
       )
       when byte_size(partial_message_data) < msg_size do
    raise RuntimeError,
          "Insufficient data fetched at offset #{offset}. Message size is #{
            msg_size
          } but only received #{byte_size(partial_message_data)} bytes. Try increasing max_bytes."
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

  defp parse_message(
         %Message{} = message,
         <<crc::32, _magic::8, attributes::8, rest::binary>>
       ) do
    maybe_decompress(%{message | crc: crc, attributes: attributes}, rest)
  end

  defp maybe_decompress(%Message{attributes: 0} = message, rest) do
    parse_key(message, rest)
  end

  defp maybe_decompress(%Message{attributes: attributes}, rest) do
    <<-1::32-signed, value_size::32, value::size(value_size)-binary>> = rest
    decompressed = Compression.decompress(attributes, value)
    {:ok, msg_set, _offset} = parse_message_set([], decompressed)
    {:ok, msg_set}
  end

  defp parse_key(%Message{} = message, <<-1::32-signed, rest::binary>>) do
    parse_value(%{message | key: nil}, rest)
  end

  defp parse_key(
         %Message{} = message,
         <<key_size::32, key::size(key_size)-binary, rest::binary>>
       ) do
    parse_value(%{message | key: key}, rest)
  end

  defp parse_value(%Message{} = message, <<-1::32-signed>>) do
    {:ok, %{message | value: nil}}
  end

  defp parse_value(
         %Message{} = message,
         <<value_size::32, value::size(value_size)-binary>>
       ) do
    {:ok, %{message | value: value}}
  end
end
