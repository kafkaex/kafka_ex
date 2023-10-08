defmodule KafkaEx.Protocol.Common do
  @moduledoc """
  A collection of common request generation and response parsing functions for the
  Kafka wire protocol.
  """

  @doc """
  Generate the wire representation for a list of topics.
  """
  def topic_data([]), do: []

  def topic_data([topic | topics]) do
    [<<byte_size(topic)::16-signed, topic::binary>> | topic_data(topics)]
  end

  def parse_topics(0, _, _), do: []

  def parse_topics(
        topics_size,
        <<topic_size::16-signed, topic::size(topic_size)-binary, partitions_size::32-signed,
          rest::binary>>,
        mod
      ) do
    struct_module = Module.concat(mod, Response)

    {partitions, topics_data} = mod.parse_partitions(partitions_size, rest, [], topic)

    [
      %{
        __struct__: struct_module,
        topic: topic,
        partitions: partitions
      }
      | parse_topics(topics_size - 1, topics_data, mod)
    ]
  end

  def read_array(0, data_after_array, _read_one) do
    {[], data_after_array}
  end

  def read_array(num_items, data, read_one) do
    {item, rest} = read_one.(data)
    {items, data_after_array} = read_array(num_items - 1, rest, read_one)
    {[item | items], data_after_array}
  end

  @spec encode_nullable_string(String.t()) :: binary
  def encode_nullable_string(text) do
    case text do
      nil -> <<-1::16-signed>>
      _ -> encode_string(text)
    end
  end

  @spec encode_string(String.t()) :: binary
  def encode_string(text) do
    <<byte_size(text)::16-signed, text::binary>>
  end

  def map_encode(elems, function) do
    if nil == elems or [] == elems do
      <<0::32-signed>>
    else
      <<length(elems)::32-signed>> <>
        (elems
         |> Enum.map(function)
         |> Enum.reduce(&(&1 <> &2)))
    end
  end
end
