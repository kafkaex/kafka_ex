defmodule KafkaEx.Protocol.Common do
  @moduledoc """
  A collection of common request generation and response parsing functions for the
  Kafka wire protocol.
  """

  @doc """
  Generate the wire representation for a list of topics.
  """
  def topic_data([]), do: ""

  def topic_data([topic|topics]) do
    << byte_size(topic) :: 16-signed, topic :: binary >> <> topic_data(topics)
  end

  def parse_topics(0, _, _), do: []
  def parse_topics(topics_size, << topic_size :: 16-signed, topic :: size(topic_size)-binary, partitions_size :: 32-signed, rest :: binary >>, mod) do
    struct_module = Module.concat(mod, Response)
    {partitions, topics_data} = mod.parse_partitions(partitions_size, rest, [])
    [
      %{
        __struct__: struct_module,
        topic: topic, partitions: partitions
      } | parse_topics(topics_size - 1, topics_data, mod)
    ]
  end
end
