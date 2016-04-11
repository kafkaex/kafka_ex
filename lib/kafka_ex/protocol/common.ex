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

end
