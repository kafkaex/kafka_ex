defmodule KafkaEx.Messages.RecordMetadata do
  @moduledoc """
  Metadata about a record that has been acknowledged by the server.

  This struct represents the response from a produce operation, containing
  information about where the record was stored in Kafka.

  Java equivalent: `org.apache.kafka.clients.producer.RecordMetadata`

  ## Fields

    * `:topic` - The topic the messages were produced to
    * `:partition` - The partition the messages were produced to
    * `:base_offset` - The offset assigned to the first message in the batch
    * `:log_append_time` - The timestamp assigned by the broker (v2+, -1 if not available)
    * `:log_start_offset` - The start offset of the log (v5+)
    * `:throttle_time_ms` - Time in ms the request was throttled (v3+)
  """

  defstruct [
    :topic,
    :partition,
    :base_offset,
    :log_append_time,
    :log_start_offset,
    :throttle_time_ms
  ]

  @type t :: %__MODULE__{
          topic: String.t(),
          partition: non_neg_integer(),
          base_offset: non_neg_integer(),
          log_append_time: integer() | nil,
          log_start_offset: non_neg_integer() | nil,
          throttle_time_ms: non_neg_integer() | nil
        }

  @doc """
  Builds a RecordMetadata struct from response data.

  ## Options

    * `:topic` - (required) The topic name
    * `:partition` - (required) The partition number
    * `:base_offset` - (required) The base offset assigned to the batch
    * `:log_append_time` - The broker-assigned timestamp (-1 if not using LogAppendTime)
    * `:log_start_offset` - The log start offset (v5+)
    * `:throttle_time_ms` - Request throttle time in milliseconds (v3+)
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      topic: Keyword.fetch!(opts, :topic),
      partition: Keyword.fetch!(opts, :partition),
      base_offset: Keyword.fetch!(opts, :base_offset),
      log_append_time: Keyword.get(opts, :log_append_time),
      log_start_offset: Keyword.get(opts, :log_start_offset),
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms)
    }
  end

  @doc """
  Returns the offset of the record in the topic/partition.

  This is an alias for `base_offset` to match Java API.
  """
  @spec offset(t()) :: non_neg_integer()
  def offset(%__MODULE__{base_offset: offset}), do: offset

  @doc """
  Returns the timestamp of the record.

  For records using CreateTime, this is the timestamp provided by the producer.
  For records using LogAppendTime, this is the broker-assigned timestamp.
  Returns nil if timestamp is not available or -1.
  """
  @spec timestamp(t()) :: integer() | nil
  def timestamp(%__MODULE__{log_append_time: -1}), do: nil
  def timestamp(%__MODULE__{log_append_time: ts}), do: ts
end
