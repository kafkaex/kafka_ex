defmodule KafkaEx.New.Structs.Produce do
  @moduledoc """
  This module represents a Produce response from Kafka.

  The response includes the base offset assigned to the first message in the
  produced batch, along with additional metadata that varies by API version.

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
  Builds a Produce struct from response data.
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
end
