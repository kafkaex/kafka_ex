defmodule KafkaEx.New.Kafka.Offset.PartitionOffset do
  @moduledoc """
  This module represents Offset value for a specific partition.

  Used for three Kafka offset-related APIs:
  - **ListOffsets API** (log position queries) - uses `timestamp` field, always has `offset`
  - **OffsetFetch API** (committed consumer group offsets) - uses `metadata` field, always has `offset`
  - **OffsetCommit API** (commit operation results) - only has `error_code`, `offset` is nil
  """
  defstruct [:partition, :offset, :error_code, :timestamp, :metadata]

  @type partition :: KafkaEx.Types.partition()
  @type offset :: KafkaEx.Types.offset() | nil
  @type timestamp :: KafkaEx.Types.timestamp() | nil
  @type metadata :: binary() | nil
  @type error_code :: KafkaEx.Types.error_code() | atom

  @type partition_response :: %{
          required(:partition) => partition,
          required(:error_code) => error_code,
          optional(:offset) => offset,
          optional(:timestamp) => timestamp,
          optional(:metadata) => metadata
        }
  @type t :: %__MODULE__{
          partition: partition,
          error_code: error_code,
          offset: offset,
          timestamp: timestamp,
          metadata: metadata
        }

  @doc """
  Builds a PartitionOffset struct from response data.

  Supports three different Kafka APIs:

  **ListOffsets API** (log position queries):
  - Includes `timestamp` when the offset was written
  - For older API versions, timestamp defaults to -1
  - Always includes `offset`

  **OffsetFetch API** (committed consumer group offsets):
  - Includes `metadata` (consumer-supplied string)
  - Metadata can be empty string or nil
  - Always includes `offset`

  **OffsetCommit API** (commit operation results):
  - Only includes `partition` and `error_code`
  - No offset (you already know what you committed)
  - No metadata or timestamp
  """
  @spec build(partition_response) :: __MODULE__.t()
  def build(%{partition: p, offset: o, error_code: e, timestamp: t, metadata: m}), do: do_build(p, o, e, t, m)
  def build(%{partition: p, offset: o, error_code: e, timestamp: t}), do: do_build(p, o, e, t, nil)
  def build(%{partition: p, offset: o, error_code: e, metadata: m}), do: do_build(p, o, e, nil, m)
  def build(%{partition: p, offset: o, error_code: e}) when is_integer(o), do: do_build(p, o, e, -1, nil)
  def build(%{partition: p, offset: o}) when is_integer(o), do: do_build(p, o, :no_error, -1, nil)
  def build(%{partition: p, error_code: e}) when is_atom(e), do: do_build(p, nil, e, nil, nil)

  defp do_build(partition, offset, error_code, timestamp, metadata) do
    %__MODULE__{
      partition: partition,
      error_code: error_code,
      offset: offset,
      timestamp: timestamp,
      metadata: metadata
    }
  end
end
