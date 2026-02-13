defmodule KafkaEx.Messages.Offset.PartitionOffset do
  @moduledoc """
  This module represents Offset value for a specific partition.

  Used for three Kafka offset-related APIs:
  - **ListOffsets API** (log position queries) - uses `timestamp` field, always has `offset`
  - **OffsetFetch API** (committed consumer group offsets) - uses `metadata` field, always has `offset`
  - **OffsetCommit API** (commit operation results) - only has `error_code`, `offset` is nil
  """

  alias KafkaEx.Messages.OffsetAndMetadata

  defstruct [:partition, :offset, :error_code, :timestamp, :metadata, :leader_epoch]

  @type partition :: KafkaEx.Support.Types.partition()
  @type offset :: KafkaEx.Support.Types.offset() | nil
  @type timestamp :: KafkaEx.Support.Types.timestamp() | nil
  @type metadata :: binary() | nil
  @type error_code :: KafkaEx.Support.Types.error_code() | atom
  @type leader_epoch :: integer() | nil

  @type partition_response :: %{
          required(:partition) => partition,
          required(:error_code) => error_code,
          optional(:offset) => offset,
          optional(:timestamp) => timestamp,
          optional(:metadata) => metadata,
          optional(:leader_epoch) => leader_epoch
        }
  @type t :: %__MODULE__{
          partition: partition,
          error_code: error_code,
          offset: offset,
          timestamp: timestamp,
          metadata: metadata,
          leader_epoch: leader_epoch
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
  def build(%{partition: p, offset: o, error_code: e, timestamp: t, metadata: m, leader_epoch: le}),
    do: do_build(p, o, e, t, m, le)

  def build(%{partition: p, offset: o, error_code: e, timestamp: t, leader_epoch: le}),
    do: do_build(p, o, e, t, nil, le)

  def build(%{partition: p, offset: o, error_code: e, timestamp: t, metadata: m}), do: do_build(p, o, e, t, m, nil)
  def build(%{partition: p, offset: o, error_code: e, timestamp: t}), do: do_build(p, o, e, t, nil, nil)
  def build(%{partition: p, offset: o, error_code: e, metadata: m, leader_epoch: le}), do: do_build(p, o, e, nil, m, le)
  def build(%{partition: p, offset: o, error_code: e, metadata: m}), do: do_build(p, o, e, nil, m, nil)
  def build(%{partition: p, offset: o, error_code: e}) when is_integer(o), do: do_build(p, o, e, -1, nil, nil)
  def build(%{partition: p, offset: o}) when is_integer(o), do: do_build(p, o, :no_error, -1, nil, nil)
  def build(%{partition: p, error_code: e}) when is_atom(e), do: do_build(p, nil, e, nil, nil, nil)

  defp do_build(partition, offset, error_code, timestamp, metadata, leader_epoch) do
    %__MODULE__{
      partition: partition,
      error_code: error_code,
      offset: offset,
      timestamp: timestamp,
      metadata: metadata,
      leader_epoch: leader_epoch
    }
  end

  @doc """
  Converts this PartitionOffset to an OffsetAndMetadata struct.

  Returns `nil` if offset is nil (e.g., for OffsetCommit results that don't include offset).

  ## Examples

      iex> po = PartitionOffset.build(%{partition: 0, offset: 100, error_code: :no_error, metadata: "v1"})
      iex> PartitionOffset.to_offset_and_metadata(po)
      %OffsetAndMetadata{offset: 100, metadata: "v1", leader_epoch: nil}

  """
  @spec to_offset_and_metadata(t()) :: OffsetAndMetadata.t() | nil
  def to_offset_and_metadata(%__MODULE__{offset: nil}), do: nil

  def to_offset_and_metadata(%__MODULE__{offset: offset, metadata: metadata, leader_epoch: leader_epoch}) do
    OffsetAndMetadata.build(offset: offset, metadata: metadata || "", leader_epoch: leader_epoch)
  end
end
