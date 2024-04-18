defmodule KafkaEx.New.Structs.Offset.PartitionOffset do
  @moduledoc """
  This module represents Offset value for a specific partition
  """
  defstruct [:partition, :offset, :timestamp]

  @type partition :: KafkaEx.Types.partition()
  @type offset :: KafkaEx.Types.offset()
  @type timestamp :: KafkaEx.Types.timestamp()

  @type partition_response :: %{
          required(:partition) => partition,
          required(:offset) => offset,
          optional(:timestamp) => timestamp
        }
  @type t :: %__MODULE__{
          partition: partition,
          offset: offset,
          timestamp: timestamp
        }

  @doc """
  For older API versions, kafka is not returning offset timestamp.
  For backward compatibility with kafka_ex, we will replace this nil values with -1
  """
  @spec build(partition_response) :: __MODULE__.t()
  def build(%{partition: p, offset: o, timestamp: t}), do: do_build(p, o, t)
  def build(%{partition: p, offset: o}), do: do_build(p, o, -1)

  defp do_build(partition, offset, timestamp) do
    %__MODULE__{
      partition: partition,
      offset: offset,
      timestamp: timestamp
    }
  end
end
