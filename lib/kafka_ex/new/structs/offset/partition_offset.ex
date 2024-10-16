defmodule KafkaEx.New.Structs.Offset.PartitionOffset do
  @moduledoc """
  This module represents Offset value for a specific partition
  """
  defstruct [:partition, :offset, :error_code, :timestamp]

  @type partition :: KafkaEx.Types.partition()
  @type offset :: KafkaEx.Types.offset()
  @type timestamp :: KafkaEx.Types.timestamp()
  @type error_code :: KafkaEx.Types.error_code() | atom

  @type partition_response :: %{
          required(:partition) => partition,
          required(:error_code) => error_code,
          required(:offset) => offset,
          optional(:timestamp) => timestamp
        }
  @type t :: %__MODULE__{
          partition: partition,
          error_code: error_code,
          offset: offset,
          timestamp: timestamp
        }

  @doc """
  For older API versions, kafka is not returning offset timestamp.
  For backward compatibility with kafka_ex, we will replace this nil values with -1
  """
  @spec build(partition_response) :: __MODULE__.t()
  def build(%{partition: p, offset: o, error_code: e, timestamp: t}), do: do_build(p, o, e, t)
  def build(%{partition: p, offset: o, error_code: e}), do: do_build(p, o, e, -1)
  def build(%{partition: p, offset: o}), do: do_build(p, o, :no_error, -1)

  defp do_build(partition, offset, error_code, timestamp) do
    %__MODULE__{
      partition: partition,
      error_code: error_code,
      offset: offset,
      timestamp: timestamp
    }
  end
end
