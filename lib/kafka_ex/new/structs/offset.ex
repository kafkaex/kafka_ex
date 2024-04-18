defmodule KafkaEx.New.Structs.Offset do
  @moduledoc """
  This module represents Offset value coming from Kafka
  """
  defstruct [:topic, :partition_offsets]

  alias KafkaEx.New.Structs.Offset.PartitionOffset

  @type topic :: KafkaEx.Types.topic()
  @type partition_response :: PartitionOffset.partition_response()

  @type t :: %__MODULE__{
          topic: topic(),
          partition_offsets: [PartitionOffset.t()]
        }

  @doc """
  Builds offset based on list offsets data response
  """
  @spec from_list_offset(topic, [partition_response]) :: __MODULE__.t()
  def from_list_offset(topic, partition_responses) do
    %__MODULE__{
      topic: topic,
      partition_offsets: Enum.map(partition_responses, &PartitionOffset.build(&1))
    }
  end
end
