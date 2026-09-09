defmodule KafkaEx.Cluster.Topic do
  @moduledoc """
  Encapsulates what we know about a topic
  """

  alias KafkaEx.Cluster.PartitionInfo

  defstruct name: nil,
            partition_leaders: %{},
            is_internal: false,
            partitions: []

  @type t :: %__MODULE__{
          name: String.t(),
          partition_leaders: %{integer() => integer()},
          is_internal: boolean(),
          partitions: [PartitionInfo.t()]
        }

  @doc false
  def from_topic_metadata(%{
        name: name,
        partitions: partition_metadata,
        is_internal: is_internal
      }) do
    partitions = Enum.map(partition_metadata, &PartitionInfo.from_partition_metadata/1)

    # Keep leaderless partitions (leader -1); dropping them shrinks the count the partitioner keys off.
    partition_leaders =
      Enum.into(partitions, %{}, fn %PartitionInfo{partition_id: id, leader: leader} ->
        {id, leader}
      end)

    %__MODULE__{
      name: name,
      partition_leaders: partition_leaders,
      is_internal: is_internal,
      partitions: partitions
    }
  end
end
