defmodule KafkaEx.Cluster.Topic do
  @moduledoc """
  Encapsulates what we know about a topic
  """

  alias KafkaEx.Cluster.PartitionInfo

  defstruct name: nil,
            partition_leaders: %{},
            is_internal: false,
            partitions: [],
            topic_id: nil

  @typedoc """
  Kafka topic id: the raw 16-byte UUID as sent on the wire (Metadata v10+).
  `nil` when the broker did not report one (older Metadata versions, or the
  all-zero "null" UUID).
  """
  @type topic_id :: <<_::128>>

  @type t :: %__MODULE__{
          name: String.t(),
          partition_leaders: %{integer() => integer()},
          is_internal: boolean(),
          partitions: [PartitionInfo.t()],
          topic_id: topic_id() | nil
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
