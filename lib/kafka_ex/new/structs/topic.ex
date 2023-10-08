defmodule KafkaEx.New.Structs.Topic do
  @moduledoc """
  Encapsulates what we know about a topic
  """

  alias KafkaEx.New.Structs.Partition

  defstruct name: nil,
            partition_leaders: %{},
            is_internal: false,
            partitions: []

  @type t :: %__MODULE__{
          name: String.t(),
          partition_leaders: %{integer() => integer()},
          is_internal: boolean(),
          partitions: [Partition.t()]
        }

  @doc false
  def from_topic_metadata(%{
        topic: name,
        partition_metadata: partition_metadata,
        is_internal: is_internal
      }) do
    partition_leaders =
      Enum.into(
        partition_metadata,
        %{},
        fn %{error_code: 0, leader: leader, partition: partition_id} ->
          {partition_id, leader}
        end
      )

    partitions = Enum.map(partition_metadata, &Partition.from_partition_metadata/1)

    %__MODULE__{
      name: name,
      partition_leaders: partition_leaders,
      is_internal: is_internal,
      partitions: partitions
    }
  end
end
