defmodule KafkaEx.New.Structs.ConsumerGroup.Member.MemberAssignment.PartitionAssignment do
  @moduledoc """
  Encapsulates what we know about a consumer group member partition assignment.
  Will only be present if the group is stable and is assigned to given topic.
  """

  @type t :: %__MODULE__{
          topic: binary,
          partitions: list(non_neg_integer)
        }

  defstruct ~w(topic partitions)a

  @type response_partial :: %{
          required(:topic) => binary,
          required(:partitions) => list(non_neg_integer)
        }

  @spec from_describe_group_response(response_partial()) :: __MODULE__.t()
  def from_describe_group_response(response) do
    %__MODULE__{
      topic: response.topic,
      partitions: response.partitions
    }
  end
end
