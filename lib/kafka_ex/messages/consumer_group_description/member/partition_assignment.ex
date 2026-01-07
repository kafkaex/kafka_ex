defmodule KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment do
  @moduledoc """
  A topic-partition assignment for a consumer group member.

  Contains the topic name and list of partition IDs assigned to a member.
  Will only be present if the group is stable.
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

  @doc """
  Builds a PartitionAssignment from a DescribeGroups API response.
  """
  @spec from_describe_group_response(response_partial()) :: __MODULE__.t()
  def from_describe_group_response(response) do
    %__MODULE__{
      topic: response.topic,
      partitions: response.partitions
    }
  end

  @doc """
  Returns the topic name.
  """
  @spec topic(t()) :: binary
  def topic(%__MODULE__{topic: topic}), do: topic

  @doc """
  Returns the list of assigned partition IDs.
  """
  @spec partitions(t()) :: [non_neg_integer]
  def partitions(%__MODULE__{partitions: partitions}), do: partitions
end
