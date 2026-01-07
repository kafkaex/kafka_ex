defmodule KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment do
  @moduledoc """
  The current assignment for a consumer group member.

  Java equivalent: `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment`

  Contains the partition assignments and user data for a member.
  Will only be present if the group is stable.
  """

  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment

  @type t :: %__MODULE__{
          version: non_neg_integer,
          user_data: binary,
          partition_assignments: [PartitionAssignment.t()]
        }

  defstruct ~w(version partition_assignments user_data)a

  @type response_partial :: %{
          required(:version) => non_neg_integer,
          required(:partition_assignments) => [
            PartitionAssignment.response_partial()
          ],
          required(:user_data) => binary
        }

  @doc """
  Builds a MemberAssignment from a DescribeGroups API response.
  """
  @spec from_describe_group_response(response_partial()) :: __MODULE__.t()
  def from_describe_group_response(response) do
    %__MODULE__{
      version: response.version,
      user_data: response.user_data,
      partition_assignments:
        Enum.map(response.partition_assignments, fn assignment ->
          PartitionAssignment.from_describe_group_response(assignment)
        end)
    }
  end

  @doc """
  Returns the assignment version.
  """
  @spec version(t()) :: non_neg_integer
  def version(%__MODULE__{version: version}), do: version

  @doc """
  Returns the partition assignments.
  """
  @spec partition_assignments(t()) :: [PartitionAssignment.t()]
  def partition_assignments(%__MODULE__{partition_assignments: assignments}), do: assignments

  @doc """
  Returns the user data.
  """
  @spec user_data(t()) :: binary
  def user_data(%__MODULE__{user_data: user_data}), do: user_data
end
