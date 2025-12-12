defmodule KafkaEx.New.Kafka.ConsumerGroupDescription do
  @moduledoc """
  A detailed description of a single consumer group in the cluster.

  Java equivalent: `org.apache.kafka.clients.admin.ConsumerGroupDescription`

  Contains information about a consumer group's state, protocol, and members as returned by the DescribeGroups API.
  """

  alias KafkaEx.New.Kafka.ConsumerGroupDescription.Member

  @type t :: %__MODULE__{
          group_id: binary,
          state: binary,
          protocol_type: binary,
          protocol: binary,
          members: list(Member.t())
        }

  defstruct ~w(group_id state protocol_type protocol members)a

  @doc """
  Builds a ConsumerGroupDescription from a DescribeGroups API response.
  """
  @spec from_describe_group_response(map) :: __MODULE__.t()
  def from_describe_group_response(describe_group) do
    %__MODULE__{
      group_id: describe_group.group_id,
      state: describe_group.state,
      protocol_type: describe_group.protocol_type,
      protocol: describe_group.protocol,
      members: Enum.map(describe_group.members, &build_consumer_group_member/1)
    }
  end

  @doc """
  Returns the group ID.
  """
  @spec group_id(t()) :: binary
  def group_id(%__MODULE__{group_id: group_id}), do: group_id

  @doc """
  Returns the current state of the group.
  """
  @spec state(t()) :: binary
  def state(%__MODULE__{state: state}), do: state

  @doc """
  Returns the list of members in the group.
  """
  @spec members(t()) :: [Member.t()]
  def members(%__MODULE__{members: members}), do: members

  @doc """
  Returns whether the group is in the Stable state.
  """
  @spec stable?(t()) :: boolean
  def stable?(%__MODULE__{state: state}), do: state == "Stable"

  defp build_consumer_group_member(group_member) do
    Member.from_describe_group_response(group_member)
  end
end
