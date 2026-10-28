defmodule KafkaEx.Messages.ConsumerGroupDescription do
  @moduledoc """
  A detailed description of a single consumer group in the cluster.

  Java equivalent: `org.apache.kafka.clients.admin.ConsumerGroupDescription`

  Contains information about a consumer group's state, protocol, and members as returned by the DescribeGroups API
  (`KafkaEx.API.describe_group/3`) or, for groups using the KIP-848 "consumer" protocol, the ConsumerGroupDescribe
  API (`KafkaEx.API.describe_consumer_group/3`).

  ## Fields set by ConsumerGroupDescribe only

  These are `nil` in DescribeGroups results.

    * `type` - `:consumer`
    * `group_epoch` - the group epoch
    * `assignment_epoch` - the epoch of the group's current target assignment

  For ConsumerGroupDescribe results `protocol_type` is `"consumer"`, `protocol` is the name of the server-side
  assignor in use (e.g. `"uniform"`) and members carry the consumer-protocol fields described in
  `KafkaEx.Messages.ConsumerGroupDescription.Member`.
  """

  alias KafkaEx.Messages.ConsumerGroupDescription.Member

  @type t :: %__MODULE__{
          group_id: binary,
          state: binary,
          protocol_type: binary,
          protocol: binary,
          members: list(Member.t()),
          authorized_operations: integer() | nil,
          type: :consumer | nil,
          group_epoch: integer() | nil,
          assignment_epoch: integer() | nil
        }

  defstruct ~w(group_id state protocol_type protocol members authorized_operations type group_epoch assignment_epoch)a

  @doc """
  Builds a ConsumerGroupDescription from a DescribeGroups API response.
  """
  @spec from_describe_group_response(map) :: __MODULE__.t()
  def from_describe_group_response(describe_group) do
    %__MODULE__{
      group_id: describe_group.group_id,
      state: describe_group.group_state,
      protocol_type: describe_group.protocol_type,
      protocol: describe_group.protocol_data,
      members: Enum.map(describe_group.members, &build_consumer_group_member/1),
      authorized_operations: Map.get(describe_group, :authorized_operations)
    }
  end

  @doc """
  Builds a ConsumerGroupDescription from one group of a ConsumerGroupDescribe API response (KIP-848).
  """
  @spec from_consumer_group_describe_response(map) :: __MODULE__.t()
  def from_consumer_group_describe_response(described_group) do
    %__MODULE__{
      group_id: described_group.group_id,
      state: described_group.group_state,
      protocol_type: "consumer",
      protocol: described_group.assignor_name,
      members: Enum.map(described_group.members, &Member.from_consumer_group_describe_response/1),
      authorized_operations: Map.get(described_group, :authorized_operations),
      type: :consumer,
      group_epoch: described_group.group_epoch,
      assignment_epoch: described_group.assignment_epoch
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
