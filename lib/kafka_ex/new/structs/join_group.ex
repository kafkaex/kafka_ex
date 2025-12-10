defmodule KafkaEx.New.Structs.JoinGroup do
  @moduledoc """
  This module represents JoinGroup response from Kafka.

  The response includes the generation ID, protocol, leader, member ID,
  and the list of all group members for leader assignment.
  """

  defmodule Member do
    @moduledoc """
    Represents a member in the JoinGroup response.

    The `member_metadata` field contains the serialized protocol metadata
    (as bytes) that was sent by this member in their join request.
    For consumer groups, this is typically a serialized GroupProtocolMetadata
    struct containing the list of topics the member is interested in.
    """

    defstruct [:member_id, :member_metadata]

    @type t :: %__MODULE__{
            member_id: binary(),
            member_metadata: bitstring()
          }
  end

  defstruct [
    :throttle_time_ms,
    :generation_id,
    :group_protocol,
    :leader_id,
    :member_id,
    :members
  ]

  @type t :: %__MODULE__{
          throttle_time_ms: nil | non_neg_integer(),
          generation_id: non_neg_integer(),
          group_protocol: binary(),
          leader_id: binary(),
          member_id: binary(),
          members: [Member.t()]
        }

  @doc """
  Builds a JoinGroup struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms),
      generation_id: Keyword.fetch!(opts, :generation_id),
      group_protocol: Keyword.fetch!(opts, :group_protocol),
      leader_id: Keyword.fetch!(opts, :leader_id),
      member_id: Keyword.fetch!(opts, :member_id),
      members: Keyword.get(opts, :members, [])
    }
  end

  @doc """
  Returns true if the member is the group leader.
  """
  @spec leader?(t()) :: boolean()
  def leader?(%__MODULE__{member_id: member_id, leader_id: leader_id}) do
    member_id == leader_id
  end
end
