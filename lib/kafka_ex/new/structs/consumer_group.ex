defmodule KafkaEx.New.Structs.ConsumerGroup do
  @moduledoc """
  Encapsulates what we know about consumer group
  """

  alias KafkaEx.New.Structs.ConsumerGroup.Member

  @type t :: %__MODULE__{
          group_id: binary,
          state: binary,
          protocol_type: binary,
          protocol: binary,
          members: list(Member.t())
        }

  defstruct ~w(group_id state protocol_type protocol members)a

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

  defp build_consumer_group_member(group_member) do
    Member.from_describe_group_response(group_member)
  end
end
