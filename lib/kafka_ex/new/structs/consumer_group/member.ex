defmodule KafkaEx.New.Structs.ConsumerGroup.Member do
  @moduledoc """
  Encapsulates what we know about a consumer group member
  """
  alias KafkaEx.New.Structs.ConsumerGroup.Member.MemberAssignment

  @type t :: %__MODULE__{
          member_id: binary,
          client_id: binary,
          client_host: binary,
          member_metadata: term,
          member_assignment: MemberAssignment.t() | nil
        }

  defstruct ~w(member_id client_id client_host member_metadata member_assignment)a

  @type partial_response :: %{
          required(:member_id) => binary,
          required(:client_id) => binary,
          required(:client_host) => binary,
          required(:member_metadata) => term,
          optional(:member_assignment) => map | nil
        }

  @spec from_describe_group_response(partial_response()) :: __MODULE__.t()
  def from_describe_group_response(response) do
    %__MODULE__{
      member_id: response.member_id,
      client_id: response.client_id,
      client_host: response.client_host,
      member_metadata: response.member_metadata,
      member_assignment: build_member_assignment(Map.get(response, :member_assignment))
    }
  end

  defp build_member_assignment(nil), do: nil

  defp build_member_assignment(member_assignment) do
    MemberAssignment.from_describe_group_response(member_assignment)
  end
end
