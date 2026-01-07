defmodule KafkaEx.Messages.ConsumerGroupDescription.Member do
  @moduledoc """
  Information about a member of a consumer group.

  Java equivalent: `org.apache.kafka.clients.admin.MemberDescription`

  Contains identifying information about a consumer group member and their
  current partition assignment.
  """
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment

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

  @doc """
  Builds a Member from a DescribeGroups API response.
  """
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

  @doc """
  Returns the member ID.
  """
  @spec member_id(t()) :: binary
  def member_id(%__MODULE__{member_id: member_id}), do: member_id

  @doc """
  Returns the client ID.
  """
  @spec client_id(t()) :: binary
  def client_id(%__MODULE__{client_id: client_id}), do: client_id

  @doc """
  Returns the client host.
  """
  @spec client_host(t()) :: binary
  def client_host(%__MODULE__{client_host: client_host}), do: client_host

  @doc """
  Returns the member's current partition assignment.
  """
  @spec assignment(t()) :: MemberAssignment.t() | nil
  def assignment(%__MODULE__{member_assignment: assignment}), do: assignment

  defp build_member_assignment(nil), do: nil

  defp build_member_assignment(member_assignment) do
    MemberAssignment.from_describe_group_response(member_assignment)
  end
end
