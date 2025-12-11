defimpl KafkaEx.New.Protocols.Kayrock.JoinGroup.Response,
  for: Kayrock.JoinGroup.V0.Response do
  @moduledoc """
  Implementation for JoinGroup v0 Response.

  V0 does not include throttle_time_ms.
  """

  alias Kayrock.ErrorCode
  alias KafkaEx.New.Client.Error
  alias KafkaEx.New.Kafka.JoinGroup

  def parse_response(%Kayrock.JoinGroup.V0.Response{
        error_code: error_code,
        generation_id: generation_id,
        group_protocol: group_protocol,
        leader_id: leader_id,
        member_id: member_id,
        members: members
      }) do
    case ErrorCode.code_to_atom(error_code) do
      :no_error ->
        parsed_members = extract_members(members)

        {:ok,
         JoinGroup.build(
           throttle_time_ms: nil,
           generation_id: generation_id,
           group_protocol: group_protocol,
           leader_id: leader_id,
           member_id: member_id,
           members: parsed_members
         )}

      error_atom ->
        {:error, Error.build(error_atom, %{})}
    end
  end

  defp extract_members(kayrock_members) when is_list(kayrock_members) do
    Enum.map(kayrock_members, fn member ->
      %JoinGroup.Member{
        member_id: member.member_id,
        member_metadata: member.member_metadata
      }
    end)
  end

  defp extract_members(_), do: []
end
