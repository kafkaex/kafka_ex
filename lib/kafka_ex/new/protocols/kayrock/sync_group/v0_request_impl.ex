defimpl KafkaEx.New.Protocols.Kayrock.SyncGroup.Request, for: Kayrock.SyncGroup.V0.Request do
  @moduledoc """
  Implementation for SyncGroup v0 Request.
  """

  alias KafkaEx.New.Protocols.Kayrock.SyncGroup.RequestHelpers

  def build_request(request_template, opts) do
    %{
      group_id: group_id,
      generation_id: generation_id,
      member_id: member_id,
      group_assignment: group_assignment
    } = RequestHelpers.extract_common_fields(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:generation_id, generation_id)
    |> Map.put(:member_id, member_id)
    |> Map.put(:group_assignment, group_assignment)
  end
end
