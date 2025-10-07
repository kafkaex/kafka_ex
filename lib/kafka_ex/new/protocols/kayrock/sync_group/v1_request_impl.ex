defimpl KafkaEx.New.Protocols.Kayrock.SyncGroup.Request, for: Kayrock.SyncGroup.V1.Request do
  @moduledoc """
  Implementation for SyncGroup v1 Request.

  V1 request has the same structure as v0 - the difference is in the response
  which includes throttle_time_ms.
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
