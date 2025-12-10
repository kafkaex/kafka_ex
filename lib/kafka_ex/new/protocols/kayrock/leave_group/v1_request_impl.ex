defimpl KafkaEx.New.Protocols.Kayrock.LeaveGroup.Request, for: Kayrock.LeaveGroup.V1.Request do
  @moduledoc """
  Implementation for LeaveGroup v1 Request.
  """

  alias KafkaEx.New.Protocols.Kayrock.LeaveGroup.RequestHelpers

  def build_request(request_template, opts) do
    %{group_id: group_id, member_id: member_id} = RequestHelpers.extract_common_fields(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:member_id, member_id)
  end
end
