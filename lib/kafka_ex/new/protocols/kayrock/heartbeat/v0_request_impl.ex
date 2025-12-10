defimpl KafkaEx.New.Protocols.Kayrock.Heartbeat.Request, for: Kayrock.Heartbeat.V0.Request do
  @moduledoc """
  Implementation for Heartbeat v0 Request.
  """

  alias KafkaEx.New.Protocols.Kayrock.Heartbeat.RequestHelpers

  def build_request(request_template, opts) do
    %{group_id: group_id, member_id: member_id, generation_id: generation_id} =
      RequestHelpers.extract_common_fields(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:member_id, member_id)
    |> Map.put(:generation_id, generation_id)
  end
end
