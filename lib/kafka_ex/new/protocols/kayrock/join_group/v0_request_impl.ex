defimpl KafkaEx.New.Protocols.Kayrock.JoinGroup.Request, for: Kayrock.JoinGroup.V0.Request do
  @moduledoc """
  Implementation for JoinGroup v0 Request.

  V0 includes: group_id, session_timeout, member_id, protocol_type, group_protocols
  """

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v0_request(request_template, opts)
  end
end
