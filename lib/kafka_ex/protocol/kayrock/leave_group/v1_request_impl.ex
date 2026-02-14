defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Request, for: Kayrock.LeaveGroup.V1.Request do
  @moduledoc """
  Implementation for LeaveGroup v1 Request.

  V1 request schema is identical to V0 (pure version bump).
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
