defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Request, for: Kayrock.LeaveGroup.V0.Request do
  @moduledoc """
  Implementation for LeaveGroup v0 Request.
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
