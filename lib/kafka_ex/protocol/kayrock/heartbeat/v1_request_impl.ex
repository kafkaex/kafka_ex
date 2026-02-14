defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Request, for: Kayrock.Heartbeat.V1.Request do
  @moduledoc """
  Implementation for Heartbeat v1 Request.

  V1 request schema is identical to V0.
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
