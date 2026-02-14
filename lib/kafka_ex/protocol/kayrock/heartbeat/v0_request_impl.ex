defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Request, for: Kayrock.Heartbeat.V0.Request do
  @moduledoc """
  Implementation for Heartbeat v0 Request.
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
