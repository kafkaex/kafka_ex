defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Request, for: Kayrock.Heartbeat.V3.Request do
  @moduledoc """
  Implementation for Heartbeat v3 Request.

  V3 adds `group_instance_id` for static membership (KIP-345).
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v3_plus_request(request_template, opts)
  end
end
