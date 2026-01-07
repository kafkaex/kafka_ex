defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Request, for: Kayrock.JoinGroup.V2.Request do
  @moduledoc """
  Implementation for JoinGroup v2 Request.

  V2 has the same request structure as V1 (includes rebalance_timeout).
  The difference is in the response which adds throttle_time_ms.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v1_or_v2_request(request_template, opts)
  end
end
