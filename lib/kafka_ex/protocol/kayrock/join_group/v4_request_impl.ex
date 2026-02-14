defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Request, for: Kayrock.JoinGroup.V4.Request do
  @moduledoc """
  Implementation for JoinGroup V4 Request.

  V4 has the same request structure as V3 (includes rebalance_timeout).
  Pure version bump — no schema changes.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v1_or_v2_request(request_template, opts)
  end
end
