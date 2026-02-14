defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Request, for: Kayrock.JoinGroup.V3.Request do
  @moduledoc """
  Implementation for JoinGroup V3 Request.

  V3 has the same request structure as V2 (includes rebalance_timeout).
  Pure version bump — no schema changes.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v1_or_v2_request(request_template, opts)
  end
end
