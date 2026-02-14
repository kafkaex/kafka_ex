defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Response, for: Kayrock.JoinGroup.V1.Response do
  @moduledoc """
  Implementation for JoinGroup V1 Response.

  V1 adds rebalance_timeout (request-side only, response same as V0).
  Does not include throttle_time_ms.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v0_or_v1_response(response)
  end
end
