defimpl KafkaEx.New.Protocols.Kayrock.JoinGroup.Response, for: Kayrock.JoinGroup.V1.Response do
  @moduledoc """
  Implementation for JoinGroup V1 Response.

  V1 adds rebalance_timeout (request-side only, response same as V0).
  Does not include throttle_time_ms.
  """

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn _resp -> nil end)
  end
end
