defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Response, for: Kayrock.JoinGroup.V3.Response do
  @moduledoc """
  Implementation for JoinGroup V3 Response.

  V3 has the same response structure as V2 (includes throttle_time_ms).
  Pure version bump — no schema changes.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v2_plus_response(response)
  end
end
