defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Response, for: Kayrock.JoinGroup.V4.Response do
  @moduledoc """
  Implementation for JoinGroup V4 Response.

  V4 has the same response structure as V3 (includes throttle_time_ms).
  Pure version bump — no schema changes.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v2_plus_response(response)
  end
end
