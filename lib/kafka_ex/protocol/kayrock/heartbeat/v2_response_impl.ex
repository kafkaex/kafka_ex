defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Response, for: Kayrock.Heartbeat.V2.Response do
  @moduledoc """
  Implementation for Heartbeat v2 Response.

  V2 response schema is identical to V1 (throttle_time_ms + error_code).
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
