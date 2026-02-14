defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Response, for: Kayrock.Heartbeat.V3.Response do
  @moduledoc """
  Implementation for Heartbeat v3 Response.

  V3 response schema is identical to V1/V2 (throttle_time_ms + error_code).
  The V3 request adds group_instance_id (KIP-345), but the response is unchanged.
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
