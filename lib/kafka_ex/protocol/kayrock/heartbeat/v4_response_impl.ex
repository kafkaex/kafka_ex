defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Response, for: Kayrock.Heartbeat.V4.Response do
  @moduledoc """
  Implementation for Heartbeat v4 Response.

  V4 is the flexible version (KIP-482) with compact encoding and tagged_fields.
  Domain-relevant fields (throttle_time_ms, error_code) are identical to V1-V3.
  Kayrock handles compact decoding transparently.
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
