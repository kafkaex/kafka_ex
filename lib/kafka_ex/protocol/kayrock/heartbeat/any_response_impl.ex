defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Response, for: Any do
  @moduledoc """
  Fallback implementation of Heartbeat Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V4) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `throttle_time_ms` key: V1+ path (extracts throttle_time_ms)
  - Otherwise: V0 path (returns {:ok, :no_error} on success)
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.ResponseHelpers

  def parse_response(response) do
    if Map.has_key?(response, :throttle_time_ms) do
      ResponseHelpers.parse_v1_plus_response(response)
    else
      ResponseHelpers.parse_v0_response(response)
    end
  end
end
