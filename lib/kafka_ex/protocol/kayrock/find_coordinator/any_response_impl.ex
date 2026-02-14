defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Response, for: Any do
  @moduledoc """
  Fallback implementation of FindCoordinator Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V3) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `throttle_time_ms`: V1+ path (includes throttle_time_ms and error_message)
  - Otherwise: V0 path (no throttle_time_ms, no error_message)
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseHelpers

  def parse_response(%{throttle_time_ms: _} = response) do
    ResponseHelpers.parse_v1_response(response)
  end

  def parse_response(response) do
    ResponseHelpers.parse_v0_response(response)
  end
end
