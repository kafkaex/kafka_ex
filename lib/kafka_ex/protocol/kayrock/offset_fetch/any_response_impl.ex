defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Response, for: Any do
  @moduledoc """
  Fallback implementation of OffsetFetch Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V6) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detects the version range by checking for distinguishing response fields:
  - Has top-level `error_code`: V2+ path (also handles V5+ leader_epoch dynamically)
  - Otherwise: V0/V1 path (no top-level error_code)
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(%{error_code: _} = response) do
    ResponseHelpers.parse_response_with_top_level_error(response)
  end

  def parse_response(response) do
    ResponseHelpers.parse_response_without_top_level_error(response)
  end
end
