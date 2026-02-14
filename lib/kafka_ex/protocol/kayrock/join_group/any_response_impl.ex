defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Response, for: Any do
  @moduledoc """
  Fallback implementation of JoinGroup Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V6) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `throttle_time_ms` key: V2+ path (extracts throttle_time_ms)
  - Otherwise: V0/V1 path (nil throttle_time_ms)

  Note: `group_instance_id` in V5+ members is NOT extracted — the domain
  layer does not use it yet. The existing `extract_members/1` helper only
  maps `member_id` and `metadata`, which works for all versions.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.ResponseHelpers

  def parse_response(response) do
    if Map.has_key?(response, :throttle_time_ms) do
      ResponseHelpers.parse_v2_plus_response(response)
    else
      ResponseHelpers.parse_v0_or_v1_response(response)
    end
  end
end
