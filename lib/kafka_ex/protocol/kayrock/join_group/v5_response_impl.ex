defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Response, for: Kayrock.JoinGroup.V5.Response do
  @moduledoc """
  Implementation for JoinGroup V5 Response.

  V5 response includes `throttle_time_ms` (same as V2+) and adds
  `group_instance_id` per member for static membership (KIP-345).

  Note: `group_instance_id` in members is NOT extracted to the domain layer
  — the existing `extract_members/1` helper only maps `member_id` and `metadata`,
  which is sufficient for current usage. The `group_instance_id` from V5+
  responses is silently ignored (same pattern as `leader_epoch` in other APIs).
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v2_plus_response(response)
  end
end
