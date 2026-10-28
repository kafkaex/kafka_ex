defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V7.Response do
  @moduledoc """
  Implementation for OffsetFetch V7 Response.

  Same response structure as V6 (top-level `error_code`, per-partition
  `committed_leader_epoch`). V7 may return `UNSTABLE_OFFSET_COMMIT` for a
  partition with a pending transactional commit.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response_with_top_level_error(response)
  end
end
