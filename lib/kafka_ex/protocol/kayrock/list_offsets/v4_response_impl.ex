defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Response, for: Kayrock.ListOffsets.V4.Response do
  @moduledoc """
  Implementation for ListOffsets V4 Response.

  V4 adds `leader_epoch` to each partition response.
  This indicates the epoch of the leader at the time the offset was committed,
  enabling consumers to detect stale offset data.

  All other fields remain the same as V2/V3:
  - `throttle_time_ms` at top level
  - `responses` with `topic`, `partition_responses` containing
    `partition`, `error_code`, `timestamp`, `offset`, `leader_epoch`
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v4_offset/2)
  end
end
