defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Response, for: Kayrock.ListOffsets.V5.Response do
  @moduledoc """
  Implementation for ListOffsets V5 Response.

  V5 has the same response schema as V4:
  - `throttle_time_ms` at top level
  - `responses` with `topic`, `partition_responses` containing
    `partition`, `error_code`, `timestamp`, `offset`, `leader_epoch`

  No response-side changes from V4.
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v4_offset/2)
  end
end
