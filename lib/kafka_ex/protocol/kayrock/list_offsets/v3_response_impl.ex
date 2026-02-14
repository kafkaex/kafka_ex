defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Response, for: Kayrock.ListOffsets.V3.Response do
  @moduledoc """
  Implementation for ListOffsets V3 Response.

  V3 response schema is identical to V2:
  - `throttle_time_ms` at top level
  - `responses` with `topic`, `partition_responses` containing
    `partition`, `error_code`, `timestamp`, `offset`

  No response-side changes from V2.
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v2_offset/2)
  end
end
