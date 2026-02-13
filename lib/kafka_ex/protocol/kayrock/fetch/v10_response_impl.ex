defimpl KafkaEx.Protocol.Kayrock.Fetch.Response, for: Kayrock.Fetch.V10.Response do
  @moduledoc """
  Implementation for Fetch V10 Response.

  V10 has the same response schema as V7-V9:
  - `throttle_time_ms` at top level
  - `error_code` and `session_id` at top level (for fetch sessions)
  - `last_stable_offset`, `log_start_offset`, `aborted_transactions` per partition

  No response-side changes from V9.
  Uses shared field extractor for V5+ responses.
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v5_plus_fields/2)
  end
end
