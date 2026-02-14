defimpl KafkaEx.Protocol.Kayrock.Fetch.Response, for: Kayrock.Fetch.V9.Response do
  @moduledoc """
  Implementation for Fetch V9 Response.

  V9 has the same response schema as V7/V8:
  - `throttle_time_ms` at top level
  - `error_code` and `session_id` at top level (for fetch sessions)
  - `last_stable_offset`, `log_start_offset`, `aborted_transactions` per partition

  No response-side changes from V8.
  Uses shared field extractor for V5+ responses.
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v5_plus_fields/2)
  end
end
