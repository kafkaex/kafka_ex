defimpl KafkaEx.Protocol.Kayrock.Fetch.Response, for: Kayrock.Fetch.V11.Response do
  @moduledoc """
  Implementation for Fetch V11 Response.

  V11 adds `preferred_read_replica` in the partition header (KIP-392).
  This tells the consumer which replica it should prefer for reading,
  enabling rack-aware consumption. A value of -1 means no preference.

  All other fields remain the same as V5-V10:
  - `throttle_time_ms` at top level
  - `error_code` and `session_id` at top level (for fetch sessions)
  - `last_stable_offset`, `log_start_offset`, `aborted_transactions` per partition
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v11_fields/2)
  end
end
