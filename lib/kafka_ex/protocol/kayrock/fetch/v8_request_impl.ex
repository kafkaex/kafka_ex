defimpl KafkaEx.Protocol.Kayrock.Fetch.Request, for: Kayrock.Fetch.V8.Request do
  @moduledoc """
  Implementation for Fetch V8 Request.

  V8 has the same request schema as V7:
  - Uses RecordBatch format
  - Supports incremental fetch sessions (session_id, session_epoch)
  - Includes max_bytes, isolation_level, log_start_offset

  No request-side changes from V7. The only V8 changes are response-side
  (same schema, no structural differences in Kayrock's implementation).
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_v7_plus(request, opts, 8)
  end
end
