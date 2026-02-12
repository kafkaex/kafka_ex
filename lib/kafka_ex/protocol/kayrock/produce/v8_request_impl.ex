defimpl KafkaEx.Protocol.Kayrock.Produce.Request, for: Kayrock.Produce.V8.Request do
  @moduledoc """
  Implementation for Produce V8 Request.

  V8 has the same request schema as V3-V7:
  - Uses RecordBatch format
  - Supports transactional_id for transactional producers
  - Supports per-record headers and timestamps

  The wire request is identical to V7. The only change in V8 is in the
  response, which adds `record_errors` and `error_message` per partition
  for better error diagnostics (KIP-467).
  """

  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v3_plus(request_template, opts)
  end
end
