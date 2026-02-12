defimpl KafkaEx.Protocol.Kayrock.Produce.Request, for: Kayrock.Produce.V7.Request do
  @moduledoc """
  Implementation for Produce V7 Request.

  V7 has the same request schema as V3-V6:
  - Uses RecordBatch format
  - Supports transactional_id for transactional producers
  - Supports per-record headers and timestamps

  The wire protocol is identical to V6. V7 was introduced alongside
  improvements to error handling (KIP-467) but does not change the
  request format.
  """

  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v3_plus(request_template, opts)
  end
end
