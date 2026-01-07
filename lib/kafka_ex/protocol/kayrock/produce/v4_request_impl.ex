defimpl KafkaEx.Protocol.Kayrock.Produce.Request, for: Kayrock.Produce.V4.Request do
  @moduledoc """
  Implementation for Produce V4 Request.

  V4 has the same request schema as V3:
  - Uses RecordBatch format
  - Supports transactional_id for transactional producers
  - Supports per-record headers and timestamps

  The only difference from V3 is in the response handling (same fields).
  """

  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v3_plus(request_template, opts)
  end
end
