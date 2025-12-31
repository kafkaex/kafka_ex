defimpl KafkaEx.Protocol.Kayrock.Produce.Request, for: Kayrock.Produce.V5.Request do
  @moduledoc """
  Implementation for Produce V5 Request.

  V5 has the same request schema as V3/V4:
  - Uses RecordBatch format
  - Supports transactional_id for transactional producers
  - Supports per-record headers and timestamps

  The difference from V3/V4 is in the response which includes `log_start_offset`.
  """

  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v3_plus(request_template, opts)
  end
end
