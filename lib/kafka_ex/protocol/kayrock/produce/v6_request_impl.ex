defimpl KafkaEx.Protocol.Kayrock.Produce.Request, for: Kayrock.Produce.V6.Request do
  @moduledoc """
  Implementation for Produce V6 Request.

  V6 has the same request schema as V3-V5:
  - Uses RecordBatch format
  - Supports transactional_id for transactional producers
  - Supports per-record headers and timestamps

  The wire protocol is identical to V5. V6 was introduced alongside
  broker-side improvements but does not add new request fields.
  """

  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v3_plus(request_template, opts)
  end
end
