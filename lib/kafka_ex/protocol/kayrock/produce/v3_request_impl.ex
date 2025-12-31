defimpl KafkaEx.Protocol.Kayrock.Produce.Request, for: Kayrock.Produce.V3.Request do
  @moduledoc """
  Implementation for Produce V3 Request.

  V3 is a major change from V0-V2:
  - Uses RecordBatch format instead of MessageSet
  - Adds transactional_id field for transactional producers
  - Supports per-record headers
  - Supports per-record timestamps

  For non-transactional producers, transactional_id should be nil.
  """

  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_v3_plus(request_template, opts)
  end
end
