defimpl KafkaEx.Protocol.Kayrock.Produce.Request, for: Any do
  @moduledoc """
  Fallback implementation of Produce Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V8) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Delegates to the V3+ helper since all versions from V3 onward use the same
  RecordBatch request format with transactional_id support.
  """

  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers

  def build_request(request_template, opts) do
    if Map.has_key?(request_template, :transactional_id) do
      RequestHelpers.build_request_v3_plus(request_template, opts)
    else
      RequestHelpers.build_request_v0_v2(request_template, opts)
    end
  end
end
