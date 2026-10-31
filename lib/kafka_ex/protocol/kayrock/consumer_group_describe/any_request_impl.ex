defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.Request, for: Any do
  @moduledoc """
  Fallback implementation of ConsumerGroupDescribe Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V1) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.
  All known versions share one request schema.
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
