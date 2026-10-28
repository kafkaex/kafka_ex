defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.Response, for: Any do
  @moduledoc """
  Fallback implementation of ConsumerGroupDescribe Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V1) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.
  Optional fields (`member_type`) are read with `Map.get/2`, so the shared
  parser handles any version.
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
