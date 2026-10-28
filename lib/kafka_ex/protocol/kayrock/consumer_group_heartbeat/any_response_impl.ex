defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.Response, for: Any do
  @moduledoc """
  Fallback implementation of ConsumerGroupHeartbeat Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V1) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.
  All known versions share one response schema, so it uses the same parser.
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
