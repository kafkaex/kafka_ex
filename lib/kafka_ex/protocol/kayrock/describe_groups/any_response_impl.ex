defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Response, for: Any do
  @moduledoc """
  Fallback implementation of DescribeGroups Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V5) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  All versions share identical parsing logic through `ResponseHelpers.parse_response/1`
  since the domain struct (`ConsumerGroupDescription`) gracefully handles optional fields
  via `Map.get/2`.
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
