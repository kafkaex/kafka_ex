defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Any do
  @moduledoc """
  Fallback implementation of OffsetCommit Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V8) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  All OffsetCommit response versions share the same structure:
  topics -> partitions -> {partition_index, error_code}.
  This fallback applies the same parsing logic regardless of version.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
