defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V8.Response do
  @moduledoc """
  Implementation for OffsetCommit V8 Response.

  V8 is the flexible version (KIP-482) with compact encodings and tagged_fields.
  Kayrock handles the decoding. Response structure is identical to V4-V7
  (throttle_time_ms + topics with partition_index/error_code).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
