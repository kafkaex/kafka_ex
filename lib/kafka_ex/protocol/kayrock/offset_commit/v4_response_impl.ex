defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V4.Response do
  @moduledoc """
  Implementation for OffsetCommit V4 Response.

  V4 is a pure version bump -- response structure is identical to V3.
  Includes throttle_time_ms. Response only contains partition_index and error_code.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
