defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V5.Response do
  @moduledoc """
  Implementation for OffsetCommit V5 Response.

  V5 response is identical to V4 (throttle_time_ms + topics with error_code).
  The only V5 change is in the request (removal of retention_time_ms).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
