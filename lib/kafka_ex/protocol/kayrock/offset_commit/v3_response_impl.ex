defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V3.Response do
  @moduledoc """
  Implementation for OffsetCommit V3 Response.

  V3 adds throttle_time_ms field (not currently used in response parsing).
  Response only contains partition and error_code (no offset returned).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
