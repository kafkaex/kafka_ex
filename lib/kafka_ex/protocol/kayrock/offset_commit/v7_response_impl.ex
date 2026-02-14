defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V7.Response do
  @moduledoc """
  Implementation for OffsetCommit V7 Response.

  V7 response is identical to V4-V6. The only V7 change is in the request
  (addition of group_instance_id for static membership, KIP-345).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
