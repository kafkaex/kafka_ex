defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V6.Response do
  @moduledoc """
  Implementation for OffsetCommit V6 Response.

  V6 response is identical to V4/V5. The only V6 change is in the request
  (addition of committed_leader_epoch per partition).
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
