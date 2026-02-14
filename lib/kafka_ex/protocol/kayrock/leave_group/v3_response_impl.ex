defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Response, for: Kayrock.LeaveGroup.V3.Response do
  @moduledoc """
  Implementation for LeaveGroup v3 Response.

  V3 adds `members` array with per-member error codes (KIP-345 batch leave).
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v3_plus_response(response)
  end
end
