defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Response, for: Kayrock.LeaveGroup.V2.Response do
  @moduledoc """
  Implementation for LeaveGroup v2 Response.

  V2 response schema is identical to V1 (throttle_time_ms + error_code).
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_v2_response(response)
  end
end
