defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Response, for: Kayrock.LeaveGroup.V1.Response do
  @moduledoc """
  Implementation for LeaveGroup v1 Response.

  V1 includes throttle_time_ms field.
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_v2_response(response)
  end
end
