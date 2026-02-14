defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Response, for: Kayrock.LeaveGroup.V0.Response do
  @moduledoc """
  Implementation for LeaveGroup v0 Response.

  V0 has no throttle_time_ms. Returns `{:ok, :no_error}` on success.
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v0_response(response)
  end
end
