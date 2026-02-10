defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Response, for: Kayrock.JoinGroup.V2.Response do
  @moduledoc """
  Implementation for JoinGroup V2 Response.

  V2 adds throttle_time_ms to the response.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn resp -> resp.throttle_time_ms end)
  end
end
