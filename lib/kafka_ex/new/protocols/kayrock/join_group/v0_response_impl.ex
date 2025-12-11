defimpl KafkaEx.New.Protocols.Kayrock.JoinGroup.Response, for: Kayrock.JoinGroup.V0.Response do
  @moduledoc """
  Implementation for JoinGroup V0 Response.

  V0 provides basic JoinGroup with session_timeout.
  Does not include throttle_time_ms.
  """

  alias KafkaEx.New.Protocols.Kayrock.JoinGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn _resp -> nil end)
  end
end
