defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Response, for: Kayrock.Heartbeat.V0.Response do
  @moduledoc """
  Implementation for Heartbeat v0 Response.

  V0 has no throttle_time_ms. Returns `{:ok, :no_error}` on success.
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v0_response(response)
  end
end
