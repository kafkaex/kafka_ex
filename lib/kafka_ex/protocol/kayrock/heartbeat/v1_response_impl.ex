defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Response, for: Kayrock.Heartbeat.V1.Response do
  @moduledoc """
  Implementation for Heartbeat v1 Response.

  V1 includes throttle_time_ms field.
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
