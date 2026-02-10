defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Response, for: Kayrock.Heartbeat.V1.Response do
  @moduledoc """
  Implementation for Heartbeat v1 Response.

  V1 includes throttle_time_ms field.
  """

  alias KafkaEx.Client.Error, as: ErrorStruct
  alias KafkaEx.Messages.Heartbeat, as: HeartbeatStruct

  def parse_response(%{error_code: 0, throttle_time_ms: throttle_time_ms}) do
    heartbeat = HeartbeatStruct.build(throttle_time_ms: throttle_time_ms)
    {:ok, heartbeat}
  end

  def parse_response(%{error_code: error_code}) do
    error = ErrorStruct.build(error_code, %{})
    {:error, error}
  end
end
