defimpl KafkaEx.New.Protocols.Kayrock.LeaveGroup.Response, for: Kayrock.LeaveGroup.V1.Response do
  @moduledoc """
  Implementation for LeaveGroup v1 Response.

  V1 includes throttle_time_ms field.
  """

  alias KafkaEx.New.Client.Error, as: ErrorStruct
  alias KafkaEx.New.Kafka.LeaveGroup, as: LeaveGroupStruct

  def parse_response(%{error_code: 0, throttle_time_ms: throttle_time_ms}) do
    leave_group = LeaveGroupStruct.build(throttle_time_ms: throttle_time_ms)
    {:ok, leave_group}
  end

  def parse_response(%{error_code: error_code}) do
    error = ErrorStruct.build(error_code, %{})
    {:error, error}
  end
end
