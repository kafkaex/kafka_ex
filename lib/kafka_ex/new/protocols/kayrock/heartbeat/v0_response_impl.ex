defimpl KafkaEx.New.Protocols.Kayrock.Heartbeat.Response, for: Kayrock.Heartbeat.V0.Response do
  @moduledoc """
  Implementation for Heartbeat v0 Response.
  """

  alias KafkaEx.New.Client.Error, as: ErrorStruct

  def parse_response(%{error_code: 0}) do
    {:ok, :no_error}
  end

  def parse_response(%{error_code: error_code}) do
    error = ErrorStruct.build(error_code, %{})
    {:error, error}
  end
end
