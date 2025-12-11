defimpl KafkaEx.New.Protocols.Kayrock.LeaveGroup.Response, for: Kayrock.LeaveGroup.V0.Response do
  @moduledoc """
  Implementation for LeaveGroup v0 Response.
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
