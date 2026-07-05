defimpl KafkaEx.Protocol.Kayrock.ListGroups.Response, for: Kayrock.ListGroups.V0.Response do
  @moduledoc """
  Implementation for ListGroups v0 Response (no throttle_time_ms).
  """

  alias KafkaEx.Protocol.Kayrock.ListGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
