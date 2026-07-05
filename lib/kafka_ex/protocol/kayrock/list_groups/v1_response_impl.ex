defimpl KafkaEx.Protocol.Kayrock.ListGroups.Response, for: Kayrock.ListGroups.V1.Response do
  @moduledoc """
  Implementation for ListGroups v1 Response (adds throttle_time_ms (ignored in domain layer)).
  """

  alias KafkaEx.Protocol.Kayrock.ListGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
