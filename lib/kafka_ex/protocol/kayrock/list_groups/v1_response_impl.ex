defimpl KafkaEx.Protocol.Kayrock.ListGroups.Response, for: Kayrock.ListGroups.V1.Response do
  @moduledoc """
  Implementation for ListGroups v1 Response. Adds throttle_time_ms, ignored in the domain layer.
  """

  alias KafkaEx.Protocol.Kayrock.ListGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
