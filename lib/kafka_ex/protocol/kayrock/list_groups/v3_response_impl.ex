defimpl KafkaEx.Protocol.Kayrock.ListGroups.Response, for: Kayrock.ListGroups.V3.Response do
  @moduledoc """
  Implementation for ListGroups v3 Response (FLEX version; tagged_fields ignored in domain layer).
  """

  alias KafkaEx.Protocol.Kayrock.ListGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
