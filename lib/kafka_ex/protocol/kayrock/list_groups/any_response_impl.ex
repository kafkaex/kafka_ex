defimpl KafkaEx.Protocol.Kayrock.ListGroups.Response, for: Any do
  @moduledoc """
  Fallback implementation of ListGroups Response for unknown future versions.

  All versions share the same parsing via `ResponseHelpers.parse_response/1`.
  """

  alias KafkaEx.Protocol.Kayrock.ListGroups.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
