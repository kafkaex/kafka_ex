defimpl KafkaEx.Protocol.Kayrock.ListGroups.Request, for: Kayrock.ListGroups.V3.Request do
  @moduledoc """
  Implementation for ListGroups v3 Request (FLEX). No request fields; tagged_fields handled by Kayrock.
  """

  def build_request(request, _opts), do: request
end
