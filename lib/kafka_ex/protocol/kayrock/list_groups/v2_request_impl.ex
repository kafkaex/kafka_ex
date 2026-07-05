defimpl KafkaEx.Protocol.Kayrock.ListGroups.Request, for: Kayrock.ListGroups.V2.Request do
  @moduledoc """
  Implementation for ListGroups v2 Request. V2 has no request fields.
  """

  def build_request(request, _opts), do: request
end
