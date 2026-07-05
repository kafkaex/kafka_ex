defimpl KafkaEx.Protocol.Kayrock.ListGroups.Request, for: Kayrock.ListGroups.V0.Request do
  @moduledoc """
  Implementation for ListGroups v0 Request. V0 has no request fields.
  """

  def build_request(request, _opts), do: request
end
