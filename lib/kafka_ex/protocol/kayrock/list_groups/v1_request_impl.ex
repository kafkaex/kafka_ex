defimpl KafkaEx.Protocol.Kayrock.ListGroups.Request, for: Kayrock.ListGroups.V1.Request do
  @moduledoc """
  Implementation for ListGroups v1 Request. V1 has no request fields.
  """

  def build_request(request, _opts), do: request
end
