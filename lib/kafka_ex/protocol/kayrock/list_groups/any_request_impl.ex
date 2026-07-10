defimpl KafkaEx.Protocol.Kayrock.ListGroups.Request, for: Any do
  @moduledoc """
  Fallback implementation of ListGroups Request for unknown future versions.

  ListGroups has no request fields, so the template is returned unchanged.
  """

  def build_request(request, _opts), do: request
end
