defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Request, for: Kayrock.ApiVersions.V2.Request do
  @moduledoc """
  Implements ApiVersions V2 request building.

  V2 requests are schema-identical to V1 — no parameters, no changes.
  The broker returns all supported API versions regardless of the request content.
  """

  @doc """
  Builds an ApiVersions V2 request.
  Since ApiVersions V2 requests have no parameters, this simply returns the request struct unchanged.
  """
  @spec build_request(Kayrock.ApiVersions.V2.Request.t(), Keyword.t()) :: Kayrock.ApiVersions.V2.Request.t()
  def build_request(request, _opts) do
    request
  end
end
