defimpl KafkaEx.New.Protocols.Kayrock.ApiVersions.Request, for: Kayrock.ApiVersions.V0.Request do
  @moduledoc """
  Implements ApiVersions V0 request building.
  V0 requests are parameter-less - the broker returns all supported API versions regardless of the request content.
  """

  @doc """
  Builds an ApiVersions V0 request.
  Since ApiVersions requests have no parameters, this simply returns the request struct unchanged.
  """
  @spec build_request(Kayrock.ApiVersions.V0.Request.t(), Keyword.t()) :: Kayrock.ApiVersions.V0.Request.t()
  def build_request(request, _opts) do
    request
  end
end
