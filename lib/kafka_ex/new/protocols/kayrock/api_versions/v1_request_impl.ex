defimpl KafkaEx.New.Protocols.Kayrock.ApiVersions.Request, for: Kayrock.ApiVersions.V1.Request do
  @moduledoc """
  Implements ApiVersions V1 request building.
  """

  @doc """
  Builds an ApiVersions V1 request.
  Since ApiVersions requests have no parameters, this simply returns the request struct unchanged.
  """
  @spec build_request(Kayrock.ApiVersions.V1.Request.t(), Keyword.t()) :: Kayrock.ApiVersions.V1.Request.t()
  def build_request(request, _opts) do
    request
  end
end
