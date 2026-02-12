defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Response, for: Kayrock.ApiVersions.V2.Response do
  @moduledoc """
  Parses ApiVersions V2 responses.

  V2 response schema is identical to V1: error_code, api_keys array, and
  throttle_time_ms. Delegates to `ResponseHelpers.parse/1`.
  """

  alias KafkaEx.Protocol.Kayrock.ApiVersions.ResponseHelpers

  @doc """
  Parses an ApiVersions V2 response into a KafkaEx struct.
  """
  @spec parse_response(Kayrock.ApiVersions.V2.Response.t()) ::
          {:ok, KafkaEx.Messages.ApiVersions.t()} | {:error, KafkaEx.Client.Error.t()}
  def parse_response(response) do
    ResponseHelpers.parse(response)
  end
end
