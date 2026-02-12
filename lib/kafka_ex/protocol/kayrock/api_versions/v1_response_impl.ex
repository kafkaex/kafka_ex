defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Response, for: Kayrock.ApiVersions.V1.Response do
  @moduledoc """
  Parses ApiVersions V1 responses.

  V1 adds `throttle_time_ms` to the response. Delegates to
  `ResponseHelpers.parse/1` for the shared V1+ parsing logic.
  """

  alias KafkaEx.Protocol.Kayrock.ApiVersions.ResponseHelpers

  @doc """
  Parses an ApiVersions V1 response into a KafkaEx struct.
  """
  @spec parse_response(Kayrock.ApiVersions.V1.Response.t()) ::
          {:ok, KafkaEx.Messages.ApiVersions.t()} | {:error, KafkaEx.Client.Error.t()}
  def parse_response(response) do
    ResponseHelpers.parse(response)
  end
end
