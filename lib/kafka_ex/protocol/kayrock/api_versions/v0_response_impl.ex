defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Response, for: Kayrock.ApiVersions.V0.Response do
  @moduledoc """
  Parses ApiVersions V0 responses.

  V0 is the only version without `throttle_time_ms`, so it uses
  `ResponseHelpers.parse_v0/1` instead of the standard `parse/1`.
  """

  alias KafkaEx.Protocol.Kayrock.ApiVersions.ResponseHelpers

  @doc """
  Parses an ApiVersions V0 response into a KafkaEx struct.
  """
  @spec parse_response(Kayrock.ApiVersions.V0.Response.t()) ::
          {:ok, KafkaEx.Messages.ApiVersions.t()} | {:error, KafkaEx.Client.Error.t()}
  def parse_response(response) do
    ResponseHelpers.parse_v0(response)
  end
end
