defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Response, for: Kayrock.ApiVersions.V3.Response do
  @moduledoc """
  Parses ApiVersions V3 responses.

  V3 is a flexible version (KIP-482) that uses compact encodings:
  - api_keys uses compact_array encoding with per-entry tagged_fields
  - Response includes top-level tagged_fields
  - Otherwise same logical fields as V1/V2: error_code, api_keys, throttle_time_ms

  Tagged fields from the response are silently discarded since KafkaEx
  does not currently use any defined tagged field extensions.

  Delegates to `ResponseHelpers.parse/1` for the shared parsing logic.
  """

  alias KafkaEx.Protocol.Kayrock.ApiVersions.ResponseHelpers

  @doc """
  Parses an ApiVersions V3 response into a KafkaEx struct.
  """
  @spec parse_response(Kayrock.ApiVersions.V3.Response.t()) ::
          {:ok, KafkaEx.Messages.ApiVersions.t()} | {:error, KafkaEx.Client.Error.t()}
  def parse_response(response) do
    ResponseHelpers.parse(response)
  end
end
