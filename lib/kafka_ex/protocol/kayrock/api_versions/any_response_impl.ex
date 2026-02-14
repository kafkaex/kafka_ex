defimpl KafkaEx.Protocol.Kayrock.ApiVersions.Response, for: Any do
  @moduledoc """
  Fallback implementation of ApiVersions Response protocol.

  Delegates to `ResponseHelpers.parse/1` which handles all V1+ responses
  that include `error_code`, `api_keys`, and `throttle_time_ms` fields.

  This ensures forward compatibility: any future ApiVersions response version
  (V4+) that follows the same schema will be parsed correctly without
  requiring an explicit implementation.
  """

  alias KafkaEx.Protocol.Kayrock.ApiVersions.ResponseHelpers

  @doc """
  Parses an ApiVersions response into the KafkaEx domain struct.
  """
  def parse_response(response) do
    ResponseHelpers.parse(response)
  end
end
