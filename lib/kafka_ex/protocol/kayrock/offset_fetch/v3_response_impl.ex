defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V3.Response do
  @moduledoc """
  Implementation for OffsetFetch V3 Response.

  V3 adds throttle_time_ms field (not currently used in response parsing).
  Includes top-level error_code field for broker-level errors.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response_with_top_level_error(response)
  end
end
