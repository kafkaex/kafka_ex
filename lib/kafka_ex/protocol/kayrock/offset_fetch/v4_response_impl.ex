defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V4.Response do
  @moduledoc """
  Implementation for OffsetFetch V4 Response.

  V4 is a pure version bump -- response structure is identical to V3.
  Includes top-level error_code field for broker-level errors.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response_with_top_level_error(response)
  end
end
