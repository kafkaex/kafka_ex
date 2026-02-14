defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Response,
  for: Kayrock.DeleteTopics.V2.Response do
  @moduledoc """
  V2 implementation of DeleteTopics Response protocol.

  V2 Response Schema (identical to V1):
  - throttle_time_ms: Time in milliseconds the request was throttled
  - responses: Array of [name, error_code]
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
