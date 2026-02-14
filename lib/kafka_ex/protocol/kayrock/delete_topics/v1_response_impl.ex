defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Response,
  for: Kayrock.DeleteTopics.V1.Response do
  @moduledoc """
  V1 implementation of DeleteTopics Response protocol.

  V1 Response Schema:
  - throttle_time_ms: Time in milliseconds the request was throttled
  - topic_error_codes: Array of [topic, error_code]
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
