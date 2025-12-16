defimpl KafkaEx.New.Protocols.Kayrock.DeleteTopics.Response,
  for: Kayrock.DeleteTopics.V1.Response do
  @moduledoc """
  V1 implementation of DeleteTopics Response protocol.

  V1 Response Schema:
  - throttle_time_ms: Time in milliseconds the request was throttled
  - topic_error_codes: Array of [topic, error_code]
  """

  alias KafkaEx.New.Protocols.Kayrock.DeleteTopics.ResponseHelpers

  def parse_response(response) do
    topic_results = ResponseHelpers.parse_topic_results(response.topic_error_codes)
    result = ResponseHelpers.build_response(topic_results, response.throttle_time_ms)
    {:ok, result}
  end
end
