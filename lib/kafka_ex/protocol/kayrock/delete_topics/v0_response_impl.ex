defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Response,
  for: Kayrock.DeleteTopics.V0.Response do
  @moduledoc """
  V0 implementation of DeleteTopics Response protocol.

  V0 Response Schema:
  - topic_error_codes: Array of [topic, error_code]

  V0 does not include throttle_time_ms.
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.ResponseHelpers

  def parse_response(response) do
    topic_results = ResponseHelpers.parse_topic_results(response.topic_error_codes)
    result = ResponseHelpers.build_response(topic_results)
    {:ok, result}
  end
end
