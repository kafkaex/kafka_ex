defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Response, for: Kayrock.CreateTopics.V2.Response do
  @moduledoc """
  Implementation for CreateTopics V2 Response.

  V2 schema:
  - throttle_time_ms: int32 (new in V2)
  - topic_errors: array of {topic: string, error_code: int16, error_message: nullable_string}
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers

  def parse_response(%{throttle_time_ms: throttle_time_ms, topic_errors: topic_errors}) do
    # V2 has error_message field and throttle_time_ms
    topic_results = ResponseHelpers.parse_topic_results(topic_errors, true)
    {:ok, ResponseHelpers.build_response(topic_results, throttle_time_ms)}
  end
end
