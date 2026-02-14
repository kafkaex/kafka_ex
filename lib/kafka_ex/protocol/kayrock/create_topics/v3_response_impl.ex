defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Response, for: Kayrock.CreateTopics.V3.Response do
  @moduledoc """
  Implementation for CreateTopics V3 Response.

  V3 response schema is identical to V2:
  - throttle_time_ms: int32
  - topics: array of {name: string, error_code: int16, error_message: nullable_string}

  V3 is a pure version bump with no changes to the response format.
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers

  def parse_response(%{throttle_time_ms: throttle_time_ms, topics: topics}) do
    topic_results = ResponseHelpers.parse_topic_results(topics, true)
    {:ok, ResponseHelpers.build_response(topic_results, throttle_time_ms)}
  end
end
