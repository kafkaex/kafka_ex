defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Response, for: Kayrock.CreateTopics.V1.Response do
  @moduledoc """
  Implementation for CreateTopics V1 Response.

  V1 schema:
  - topic_errors: array of {topic: string, error_code: int16, error_message: nullable_string}
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers

  def parse_response(%{topic_errors: topic_errors}) do
    # V1 has error_message field
    topic_results = ResponseHelpers.parse_topic_results(topic_errors, true)
    {:ok, ResponseHelpers.build_response(topic_results)}
  end
end
