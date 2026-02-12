defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Response, for: Kayrock.CreateTopics.V0.Response do
  @moduledoc """
  Implementation for CreateTopics V0 Response.

  V0 schema:
  - topic_errors: array of {topic: string, error_code: int16}
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers

  def parse_response(%{topics: topics}) do
    # V0 does not have error_message field
    topic_results = ResponseHelpers.parse_topic_results(topics, false)
    {:ok, ResponseHelpers.build_response(topic_results)}
  end
end
