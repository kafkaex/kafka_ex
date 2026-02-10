defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Request, for: Kayrock.CreateTopics.V1.Request do
  @moduledoc """
  Implementation for CreateTopics V1 Request.

  V1 schema:
  - create_topic_requests: array of topic configs
  - timeout: int32
  - validate_only: boolean (new in V1)
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v1_v2_request(request_template, opts)
  end
end
