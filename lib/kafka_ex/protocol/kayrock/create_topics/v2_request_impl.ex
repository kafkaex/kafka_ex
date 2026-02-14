defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Request, for: Kayrock.CreateTopics.V2.Request do
  @moduledoc """
  Implementation for CreateTopics V2 Request.

  V2 schema is identical to V1:
  - create_topic_requests: array of topic configs
  - timeout: int32
  - validate_only: boolean

  The difference is in the response format (V2 adds throttle_time_ms).
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v1_plus_request(request_template, opts)
  end
end
