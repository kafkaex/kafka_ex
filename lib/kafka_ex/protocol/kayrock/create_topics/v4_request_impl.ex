defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Request, for: Kayrock.CreateTopics.V4.Request do
  @moduledoc """
  Implementation for CreateTopics V4 Request.

  V4 request schema is identical to V1-V3:
  - topics: array of topic configs
  - timeout_ms: int32
  - validate_only: boolean

  V4 is a pure version bump. The only changes are in the response format.
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v1_plus_request(request_template, opts)
  end
end
