defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Request, for: Kayrock.CreateTopics.V3.Request do
  @moduledoc """
  Implementation for CreateTopics V3 Request.

  V3 request schema is identical to V1/V2:
  - topics: array of topic configs
  - timeout_ms: int32
  - validate_only: boolean

  V3 is a pure version bump. The only changes are in the response format.
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v1_plus_request(request_template, opts)
  end
end
