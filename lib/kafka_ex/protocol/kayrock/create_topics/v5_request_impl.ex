defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Request, for: Kayrock.CreateTopics.V5.Request do
  @moduledoc """
  Implementation for CreateTopics V5 Request.

  V5 is the flexible version (KIP-482) with compact types and tagged_fields.
  The logical fields are identical to V1-V4:
  - topics: compact_array of topic configs
  - timeout_ms: int32
  - validate_only: boolean
  - tagged_fields: tagged_fields

  Compact encoding is handled transparently by Kayrock's serializer,
  so we delegate to the same `build_v1_plus_request/2` helper.
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v1_plus_request(request_template, opts)
  end
end
