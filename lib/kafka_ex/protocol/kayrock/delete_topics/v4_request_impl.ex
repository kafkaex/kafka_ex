defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Request,
  for: Kayrock.DeleteTopics.V4.Request do
  @moduledoc """
  V4 implementation of DeleteTopics Request protocol.

  V4 is the flexible version (KIP-482) with compact encodings + tagged_fields.
  Kayrock handles the encoding transparently. Domain-relevant fields are
  identical to V0-V3:
  - topics: List of topic names to delete
  - timeout: Request timeout in milliseconds
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_request_from_template(request, opts)
  end
end
