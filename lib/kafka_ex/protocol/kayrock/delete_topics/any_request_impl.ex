defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Request, for: Any do
  @moduledoc """
  Fallback implementation of DeleteTopics Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V4) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  All DeleteTopics versions share the same domain-relevant request fields
  (topic_names, timeout_ms), so a single path handles everything.
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_request_from_template(request_template, opts)
  end
end
