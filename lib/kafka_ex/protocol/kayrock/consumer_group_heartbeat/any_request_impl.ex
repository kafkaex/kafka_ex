defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.Request, for: Any do
  @moduledoc """
  Fallback implementation of ConsumerGroupHeartbeat Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V1) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `subscribed_topic_regex` key: V1+ path (build_v1_plus_request)
  - Otherwise: V0 path (build_v0_request)
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.RequestHelpers

  def build_request(request_template, opts) do
    if Map.has_key?(request_template, :subscribed_topic_regex) do
      RequestHelpers.build_v1_plus_request(request_template, opts)
    else
      RequestHelpers.build_v0_request(request_template, opts)
    end
  end
end
