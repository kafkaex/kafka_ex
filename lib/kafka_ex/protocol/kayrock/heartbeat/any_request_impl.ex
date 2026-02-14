defimpl KafkaEx.Protocol.Kayrock.Heartbeat.Request, for: Any do
  @moduledoc """
  Fallback implementation of Heartbeat Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V4) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `group_instance_id` key: V3+ path (build_v3_plus_request)
  - Otherwise: V0-V2 path (build_request_from_template)
  """

  alias KafkaEx.Protocol.Kayrock.Heartbeat.RequestHelpers

  def build_request(request_template, opts) do
    if Map.has_key?(request_template, :group_instance_id) do
      RequestHelpers.build_v3_plus_request(request_template, opts)
    else
      RequestHelpers.build_request_from_template(request_template, opts)
    end
  end
end
