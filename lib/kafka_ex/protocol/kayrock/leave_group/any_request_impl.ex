defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Request, for: Any do
  @moduledoc """
  Fallback implementation of LeaveGroup Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V4) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `members` key: V3+ path (build_v3_plus_request -- batch leave)
  - Otherwise: V0-V2 path (build_request_from_template -- single member)
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.RequestHelpers

  def build_request(request_template, opts) do
    if Map.has_key?(request_template, :members) do
      RequestHelpers.build_v3_plus_request(request_template, opts)
    else
      RequestHelpers.build_request_from_template(request_template, opts)
    end
  end
end
