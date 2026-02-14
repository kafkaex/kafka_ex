defimpl KafkaEx.Protocol.Kayrock.DescribeGroups.Request, for: Any do
  @moduledoc """
  Fallback implementation of DescribeGroups Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V5) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `include_authorized_operations` key: V3+ path (build_v3_plus_request)
  - Otherwise: V0-V2 path (build_request_from_template)
  """

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpers

  def build_request(request_template, opts) do
    if Map.has_key?(request_template, :include_authorized_operations) do
      RequestHelpers.build_v3_plus_request(request_template, opts)
    else
      RequestHelpers.build_request_from_template(request_template, opts)
    end
  end
end
