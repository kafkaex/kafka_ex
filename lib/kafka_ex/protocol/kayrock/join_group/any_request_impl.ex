defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Request, for: Any do
  @moduledoc """
  Fallback implementation of JoinGroup Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V6) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `group_instance_id` key: V5+ path (build_v5_plus_request)
  - Has `rebalance_timeout_ms` key: V1-V4 path (build_v1_or_v2_request)
  - Otherwise: V0 path (build_v0_request)
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers

  def build_request(request_template, opts) do
    cond do
      Map.has_key?(request_template, :group_instance_id) ->
        RequestHelpers.build_v5_plus_request(request_template, opts)

      Map.has_key?(request_template, :rebalance_timeout_ms) ->
        RequestHelpers.build_v1_or_v2_request(request_template, opts)

      true ->
        RequestHelpers.build_v0_request(request_template, opts)
    end
  end
end
