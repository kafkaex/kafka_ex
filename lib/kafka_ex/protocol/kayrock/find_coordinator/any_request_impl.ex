defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Request, for: Any do
  @moduledoc """
  Fallback implementation of FindCoordinator Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V3) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `key_type` key: V1+ path (uses key + key_type)
  - Otherwise: V0 path (uses key only, from group_id)
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request_template, opts) do
    if Map.has_key?(request_template, :key_type) do
      # V1+ path: key + key_type (works for V1, V2, V3, and future versions)
      {coordinator_key, coordinator_type} = RequestHelpers.extract_v1_fields(opts)

      request_template
      |> Map.put(:key, coordinator_key)
      |> Map.put(:key_type, coordinator_type)
    else
      # V0 path: only key (group_id)
      group_id = RequestHelpers.extract_group_id(opts)

      Map.put(request_template, :key, group_id)
    end
  end
end
