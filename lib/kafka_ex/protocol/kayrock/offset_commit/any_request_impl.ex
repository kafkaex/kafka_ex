defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Request, for: Any do
  @moduledoc """
  Fallback implementation of OffsetCommit Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V8) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detects the version range by checking for distinguishing request fields:
  - Has `group_instance_id`: V7+ path (includes committed_leader_epoch)
  - Has `retention_time_ms`: V2/V3/V4 path
  - Has `generation_id`: V5/V6+ path (uses build_v6_request which is a safe
    superset of V5 — adds committed_leader_epoch defaulting to -1)
  - Otherwise: V0 path
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  def build_request(request_template, opts) do
    cond do
      Map.has_key?(request_template, :group_instance_id) ->
        RequestHelpers.build_v7_plus_request(request_template, opts)

      Map.has_key?(request_template, :retention_time_ms) ->
        RequestHelpers.build_v2_v3_request(request_template, opts)

      Map.has_key?(request_template, :generation_id) ->
        # V5/V6+ path: build_v6_request is a safe superset of V5 — it includes
        # committed_leader_epoch (defaults to -1 when absent from opts).
        # For V5 structs, the extra field is ignored by Kayrock's serializer.
        RequestHelpers.build_v6_request(request_template, opts)

      true ->
        # V0 fallback
        %{group_id: group_id} = RequestHelpers.extract_common_fields(opts)
        topics = RequestHelpers.build_topics(opts, false)

        request_template
        |> Map.put(:group_id, group_id)
        |> Map.put(:topics, topics)
    end
  end
end
