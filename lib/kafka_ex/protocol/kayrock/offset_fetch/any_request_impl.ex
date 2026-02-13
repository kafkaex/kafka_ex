defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Request, for: Any do
  @moduledoc """
  Fallback implementation of OffsetFetch Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V6) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  All OffsetFetch request versions use the same request structure (group_id + topics),
  so this fallback applies the same logic regardless of version.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers

  def build_request(request_template, opts) do
    %{group_id: group_id} = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:topics, topics)
  end
end
