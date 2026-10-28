defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Request, for: Any do
  @moduledoc """
  Fallback implementation of OffsetFetch Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V10) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detects the request shape by its fields:
  - Has `groups` (V8+): batched groups, V9 group shape (`member_id`/`member_epoch`)
  - Has `require_stable` (V7): group_id + topics + require_stable
  - Otherwise: group_id + topics
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers

  # The V9 group shape is a superset of V8's; Kayrock's serializer only reads
  # the keys its schema defines, so the extra member fields are harmless.
  def build_request(%{groups: _} = request_template, opts) do
    RequestHelpers.build_v9_request(request_template, opts)
  end

  def build_request(%{require_stable: _} = request_template, opts) do
    RequestHelpers.build_v7_request(request_template, opts)
  end

  def build_request(request_template, opts) do
    RequestHelpers.build_request(request_template, opts)
  end
end
