defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Response, for: Kayrock.JoinGroup.V6.Response do
  @moduledoc """
  Implementation for JoinGroup V6 Response.

  V6 is the flexible version (KIP-482) — same logical fields as V5 but
  uses compact_string, compact_nullable_string, compact_array, compact_bytes,
  and tagged_fields. Kayrock handles compact deserialization transparently.

  `group_instance_id` in members is NOT extracted (same as V5).
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v2_plus_response(response)
  end
end
