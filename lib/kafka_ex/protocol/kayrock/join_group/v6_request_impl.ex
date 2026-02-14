defimpl KafkaEx.Protocol.Kayrock.JoinGroup.Request, for: Kayrock.JoinGroup.V6.Request do
  @moduledoc """
  Implementation for JoinGroup V6 Request.

  V6 is the flexible version (KIP-482) — same logical fields as V5 but
  uses compact_string, compact_nullable_string, compact_array, compact_bytes,
  and tagged_fields. Kayrock handles compact encoding transparently.
  """

  alias KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v5_plus_request(request_template, opts)
  end
end
