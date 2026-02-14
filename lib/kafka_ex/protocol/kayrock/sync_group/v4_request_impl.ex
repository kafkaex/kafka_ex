defimpl KafkaEx.Protocol.Kayrock.SyncGroup.Request, for: Kayrock.SyncGroup.V4.Request do
  @moduledoc """
  Implementation for SyncGroup v4 Request.

  V4 is the flexible version (KIP-482) with compact string encoding and tagged_fields.
  Logical fields are identical to V3 -- Kayrock handles the encoding differences.
  """

  alias KafkaEx.Protocol.Kayrock.SyncGroup.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v3_plus_request(request_template, opts)
  end
end
