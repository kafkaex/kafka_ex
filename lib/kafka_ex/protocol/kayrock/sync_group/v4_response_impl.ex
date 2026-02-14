defimpl KafkaEx.Protocol.Kayrock.SyncGroup.Response, for: Kayrock.SyncGroup.V4.Response do
  @moduledoc """
  Implementation for SyncGroup v4 Response.

  V4 is the flexible version (KIP-482) with compact bytes encoding and tagged_fields.
  Kayrock handles compact deserialization transparently. The `assignment` field may
  be raw binary instead of `%Kayrock.MemberAssignment{}` -- the ResponseHelpers
  `extract_partition_assignments/1` handles both formats.
  """

  alias KafkaEx.Protocol.Kayrock.SyncGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
