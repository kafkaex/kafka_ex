defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Response, for: Kayrock.LeaveGroup.V4.Response do
  @moduledoc """
  Implementation for LeaveGroup v4 Response.

  V4 is the flexible version (KIP-482) with compact encoding and tagged_fields.
  Domain-relevant fields (throttle_time_ms, error_code, members) are identical to V3.
  Kayrock handles compact decoding transparently.
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v3_plus_response(response)
  end
end
