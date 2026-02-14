defimpl KafkaEx.Protocol.Kayrock.SyncGroup.Response, for: Kayrock.SyncGroup.V3.Response do
  @moduledoc """
  Implementation for SyncGroup v3 Response.

  V3 response has the same structure as V1/V2 (throttle_time_ms, error_code, assignment).
  The protocol_type and protocol_name fields mentioned in the Kafka spec are
  NOT present in Kayrock's generated V3 response schema.
  """

  alias KafkaEx.Protocol.Kayrock.SyncGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
