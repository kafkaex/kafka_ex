defimpl KafkaEx.Protocol.Kayrock.SyncGroup.Response, for: Kayrock.SyncGroup.V2.Response do
  @moduledoc """
  Implementation for SyncGroup v2 Response.

  V2 response has the same structure as V1 (throttle_time_ms, error_code, assignment).
  Pure version bump.
  """

  alias KafkaEx.Protocol.Kayrock.SyncGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
