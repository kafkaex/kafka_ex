defimpl KafkaEx.Protocol.Kayrock.SyncGroup.Response, for: Kayrock.SyncGroup.V1.Response do
  @moduledoc """
  Implementation for SyncGroup v1 Response.

  V1 adds throttle_time_ms to the response compared to v0.
  """

  alias KafkaEx.Protocol.Kayrock.SyncGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
