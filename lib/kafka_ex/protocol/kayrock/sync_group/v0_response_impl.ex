defimpl KafkaEx.Protocol.Kayrock.SyncGroup.Response, for: Kayrock.SyncGroup.V0.Response do
  @moduledoc """
  Implementation for SyncGroup v0 Response.

  V0 has no throttle_time_ms field.
  """

  alias KafkaEx.Protocol.Kayrock.SyncGroup.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v0_response(response)
  end
end
