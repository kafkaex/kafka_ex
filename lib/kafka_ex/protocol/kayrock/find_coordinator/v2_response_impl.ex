defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Response, for: Kayrock.FindCoordinator.V2.Response do
  @moduledoc """
  V2 FindCoordinator Response implementation.

  V2 response is schema-identical to V1: `throttle_time_ms, error_code,
  error_message, node_id, host, port`. No new fields were added in V2.
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_response(response)
  end
end
