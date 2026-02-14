defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Response, for: Kayrock.FindCoordinator.V3.Response do
  @moduledoc """
  V3 FindCoordinator Response implementation.

  V3 is the flexible version (KIP-482). The schema uses compact encodings:
  `throttle_time_ms, error_code, error_message(compact_nullable_string),
  node_id, host(compact_string), port, tagged_fields`.

  The domain-relevant fields are identical to V1/V2. Kayrock handles
  compact deserialization transparently.
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_response(response)
  end
end
