defimpl KafkaEx.Protocol.Kayrock.DeleteTopics.Response,
  for: Kayrock.DeleteTopics.V4.Response do
  @moduledoc """
  V4 implementation of DeleteTopics Response protocol.

  V4 is the flexible version (KIP-482) with compact encodings + tagged_fields.
  Kayrock handles the decoding transparently. Domain-relevant fields are
  identical to V1-V3:
  - throttle_time_ms: Time in milliseconds the request was throttled
  - responses: Array of [name, error_code]
  """

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_plus_response(response)
  end
end
