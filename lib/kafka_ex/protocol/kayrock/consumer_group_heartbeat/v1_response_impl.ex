defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.Response, for: Kayrock.ConsumerGroupHeartbeat.V1.Response do
  @moduledoc """
  Implementation for ConsumerGroupHeartbeat v1 Response.

  V1 response schema is identical to V0.
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
