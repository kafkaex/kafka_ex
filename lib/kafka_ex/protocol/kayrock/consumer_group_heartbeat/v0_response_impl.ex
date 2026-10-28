defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.Response, for: Kayrock.ConsumerGroupHeartbeat.V0.Response do
  @moduledoc """
  Implementation for ConsumerGroupHeartbeat v0 Response.

  V0 is the initial KIP-848 version.
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
