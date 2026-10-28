defimpl KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.Request, for: Kayrock.ConsumerGroupHeartbeat.V0.Request do
  @moduledoc """
  Implementation for ConsumerGroupHeartbeat v0 Request.

  V0 is the initial KIP-848 version.
  """

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v0_request(request_template, opts)
  end
end
