defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Response, for: Kayrock.FindCoordinator.V0.Response do
  alias KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v0_response(response)
  end
end
