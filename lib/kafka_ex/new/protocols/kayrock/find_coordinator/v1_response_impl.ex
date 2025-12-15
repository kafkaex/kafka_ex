defimpl KafkaEx.New.Protocols.Kayrock.FindCoordinator.Response, for: Kayrock.FindCoordinator.V1.Response do
  alias KafkaEx.New.Protocols.Kayrock.FindCoordinator.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v1_response(response)
  end
end
