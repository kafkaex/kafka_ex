defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Request, for: Kayrock.FindCoordinator.V1.Request do
  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_v1_plus_request(request, opts)
  end
end
