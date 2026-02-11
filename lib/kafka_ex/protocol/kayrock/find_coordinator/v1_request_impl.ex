defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Request, for: Kayrock.FindCoordinator.V1.Request do
  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request, opts) do
    {coordinator_key, coordinator_type} = RequestHelpers.extract_v1_fields(opts)

    %{request | key: coordinator_key, key_type: coordinator_type}
  end
end
