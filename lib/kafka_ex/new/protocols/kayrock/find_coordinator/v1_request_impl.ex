defimpl KafkaEx.New.Protocols.Kayrock.FindCoordinator.Request, for: Kayrock.FindCoordinator.V1.Request do
  alias KafkaEx.New.Protocols.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request, opts) do
    {coordinator_key, coordinator_type} = RequestHelpers.extract_v1_fields(opts)

    %{request | coordinator_key: coordinator_key, coordinator_type: coordinator_type}
  end
end
