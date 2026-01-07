defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Request, for: Kayrock.FindCoordinator.V0.Request do
  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request, opts) do
    group_id = RequestHelpers.extract_group_id(opts)

    %{request | group_id: group_id}
  end
end
