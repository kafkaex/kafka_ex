defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Request, for: Kayrock.FindCoordinator.V6.Request do
  @moduledoc """
  V6 FindCoordinator Request implementation.

  Same request as V4/V5. V6 adds the share-group key type (KIP-932), which
  KafkaEx does not use; group (0) and transaction (1) keys are unchanged.
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_v4_plus_request(request, opts)
  end
end
