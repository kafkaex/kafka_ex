defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Request, for: Kayrock.FindCoordinator.V5.Request do
  @moduledoc """
  V5 FindCoordinator Request implementation.

  Same request as V4. V5 only adds the `TRANSACTION_ABORTABLE` error code
  (KIP-890) for transaction coordinators.
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_v4_plus_request(request, opts)
  end
end
