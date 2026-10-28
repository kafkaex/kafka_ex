defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Response, for: Kayrock.FindCoordinator.V5.Response do
  @moduledoc """
  V5 FindCoordinator Response implementation.

  Same structure as V4 (`coordinators` array). V5 can return
  `TRANSACTION_ABORTABLE` (KIP-890) for transaction coordinator lookups.
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v4_plus_response(response)
  end
end
