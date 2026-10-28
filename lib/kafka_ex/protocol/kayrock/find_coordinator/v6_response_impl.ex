defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Response, for: Kayrock.FindCoordinator.V6.Response do
  @moduledoc """
  V6 FindCoordinator Response implementation.

  Same structure as V4/V5 (`coordinators` array).
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v4_plus_response(response)
  end
end
