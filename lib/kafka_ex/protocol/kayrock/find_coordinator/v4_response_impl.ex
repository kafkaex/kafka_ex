defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Response, for: Kayrock.FindCoordinator.V4.Response do
  @moduledoc """
  V4 FindCoordinator Response implementation.

  V4 replaces the flat coordinator fields with a `coordinators` array, one
  entry per requested key, each with its own `error_code` / `error_message`.
  The single entry is parsed into the same `FindCoordinator.t()` as V1-V3.
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_v4_plus_response(response)
  end
end
