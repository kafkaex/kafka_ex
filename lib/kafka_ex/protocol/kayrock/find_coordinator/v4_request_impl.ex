defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Request, for: Kayrock.FindCoordinator.V4.Request do
  @moduledoc """
  V4 FindCoordinator Request implementation.

  V4 adds batching (KIP-699): `key` is replaced by a `coordinator_keys`
  array. KafkaEx sends exactly one key (the group id or `:coordinator_key`)
  with `key_type` as in V1-V3.
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request, opts) do
    RequestHelpers.build_v4_plus_request(request, opts)
  end
end
