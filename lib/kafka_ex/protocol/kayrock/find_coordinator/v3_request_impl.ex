defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Request, for: Kayrock.FindCoordinator.V3.Request do
  @moduledoc """
  V3 FindCoordinator Request implementation.

  V3 is the flexible version (KIP-482). The schema uses compact encodings:
  `key: :compact_string, key_type: :int8, tagged_fields: :tagged_fields`.

  The domain-relevant fields are identical to V1/V2 (key + key_type).
  Kayrock handles the compact encoding transparently.
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request, opts) do
    {coordinator_key, coordinator_type} = RequestHelpers.extract_v1_fields(opts)

    %{request | key: coordinator_key, key_type: coordinator_type}
  end
end
