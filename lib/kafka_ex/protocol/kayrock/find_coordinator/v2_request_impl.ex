defimpl KafkaEx.Protocol.Kayrock.FindCoordinator.Request, for: Kayrock.FindCoordinator.V2.Request do
  @moduledoc """
  V2 FindCoordinator Request implementation.

  V2 is schema-identical to V1: `key: :string, key_type: :int8`.
  No new fields were added in V2.
  """

  alias KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers

  def build_request(request, opts) do
    {coordinator_key, coordinator_type} = RequestHelpers.extract_v1_fields(opts)

    %{request | key: coordinator_key, key_type: coordinator_type}
  end
end
