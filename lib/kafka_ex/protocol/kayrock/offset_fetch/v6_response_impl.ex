defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V6.Response do
  @moduledoc """
  Implementation for OffsetFetch V6 Response.

  V6 is a flexible version (KIP-482) -- uses compact strings, compact arrays,
  and tagged fields. Kayrock handles the decoding transparently.

  Like V5, the response includes `committed_leader_epoch` per partition,
  mapped to `leader_epoch` in the `PartitionOffset` domain struct.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response_with_top_level_error(response)
  end
end
