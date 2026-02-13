defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V5.Response do
  @moduledoc """
  Implementation for OffsetFetch V5 Response.

  V5 adds `committed_leader_epoch` per partition in the response.
  This field is mapped to `leader_epoch` in the `PartitionOffset` domain struct.
  Includes top-level error_code field for broker-level errors.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response_with_top_level_error(response)
  end
end
