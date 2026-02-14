defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Response, for: Any do
  @moduledoc """
  Fallback implementation of ListOffsets Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V5) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Extracts all available response fields dynamically by checking whether
  they exist in the partition response structures:
  - `offsets` array present: V0 path
  - `leader_epoch` present: V4+ path
  - `timestamp` present: V2+ path
  - Otherwise: V1 path
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn topic, partition_resp ->
      cond do
        Map.has_key?(partition_resp, :offsets) ->
          ResponseHelpers.extract_v0_offset(topic, partition_resp)

        Map.has_key?(partition_resp, :leader_epoch) ->
          ResponseHelpers.extract_v4_offset(topic, partition_resp)

        Map.has_key?(partition_resp, :timestamp) ->
          ResponseHelpers.extract_v2_offset(topic, partition_resp)

        true ->
          ResponseHelpers.extract_v1_offset(topic, partition_resp)
      end
    end)
  end
end
