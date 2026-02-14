defimpl KafkaEx.Protocol.Kayrock.LeaveGroup.Response, for: Any do
  @moduledoc """
  Fallback implementation of LeaveGroup Response protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V4) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `members` key: V3+ path (parse_v3_plus_response -- batch leave with per-member errors)
  - Has `throttle_time_ms` key: V1/V2 path (parse_v1_v2_response)
  - Otherwise: V0 path (parse_v0_response -- returns {:ok, :no_error})
  """

  alias KafkaEx.Protocol.Kayrock.LeaveGroup.ResponseHelpers

  def parse_response(response) do
    cond do
      Map.has_key?(response, :members) ->
        ResponseHelpers.parse_v3_plus_response(response)

      Map.has_key?(response, :throttle_time_ms) ->
        ResponseHelpers.parse_v1_v2_response(response)

      true ->
        ResponseHelpers.parse_v0_response(response)
    end
  end
end
