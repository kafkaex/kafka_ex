defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V9.Response do
  @moduledoc """
  Implementation for OffsetFetch V9 Response.

  Same structure as V8. V9 may additionally return `STALE_MEMBER_EPOCH` and
  `UNKNOWN_MEMBER_ID` for consumer-protocol groups, surfaced as the matching
  error atoms.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_grouped_response(response)
  end
end
