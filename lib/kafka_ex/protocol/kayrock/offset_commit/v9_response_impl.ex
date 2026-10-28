defimpl KafkaEx.Protocol.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V9.Response do
  @moduledoc """
  Implementation for OffsetCommit V9 Response.

  Same structure as V8. V9 can additionally return `STALE_MEMBER_EPOCH`
  (consumer-protocol groups) and `GROUP_ID_NOT_FOUND`, which surface as
  `:stale_member_epoch` / `:group_id_not_found`.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response)
  end
end
