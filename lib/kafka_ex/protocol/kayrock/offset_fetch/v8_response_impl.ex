defimpl KafkaEx.Protocol.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V8.Response do
  @moduledoc """
  Implementation for OffsetFetch V8 Response.

  V8 nests results under a `groups` array; the top-level `error_code` moves
  to a per-group `error_code`. KafkaEx sends one group, so the result is the
  same flat `[KafkaEx.Messages.Offset.t()]` as for earlier versions.
  """

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_grouped_response(response)
  end
end
