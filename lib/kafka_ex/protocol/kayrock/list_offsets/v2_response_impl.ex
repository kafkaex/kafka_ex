defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Response, for: Kayrock.ListOffsets.V2.Response do
  @moduledoc """
  Implementation for ListOffsets V2 Response.

  V2 uses a single `offset` field with `timestamp`.
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v2_offset/2)
  end
end
