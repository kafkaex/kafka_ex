defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Response, for: Kayrock.ListOffsets.V1.Response do
  @moduledoc """
  Implementation for ListOffsets V1 Response.

  V1 uses a single `offset` field (replaces offsets array from V0).
  Does not include timestamp.
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v1_offset/2)
  end
end
