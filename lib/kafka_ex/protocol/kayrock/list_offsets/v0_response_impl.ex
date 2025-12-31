defimpl KafkaEx.Protocol.Kayrock.ListOffsets.Response, for: Kayrock.ListOffsets.V0.Response do
  @moduledoc """
  Implementation for ListOffsets V0 Response.

  V0 uses an `offsets` array and returns the first offset (or 0 if empty).
  """

  alias KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v0_offset/2)
  end
end
