defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V2.Response do
  @moduledoc """
  Implementation for OffsetFetch V2 Response.

  V2 adds top-level error_code field for broker-level errors.
  """

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response_with_top_level_error(response)
  end
end
