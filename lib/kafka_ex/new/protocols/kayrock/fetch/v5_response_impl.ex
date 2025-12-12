defimpl KafkaEx.New.Protocols.Kayrock.Fetch.Response, for: Kayrock.Fetch.V5.Response do
  @moduledoc """
  V5 adds log_start_offset compared to V4.
  Uses shared field extractor for V5+ responses.
  """

  alias KafkaEx.New.Protocols.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v5_plus_fields/2)
  end
end
