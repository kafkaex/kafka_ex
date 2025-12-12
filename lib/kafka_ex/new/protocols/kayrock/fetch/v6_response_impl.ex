defimpl KafkaEx.New.Protocols.Kayrock.Fetch.Response, for: Kayrock.Fetch.V6.Response do
  @moduledoc """
  V6 has the same response structure as V5.
  Uses shared field extractor for V5+ responses.
  """

  alias KafkaEx.New.Protocols.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v5_plus_fields/2)
  end
end
