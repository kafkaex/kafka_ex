defimpl KafkaEx.Protocol.Kayrock.Fetch.Response, for: Kayrock.Fetch.V7.Response do
  @moduledoc """
  V7 adds session_id/session_epoch at request level but response fields match V5/V6.
  Uses shared field extractor for V5+ responses.
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, &ResponseHelpers.extract_v5_plus_fields/2)
  end
end
