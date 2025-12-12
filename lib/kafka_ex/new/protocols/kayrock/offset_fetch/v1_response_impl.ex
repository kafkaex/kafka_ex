defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V1.Response do
  @moduledoc """
  Implementation for OffsetFetch V1 Response.

  V1 uses coordinator-based Kafka offset storage.
  Does not include a top-level error_code field.
  """

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response_without_top_level_error(response)
  end
end
