defimpl KafkaEx.New.Protocols.Kayrock.Fetch.Response, for: Kayrock.Fetch.V0.Response do
  alias KafkaEx.New.Protocols.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn _response, _partition_resp ->
      # V0 has no additional fields
      []
    end)
  end
end
