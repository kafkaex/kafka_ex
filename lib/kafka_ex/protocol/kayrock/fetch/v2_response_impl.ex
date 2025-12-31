defimpl KafkaEx.Protocol.Kayrock.Fetch.Response, for: Kayrock.Fetch.V2.Response do
  alias KafkaEx.Protocol.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn response, _partition_resp ->
      # V2 same as V1
      [throttle_time_ms: Map.get(response, :throttle_time_ms, 0)]
    end)
  end
end
