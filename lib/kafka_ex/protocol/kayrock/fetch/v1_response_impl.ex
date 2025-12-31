defimpl KafkaEx.Protocol.Kayrock.Fetch.Response, for: Kayrock.Fetch.V1.Response do
  alias KafkaEx.Protocol.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn response, _partition_resp ->
      # V1 adds throttle_time_ms at top level
      [throttle_time_ms: Map.get(response, :throttle_time_ms, 0)]
    end)
  end
end
