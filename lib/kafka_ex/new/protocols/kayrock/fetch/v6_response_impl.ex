defimpl KafkaEx.New.Protocols.Kayrock.Fetch.Response, for: Kayrock.Fetch.V6.Response do
  alias KafkaEx.New.Protocols.Kayrock.Fetch.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn response, partition_resp ->
      partition_header = Map.get(partition_resp, :partition_header, %{})

      # V6 same as V5
      [
        throttle_time_ms: Map.get(response, :throttle_time_ms, 0),
        last_stable_offset: Map.get(partition_header, :last_stable_offset),
        log_start_offset: Map.get(partition_header, :log_start_offset),
        aborted_transactions: Map.get(partition_header, :aborted_transactions)
      ]
    end)
  end
end
