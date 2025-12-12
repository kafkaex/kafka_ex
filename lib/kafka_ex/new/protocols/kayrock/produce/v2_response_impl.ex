defimpl KafkaEx.New.Protocols.Kayrock.Produce.Response, for: Kayrock.Produce.V2.Response do
  @moduledoc """
  Implementation for Produce V2 Response.

  V2 adds timestamp support to MessageSet format:
  - `log_append_time` - Timestamp assigned by broker (or -1 if CreateTime)
  - `throttle_time_ms` - Time in ms the request was throttled
  """

  alias KafkaEx.New.Protocols.Kayrock.Produce.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn resp, partition_resp ->
      [
        log_append_time: partition_resp.log_append_time,
        throttle_time_ms: resp.throttle_time_ms
      ]
    end)
  end
end
