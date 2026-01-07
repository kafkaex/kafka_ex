defimpl KafkaEx.Protocol.Kayrock.Produce.Response, for: Kayrock.Produce.V3.Response do
  @moduledoc """
  Implementation for Produce V3 Response.

  V3 uses RecordBatch format (replaces MessageSet), adds headers support.
  Response structure same as V2:
  - `log_append_time` - Timestamp assigned by broker (or -1 if CreateTime)
  - `throttle_time_ms` - Time in ms the request was throttled
  """

  alias KafkaEx.Protocol.Kayrock.Produce.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn resp, partition_resp ->
      [
        log_append_time: partition_resp.log_append_time,
        throttle_time_ms: resp.throttle_time_ms
      ]
    end)
  end
end
