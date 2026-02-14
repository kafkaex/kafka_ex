defimpl KafkaEx.Protocol.Kayrock.Produce.Response, for: Kayrock.Produce.V7.Response do
  @moduledoc """
  Implementation for Produce V7 Response.

  V7 response has the same schema as V5/V6:
  - `base_offset` - The offset assigned to the first message
  - `log_append_time` - Timestamp assigned by broker (or -1 if CreateTime)
  - `log_start_offset` - The start offset of the log
  - `throttle_time_ms` - Time in ms the request was throttled

  No new fields compared to V6.
  """

  alias KafkaEx.Protocol.Kayrock.Produce.ResponseHelpers

  def parse_response(response) do
    ResponseHelpers.parse_response(response, fn resp, partition_resp ->
      [
        log_append_time: partition_resp.log_append_time,
        log_start_offset: partition_resp.log_start_offset,
        throttle_time_ms: resp.throttle_time_ms
      ]
    end)
  end
end
