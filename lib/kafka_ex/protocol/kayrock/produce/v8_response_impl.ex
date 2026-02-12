defimpl KafkaEx.Protocol.Kayrock.Produce.Response, for: Kayrock.Produce.V8.Response do
  @moduledoc """
  Implementation for Produce V8 Response.

  V8 adds per-partition error diagnostics (KIP-467):
  - `record_errors` - Array of `{batch_index, batch_index_error_message}` for
    individual records that failed within a batch
  - `error_message` - A human-readable error message for the partition

  These fields are parsed by Kayrock but are not currently exposed in the
  `RecordMetadata` domain struct. They are only populated when the partition
  has a non-zero error code, so the error path handles them implicitly
  via the standard error code mechanism.

  All other fields remain the same as V5-V7:
  - `base_offset` - The offset assigned to the first message
  - `log_append_time` - Timestamp assigned by broker (or -1 if CreateTime)
  - `log_start_offset` - The start offset of the log
  - `throttle_time_ms` - Time in ms the request was throttled
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
