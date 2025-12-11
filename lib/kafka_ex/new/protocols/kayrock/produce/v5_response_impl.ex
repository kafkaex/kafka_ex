defimpl KafkaEx.New.Protocols.Kayrock.Produce.Response, for: Kayrock.Produce.V5.Response do
  @moduledoc """
  Implementation for Produce V5 Response.

  V5 response adds:
  - `log_start_offset` - The start offset of the log at the time this produce
    request was processed

  All fields:
  - `base_offset` - The offset assigned to the first message
  - `log_append_time` - Timestamp assigned by the broker (or -1)
  - `throttle_time_ms` - Time in ms the request was throttled
  - `log_start_offset` - The log start offset
  """

  alias KafkaEx.New.Protocols.Kayrock.Produce.ResponseHelpers

  def parse_response(response) do
    with {:ok, topic, partition_resp} <- ResponseHelpers.extract_first_partition_response(response),
         {:ok, partition_resp} <- ResponseHelpers.check_error(topic, partition_resp) do
      record_metadata =
        ResponseHelpers.build_record_metadata(
          topic: topic,
          partition: partition_resp.partition,
          base_offset: partition_resp.base_offset,
          log_append_time: partition_resp.log_append_time,
          log_start_offset: partition_resp.log_start_offset,
          throttle_time_ms: response.throttle_time_ms
        )

      {:ok, record_metadata}
    else
      {:error, :empty_response} -> ResponseHelpers.empty_response_error()
      {:error, _} = error -> error
    end
  end
end
