defimpl KafkaEx.New.Protocols.Kayrock.Produce.Response, for: Kayrock.Produce.V3.Response do
  @moduledoc """
  Implementation for Produce V3 Response.

  V3 response has the same fields as V2:
  - `base_offset` - The offset assigned to the first message
  - `log_append_time` - Timestamp assigned by the broker (or -1)
  - `throttle_time_ms` - Time in ms the request was throttled

  Does not include log_start_offset.
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
          throttle_time_ms: response.throttle_time_ms
        )

      {:ok, record_metadata}
    else
      {:error, :empty_response} -> ResponseHelpers.empty_response_error()
      {:error, _} = error -> error
    end
  end
end
