defimpl KafkaEx.New.Protocols.Kayrock.Produce.Response, for: Kayrock.Produce.V0.Response do
  @moduledoc """
  Implementation for Produce V0 Response.

  V0 response only includes:
  - `base_offset` - The offset assigned to the first message
  - `error_code` - Error code (0 = success)

  No log_append_time, throttle_time_ms, or log_start_offset.
  """

  alias KafkaEx.New.Protocols.Kayrock.Produce.ResponseHelpers

  def parse_response(response) do
    with {:ok, topic, partition_resp} <- ResponseHelpers.extract_first_partition_response(response),
         {:ok, partition_resp} <- ResponseHelpers.check_error(topic, partition_resp) do
      record_metadata =
        ResponseHelpers.build_record_metadata(
          topic: topic,
          partition: partition_resp.partition,
          base_offset: partition_resp.base_offset
        )

      {:ok, record_metadata}
    else
      {:error, :empty_response} -> ResponseHelpers.empty_response_error()
      {:error, _} = error -> error
    end
  end
end
