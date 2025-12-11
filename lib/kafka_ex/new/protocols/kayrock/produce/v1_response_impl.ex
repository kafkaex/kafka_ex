defimpl KafkaEx.New.Protocols.Kayrock.Produce.Response, for: Kayrock.Produce.V1.Response do
  @moduledoc """
  Implementation for Produce V1 Response.

  V1 response adds:
  - `throttle_time_ms` - Time in ms the request was throttled due to quota violations

  Does not include log_append_time or log_start_offset.
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
          throttle_time_ms: response.throttle_time_ms
        )

      {:ok, record_metadata}
    else
      {:error, :empty_response} -> ResponseHelpers.empty_response_error()
      {:error, _} = error -> error
    end
  end
end
