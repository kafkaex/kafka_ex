defimpl KafkaEx.New.Protocols.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V3.Response do
  @moduledoc """
  Implementation for OffsetCommit v3 Response.

  Includes throttle_time_ms field for rate limiting information.
  Response only contains partition and error_code (no offset).
  """

  import KafkaEx.New.Protocols.Kayrock.ResponseHelpers,
    only: [build_response: 1, fail_fast_iterate_topics: 2, fail_fast_iterate_partitions: 3]

  def parse_response(%{responses: responses, throttle_time_ms: _throttle_time}) do
    # Note: throttle_time_ms is available but not currently used in the response
    # Future enhancement: Add throttle_time to response metadata
    responses
    |> fail_fast_iterate_topics(&parse_partition_responses/2)
    |> build_response()
  end

  defp parse_partition_responses(topic, partition_responses) do
    fail_fast_iterate_partitions(partition_responses, topic, &build_commit_result/2)
  end

  defp build_commit_result(topic, %{partition: partition, error_code: 0}) do
    data = %{partition: partition, error_code: :no_error}
    {:ok, KafkaEx.New.Kafka.Offset.from_list_offset(topic, [data])}
  end

  defp build_commit_result(topic, %{error_code: error_code, partition: partition}) do
    {:error, {error_code, topic, partition}}
  end
end
