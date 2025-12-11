defimpl KafkaEx.New.Protocols.Kayrock.OffsetCommit.Response, for: Kayrock.OffsetCommit.V2.Response do
  @moduledoc """
  Implementation for OffsetCommit v2 Response.

  Parses response from Kafka-based offset commit (recommended version).
  Response only contains partition and error_code (no offset).
  """

  import KafkaEx.New.Protocols.Kayrock.ResponseHelpers,
    only: [build_response: 1, fail_fast_iterate_topics: 2, fail_fast_iterate_partitions: 3]

  def parse_response(%{responses: responses}) do
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
