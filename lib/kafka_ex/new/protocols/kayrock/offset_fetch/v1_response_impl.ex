defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V1.Response do
  @moduledoc """
  Implementation for OffsetFetch v1 Response.

  Parses response from coordinator-based Kafka offset storage.
  """

  import KafkaEx.New.Protocols.OffsetFetch.Shared,
    only: [build_response: 1, fail_fast_iterate_topics: 2, fail_fast_iterate_partitions: 3]

  def parse_response(%{responses: responses}) do
    responses
    |> fail_fast_iterate_topics(&parse_partition_responses/2)
    |> build_response()
  end

  defp parse_partition_responses(topic, partition_responses) do
    fail_fast_iterate_partitions(partition_responses, topic, &build_offset/2)
  end

  defp build_offset(topic, %{partition: partition, error_code: 0, offset: offset, metadata: metadata}) do
    data = %{
      partition: partition,
      offset: offset,
      error_code: :no_error,
      metadata: metadata || ""
    }

    {:ok, KafkaEx.New.Structs.Offset.from_list_offset(topic, [data])}
  end

  defp build_offset(topic, %{error_code: error_code, partition: partition}) do
    {:error, {error_code, topic, partition}}
  end
end
