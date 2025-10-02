defimpl KafkaEx.New.Protocols.Kayrock.OffsetFetch.Response, for: Kayrock.OffsetFetch.V3.Response do
  @moduledoc """
  Implementation for OffsetFetch v3 Response.

  Includes throttle_time_ms field for rate limiting information.
  """

  import KafkaEx.New.Protocols.OffsetFetch.Shared,
    only: [build_response: 1, fail_fast_iterate_topics: 2, fail_fast_iterate_partitions: 3]

  alias KafkaEx.New.Structs.Error, as: ErrorStruct

  def parse_response(%{error_code: error_code, responses: _responses}) when error_code != 0 do
    {:error, ErrorStruct.build(error_code, %{})}
  end

  def parse_response(%{error_code: 0, responses: responses, throttle_time_ms: _throttle_time}) do
    # Note: throttle_time_ms is available but not currently used in the response
    # Future enhancement: Add throttle_time to response metadata
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
