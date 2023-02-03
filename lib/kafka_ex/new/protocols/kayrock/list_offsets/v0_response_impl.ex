defimpl KafkaEx.New.Protocols.ListOffsets.Response,
  for: Kayrock.ListOffsets.V0.Response do
  def get_partition_offset(%{responses: responses}, topic_name, partition_id) do
    responses
    |> find_topic(topic_name)
    |> find_partition(partition_id)
    |> parse_partition_response()
  end

  defp find_topic(responses, topic_name) do
    case Enum.find(responses, &(&1.topic == topic_name)) do
      nil -> {:error, :topic_not_found}
      topic_data -> topic_data
    end
  end

  defp find_partition({:error, error_code}, _), do: {:error, error_code}

  defp find_partition(%{partition_responses: partition_responses}, partition_id) do
    case Enum.find(partition_responses, &(&1.partition == partition_id)) do
      nil -> {:error, :partition_not_found}
      value -> value
    end
  end

  defp parse_partition_response({:error, error_code}), do: {:error, error_code}

  defp parse_partition_response(%{error_code: 0, offsets: offsets}),
    do: {:ok, Enum.max(offsets)}

  defp parse_partition_response(%{error_code: error_code}),
    do: {:error, Kayrock.ErrorCode.code_to_atom(error_code)}
end
