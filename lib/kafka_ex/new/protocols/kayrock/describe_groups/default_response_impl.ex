defimpl KafkaEx.New.Protocols.Kayrock.DescribeGroups.Response,
  for: [Kayrock.DescribeGroups.V0.Response, Kayrock.DescribeGroups.V1.Response] do
  def parse_response(%{groups: groups}) do
    case Enum.filter(groups, &(&1.error_code != 0)) do
      [] ->
        groups = Enum.map(groups, &build_consumer_group/1)
        {:ok, groups}

      errors ->
        error_list =
          Enum.map(errors, fn %{group_id: group_id, error_code: error_code} ->
            {group_id, Kayrock.ErrorCode.code_to_atom!(error_code)}
          end)

        {:error, error_list}
    end
  end

  defp build_consumer_group(kayrock_group) do
    KafkaEx.New.Structs.ConsumerGroup.from_describe_group_response(kayrock_group)
  end
end
