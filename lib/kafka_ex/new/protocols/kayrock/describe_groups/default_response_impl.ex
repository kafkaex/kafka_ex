defimpl KafkaEx.New.Protocols.DescribeGroups.Response,
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
    %KafkaEx.New.ConsumerGroup{
      group_id: kayrock_group.group_id,
      state: kayrock_group.state,
      protocol_type: kayrock_group.protocol_type,
      protocol: kayrock_group.protocol,
      members:
        Enum.map(kayrock_group.members, fn member ->
          %KafkaEx.New.ConsumerGroup.Member{
            member_id: member.member_id,
            client_id: member.client_id,
            client_host: member.client_host,
            member_metadata: member.member_metadata,
            member_assignment:
              %KafkaEx.New.ConsumerGroup.Member.MemberAssigment{
                version: member.member_assignment.version,
                user_data: member.member_assignment.user_data,
                partition_assignments:
                  Enum.map(
                    member.member_assignment.partition_assignments,
                    fn assigment ->
                      %KafkaEx.New.ConsumerGroup.Member.MemberAssigment.PartitionAssignment{
                        topic: assigment.topic,
                        partitions: assigment.partitions
                      }
                    end
                  )
              }
          }
        end)
    }
  end
end
