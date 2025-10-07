defimpl KafkaEx.New.Protocols.Kayrock.SyncGroup.Response, for: Kayrock.SyncGroup.V0.Response do
  @moduledoc """
  Implementation for SyncGroup v0 Response.
  """

  alias Kayrock.ErrorCode
  alias KafkaEx.New.Structs.Error
  alias KafkaEx.New.Structs.SyncGroup
  alias KafkaEx.New.Structs.ConsumerGroup.Member.MemberAssignment.PartitionAssignment

  def parse_response(%Kayrock.SyncGroup.V0.Response{error_code: error_code, member_assignment: member_assignment}) do
    case ErrorCode.code_to_atom(error_code) do
      :no_error ->
        partition_assignments = extract_partition_assignments(member_assignment)

        {:ok, SyncGroup.build(throttle_time_ms: nil, partition_assignments: partition_assignments)}

      error_atom ->
        {:error, Error.build(error_atom, %{})}
    end
  end

  defp extract_partition_assignments(%Kayrock.MemberAssignment{partition_assignments: kayrock_assignments}) do
    Enum.map(kayrock_assignments, fn %Kayrock.MemberAssignment.PartitionAssignment{topic: topic, partitions: partitions} ->
      %PartitionAssignment{
        topic: topic,
        partitions: partitions
      }
    end)
  end

  defp extract_partition_assignments(_), do: []
end
