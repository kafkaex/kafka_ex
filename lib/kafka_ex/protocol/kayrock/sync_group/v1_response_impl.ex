defimpl KafkaEx.Protocol.Kayrock.SyncGroup.Response, for: Kayrock.SyncGroup.V1.Response do
  @moduledoc """
  Implementation for SyncGroup v1 Response.

  V1 adds throttle_time_ms to the response compared to v0.
  """

  alias Kayrock.ErrorCode
  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.SyncGroup
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment

  def parse_response(%Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: throttle_time_ms,
        error_code: error_code,
        member_assignment: member_assignment
      }) do
    case ErrorCode.code_to_atom(error_code) do
      :no_error ->
        partition_assignments = extract_partition_assignments(member_assignment)
        {:ok, SyncGroup.build(throttle_time_ms: throttle_time_ms, partition_assignments: partition_assignments)}

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
