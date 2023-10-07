defmodule KafkaEx.New.ConsumerGroup.Member.MemberAssignmentTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.ConsumerGroup.Member.MemberAssignment

  describe "from_describe_group_response/1" do
    test "returns a MemberAssignment struct without partitions" do
      response = %{
        version: 0,
        user_data: "user_data",
        partition_assignments: []
      }

      assert %MemberAssignment{} =
               MemberAssignment.from_describe_group_response(response)
    end

    test "returns a MemberAssignment struct with partitions" do
      response = %{
        version: 0,
        user_data: "user_data",
        partition_assignments: [
          %{
            topic: "topic",
            partitions: [0, 1]
          }
        ]
      }

      result = MemberAssignment.from_describe_group_response(response)

      assert 0 == result.version
      assert "user_data" == result.user_data

      assert result.partition_assignments == [
               %MemberAssignment.PartitionAssignment{
                 topic: "topic",
                 partitions: [0, 1]
               }
             ]
    end
  end
end
