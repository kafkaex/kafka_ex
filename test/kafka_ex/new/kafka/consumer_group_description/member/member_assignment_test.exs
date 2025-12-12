defmodule KafkaEx.New.Kafka.ConsumerGroupDescription.Member.MemberAssignmentTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Kafka.ConsumerGroupDescription.Member.MemberAssignment

  describe "from_describe_group_response/1" do
    test "returns a MemberAssignment struct without partitions" do
      response = %{
        version: 0,
        user_data: "user_data",
        partition_assignments: []
      }

      assert %MemberAssignment{} = MemberAssignment.from_describe_group_response(response)
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

  describe "accessor functions" do
    setup do
      assignment = %MemberAssignment{
        version: 1,
        user_data: "custom-data",
        partition_assignments: []
      }

      {:ok, %{assignment: assignment}}
    end

    test "version/1 returns version", %{assignment: assignment} do
      assert MemberAssignment.version(assignment) == 1
    end

    test "user_data/1 returns user data", %{assignment: assignment} do
      assert MemberAssignment.user_data(assignment) == "custom-data"
    end

    test "partition_assignments/1 returns partition assignments", %{assignment: assignment} do
      assert MemberAssignment.partition_assignments(assignment) == []
    end
  end
end
