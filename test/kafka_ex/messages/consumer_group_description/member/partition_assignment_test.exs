defmodule KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignmentTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment

  describe "from_describe_group_response/1" do
    test "builds a struct from a describe group response" do
      response = %{topic: "test", partitions: [0, 1, 2]}
      expected = %PartitionAssignment{topic: "test", partitions: [0, 1, 2]}

      assert expected ==
               PartitionAssignment.from_describe_group_response(response)
    end
  end

  describe "accessor functions" do
    setup do
      assignment = %PartitionAssignment{
        topic: "my-topic",
        partitions: [0, 1, 2]
      }

      {:ok, %{assignment: assignment}}
    end

    test "topic/1 returns topic name", %{assignment: assignment} do
      assert PartitionAssignment.topic(assignment) == "my-topic"
    end

    test "partitions/1 returns partition list", %{assignment: assignment} do
      assert PartitionAssignment.partitions(assignment) == [0, 1, 2]
    end
  end
end
