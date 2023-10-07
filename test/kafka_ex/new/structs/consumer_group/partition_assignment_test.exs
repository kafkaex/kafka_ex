defmodule KafkaEx.New.ConsumerGroup.Member.MemberAssignment.PartitionAssignmentTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.ConsumerGroup.Member.MemberAssignment.PartitionAssignment

  describe "from_describe_group_response/1" do
    test "builds a struct from a describe group response" do
      response = %{topic: "test", partitions: [0, 1, 2]}
      expected = %PartitionAssignment{topic: "test", partitions: [0, 1, 2]}

      assert expected ==
               PartitionAssignment.from_describe_group_response(response)
    end
  end
end
