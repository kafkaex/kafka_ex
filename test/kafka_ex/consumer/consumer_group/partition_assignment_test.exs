defmodule KafkaEx.Consumer.ConsumerGroup.PartitionAssignmentTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Consumer.ConsumerGroup.PartitionAssignment

  describe "round_robin/2" do
    test "assigns partitions evenly to members" do
      members = ["member1", "member2"]
      partitions = [{"topic", 0}, {"topic", 1}, {"topic", 2}, {"topic", 3}]

      assignments = PartitionAssignment.round_robin(members, partitions)

      assert assignments["member1"] == [{"topic", 0}, {"topic", 2}]
      assert assignments["member2"] == [{"topic", 1}, {"topic", 3}]
    end

    test "handles single member" do
      members = ["member1"]
      partitions = [{"topic", 0}, {"topic", 1}, {"topic", 2}]

      assignments = PartitionAssignment.round_robin(members, partitions)

      assert assignments["member1"] == [{"topic", 0}, {"topic", 1}, {"topic", 2}]
    end

    test "handles more members than partitions" do
      members = ["member1", "member2", "member3"]
      partitions = [{"topic", 0}, {"topic", 1}]

      assignments = PartitionAssignment.round_robin(members, partitions)

      assert assignments["member1"] == [{"topic", 0}]
      assert assignments["member2"] == [{"topic", 1}]
      refute Map.has_key?(assignments, "member3")
    end

    test "handles empty partitions" do
      members = ["member1", "member2"]
      partitions = []

      assignments = PartitionAssignment.round_robin(members, partitions)

      assert assignments == %{}
    end

    test "handles multiple topics" do
      members = ["member1", "member2"]
      partitions = [{"topic1", 0}, {"topic1", 1}, {"topic2", 0}, {"topic2", 1}]

      assignments = PartitionAssignment.round_robin(members, partitions)

      assert assignments["member1"] == [{"topic1", 0}, {"topic2", 0}]
      assert assignments["member2"] == [{"topic1", 1}, {"topic2", 1}]
    end

    test "distributes uneven partitions" do
      members = ["m1", "m2", "m3"]
      partitions = [{"t", 0}, {"t", 1}, {"t", 2}, {"t", 3}, {"t", 4}]

      assignments = PartitionAssignment.round_robin(members, partitions)

      assert assignments["m1"] == [{"t", 0}, {"t", 3}]
      assert assignments["m2"] == [{"t", 1}, {"t", 4}]
      assert assignments["m3"] == [{"t", 2}]
    end

    test "preserves partition order within member assignment" do
      members = ["member1"]
      partitions = [{"a", 0}, {"b", 1}, {"c", 2}]

      assignments = PartitionAssignment.round_robin(members, partitions)

      assert assignments["member1"] == [{"a", 0}, {"b", 1}, {"c", 2}]
    end
  end
end
