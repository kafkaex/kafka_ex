defmodule KafkaEx.Consumer.ConsumerGroup.AssignmentTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Consumer.ConsumerGroup.Assignment

  describe "for_members/3" do
    test "runs the strategy callback and packs each member's assignment by topic" do
      members = ["m1", "m2"]
      partitions = [{"t", 0}, {"t", 1}, {"t", 2}]

      callback = fn _members, _partitions ->
        [{"m1", [{"t", 0}, {"t", 2}]}, {"m2", [{"t", 1}]}]
      end

      assert [{"m1", [{"t", [0, 2]}]}, {"m2", [{"t", [1]}]}] =
               Assignment.for_members(members, partitions, callback)
    end

    test "a member with no assignment gets an empty list" do
      callback = fn _m, _p -> [{"m1", [{"t", 0}]}] end
      assert [{"m1", [{"t", [0]}]}, {"m2", []}] = Assignment.for_members(["m1", "m2"], [{"t", 0}], callback)
    end
  end

  describe "format_for_sync_group/1" do
    test "maps to the protocol-agnostic member/topic_partitions shape" do
      assert [%{member_id: "m1", topic_partitions: [{"t", [0, 1]}]}] =
               Assignment.format_for_sync_group([{"m1", [{"t", [0, 1]}]}])
    end
  end

  describe "from_sync_response/1" do
    test "flattens partition_assignments into {topic, partition} tuples" do
      partition_assignments = [
        %{topic: "t1", partitions: [0, 1]},
        %{topic: "t2", partitions: [3]}
      ]

      assert [{"t1", 0}, {"t1", 1}, {"t2", 3}] = Assignment.from_sync_response(partition_assignments)
    end
  end
end
