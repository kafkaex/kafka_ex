defmodule KafkaEx.New.Kafka.SyncGroupTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Kafka.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment
  alias KafkaEx.New.Kafka.SyncGroup

  describe "build/1" do
    test "builds sync_group with throttle_time_ms" do
      result = SyncGroup.build(throttle_time_ms: 100)

      assert %SyncGroup{throttle_time_ms: 100} = result
    end

    test "builds sync_group with zero throttle_time_ms" do
      result = SyncGroup.build(throttle_time_ms: 0)

      assert result.throttle_time_ms == 0
    end

    test "builds sync_group with nil when throttle_time_ms not provided" do
      result = SyncGroup.build()

      assert result.throttle_time_ms == nil
    end

    test "builds sync_group with partition_assignments" do
      partition_assignments = [
        %PartitionAssignment{topic: "test-topic", partitions: [0, 1, 2]}
      ]

      result = SyncGroup.build(partition_assignments: partition_assignments)

      assert %SyncGroup{partition_assignments: ^partition_assignments} = result
    end

    test "builds sync_group with empty partition_assignments when not provided" do
      result = SyncGroup.build()

      assert result.partition_assignments == []
    end

    test "builds sync_group with both throttle_time_ms and partition_assignments" do
      partition_assignments = [
        %PartitionAssignment{topic: "test-topic", partitions: [0, 1]}
      ]

      result =
        SyncGroup.build(throttle_time_ms: 50, partition_assignments: partition_assignments)

      assert %SyncGroup{throttle_time_ms: 50, partition_assignments: ^partition_assignments} =
               result
    end

    test "builds sync_group ignoring extra options" do
      result = SyncGroup.build(throttle_time_ms: 50, extra: "ignored")

      assert %SyncGroup{throttle_time_ms: 50} = result
      refute Map.has_key?(result, :extra)
    end
  end
end
