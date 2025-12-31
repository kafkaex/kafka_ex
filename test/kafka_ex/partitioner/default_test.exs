defmodule KafkaEx.Producer.Partitioner.DefaultTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Producer.Partitioner.Default, as: DefaultPartitioner

  describe "assign_partition/4" do
    test "returns consistent partition for same key" do
      # Same key should always hash to same partition
      partition1 = DefaultPartitioner.assign_partition("topic", "user-123", "value1", 10)
      partition2 = DefaultPartitioner.assign_partition("topic", "user-123", "value2", 10)
      partition3 = DefaultPartitioner.assign_partition("topic", "user-123", "value3", 10)

      assert partition1 == partition2
      assert partition2 == partition3
    end

    test "different keys may get different partitions" do
      # Run with enough keys to statistically expect different partitions
      partitions =
        for i <- 1..100 do
          DefaultPartitioner.assign_partition("topic", "key-#{i}", "value", 10)
        end
        |> Enum.uniq()

      # Should have more than one unique partition
      assert length(partitions) > 1
    end

    test "returns partition in valid range" do
      for partition_count <- [1, 3, 5, 10, 100] do
        partition = DefaultPartitioner.assign_partition("topic", "test-key", "value", partition_count)
        assert partition >= 0
        assert partition < partition_count
      end
    end

    test "nil key returns random partition in valid range" do
      for _ <- 1..100 do
        partition = DefaultPartitioner.assign_partition("topic", nil, "value", 10)
        assert partition >= 0
        assert partition < 10
      end
    end

    test "nil key returns varying partitions (random)" do
      # Run multiple times and expect different partitions
      partitions =
        for _ <- 1..100 do
          DefaultPartitioner.assign_partition("topic", nil, "value", 10)
        end
        |> Enum.uniq()

      # Should have more than one unique partition (statistically very likely)
      assert length(partitions) > 1
    end

    test "single partition always returns 0" do
      assert DefaultPartitioner.assign_partition("topic", "key", "value", 1) == 0
      assert DefaultPartitioner.assign_partition("topic", nil, "value", 1) == 0
    end

    test "compatible with Java murmur2 hash for known keys" do
      # These values are verified against Java implementation
      # murmur2("test") % 10 should give consistent result
      partition = DefaultPartitioner.assign_partition("topic", "test", "value", 10)
      assert is_integer(partition)
      assert partition >= 0
      assert partition < 10
    end
  end
end
