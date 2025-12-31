defmodule KafkaEx.Producer.Partitioner.LegacyTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Producer.Partitioner.Legacy, as: LegacyPartitioner

  describe "assign_partition/4" do
    test "returns consistent partition for same key" do
      # Same key should always hash to same partition
      partition1 = LegacyPartitioner.assign_partition("topic", "user-123", "value1", 10)
      partition2 = LegacyPartitioner.assign_partition("topic", "user-123", "value2", 10)
      partition3 = LegacyPartitioner.assign_partition("topic", "user-123", "value3", 10)

      assert partition1 == partition2
      assert partition2 == partition3
    end

    test "returns partition in valid range" do
      for partition_count <- [1, 3, 5, 10, 100] do
        partition = LegacyPartitioner.assign_partition("topic", "test-key", "value", partition_count)
        assert partition >= 0
        assert partition < partition_count
      end
    end

    test "nil key returns random partition in valid range" do
      for _ <- 1..100 do
        partition = LegacyPartitioner.assign_partition("topic", nil, "value", 10)
        assert partition >= 0
        assert partition < 10
      end
    end

    test "single partition always returns 0" do
      assert LegacyPartitioner.assign_partition("topic", "key", "value", 1) == 0
      assert LegacyPartitioner.assign_partition("topic", nil, "value", 1) == 0
    end

    test "may differ from DefaultPartitioner for same key" do
      # Legacy and Default partitioners use different hash masking
      # They may produce different results for the same key
      legacy = LegacyPartitioner.assign_partition("topic", "test-key", "value", 100)
      default = KafkaEx.Producer.Partitioner.Default.assign_partition("topic", "test-key", "value", 100)

      # They could be the same or different - we just verify both are valid
      assert legacy >= 0 and legacy < 100
      assert default >= 0 and default < 100
    end
  end
end
