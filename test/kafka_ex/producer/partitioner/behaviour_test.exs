defmodule KafkaEx.Producer.PartitionerTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Producer.Partitioner

  describe "get_partitioner/0" do
    test "returns DefaultPartitioner by default" do
      # Clear any existing config
      original = Application.get_env(:kafka_ex, :partitioner)
      Application.delete_env(:kafka_ex, :partitioner)

      assert Partitioner.get_partitioner() == KafkaEx.Producer.Partitioner.Default

      # Restore original
      if original, do: Application.put_env(:kafka_ex, :partitioner, original)
    end

    test "returns configured partitioner" do
      original = Application.get_env(:kafka_ex, :partitioner)

      Application.put_env(:kafka_ex, :partitioner, TestPartitioner)
      assert Partitioner.get_partitioner() == TestPartitioner

      # Restore original
      if original do
        Application.put_env(:kafka_ex, :partitioner, original)
      else
        Application.delete_env(:kafka_ex, :partitioner)
      end
    end
  end

  describe "behaviour" do
    defmodule TestPartitioner do
      @behaviour KafkaEx.Producer.Partitioner

      @impl true
      def assign_partition(_topic, _key, _value, _partition_count) do
        # Always return first partition
        0
      end
    end

    test "custom partitioner implements callback" do
      assert TestPartitioner.assign_partition("topic", "key", "value", 10) == 0
    end
  end
end
