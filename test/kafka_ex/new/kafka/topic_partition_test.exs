defmodule KafkaEx.New.Kafka.TopicPartitionTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Kafka.TopicPartition

  describe "new/2" do
    test "creates topic-partition with valid inputs" do
      result = TopicPartition.new("my-topic", 0)
      assert %TopicPartition{topic: "my-topic", partition: 0} = result
    end

    test "creates topic-partition with non-zero partition" do
      result = TopicPartition.new("events", 5)
      assert result.topic == "events"
      assert result.partition == 5
    end

    test "creates topic-partition with large partition number" do
      result = TopicPartition.new("topic", 999)
      assert result.partition == 999
    end

    test "handles topic name with dashes" do
      result = TopicPartition.new("my-topic-name", 0)
      assert result.topic == "my-topic-name"
    end

    test "handles topic name with dots" do
      result = TopicPartition.new("my.topic.name", 0)
      assert result.topic == "my.topic.name"
    end

    test "handles topic name with underscores" do
      result = TopicPartition.new("my_topic_name", 0)
      assert result.topic == "my_topic_name"
    end

    test "handles empty topic name" do
      result = TopicPartition.new("", 0)
      assert result.topic == ""
    end

    test "raises FunctionClauseError for negative partition" do
      assert_raise FunctionClauseError, fn ->
        TopicPartition.new("topic", -1)
      end
    end

    test "raises FunctionClauseError for non-string topic" do
      assert_raise FunctionClauseError, fn ->
        TopicPartition.new(:topic, 0)
      end
    end

    test "raises FunctionClauseError for non-integer partition" do
      assert_raise FunctionClauseError, fn ->
        TopicPartition.new("topic", "0")
      end
    end
  end

  describe "build/1" do
    test "builds topic-partition from keyword opts" do
      result = TopicPartition.build(topic: "orders", partition: 1)
      assert %TopicPartition{topic: "orders", partition: 1} = result
    end

    test "raises KeyError when topic is missing" do
      assert_raise KeyError, ~r/key :topic not found/, fn ->
        TopicPartition.build(partition: 0)
      end
    end

    test "raises KeyError when partition is missing" do
      assert_raise KeyError, ~r/key :partition not found/, fn ->
        TopicPartition.build(topic: "topic")
      end
    end

    test "ignores extra options" do
      result = TopicPartition.build(topic: "t", partition: 0, extra: "ignored")
      assert result.topic == "t"
      assert result.partition == 0
      refute Map.has_key?(result, :extra)
    end
  end

  describe "to_tuple/1" do
    test "converts to tuple format" do
      tp = TopicPartition.new("orders", 3)
      assert TopicPartition.to_tuple(tp) == {"orders", 3}
    end

    test "converts with partition 0" do
      tp = TopicPartition.new("events", 0)
      assert TopicPartition.to_tuple(tp) == {"events", 0}
    end
  end

  describe "from_tuple/1" do
    test "creates from tuple format" do
      result = TopicPartition.from_tuple({"orders", 3})
      assert %TopicPartition{topic: "orders", partition: 3} = result
    end

    test "creates from tuple with partition 0" do
      result = TopicPartition.from_tuple({"events", 0})
      assert result.topic == "events"
      assert result.partition == 0
    end

    test "raises FunctionClauseError for invalid tuple format" do
      assert_raise FunctionClauseError, fn ->
        TopicPartition.from_tuple({:topic, 0})
      end
    end
  end

  describe "round-trip conversion" do
    test "to_tuple/1 -> from_tuple/1 preserves data" do
      original = TopicPartition.new("my-topic", 7)
      tuple = TopicPartition.to_tuple(original)
      restored = TopicPartition.from_tuple(tuple)

      assert restored.topic == original.topic
      assert restored.partition == original.partition
    end

    test "from_tuple/1 -> to_tuple/1 preserves data" do
      original_tuple = {"events", 12}
      tp = TopicPartition.from_tuple(original_tuple)
      result_tuple = TopicPartition.to_tuple(tp)

      assert result_tuple == original_tuple
    end
  end

  describe "struct behavior" do
    test "can pattern match on struct" do
      tp = TopicPartition.new("topic", 5)
      assert %TopicPartition{topic: "topic", partition: 5} = tp
    end

    test "can access fields directly" do
      tp = TopicPartition.new("events", 3)
      assert tp.topic == "events"
      assert tp.partition == 3
    end

    test "can be used in map operations" do
      tp = TopicPartition.new("topic", 0)
      updated = %{tp | partition: 1}
      assert updated.partition == 1
      assert updated.topic == "topic"
    end
  end
end
