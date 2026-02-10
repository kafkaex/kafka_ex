defmodule KafkaEx.Messages.RecordMetadataTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.RecordMetadata

  describe "build/1" do
    test "builds record metadata with required fields" do
      result = RecordMetadata.build(topic: "test-topic", partition: 0, base_offset: 100)
      assert %RecordMetadata{topic: "test-topic", partition: 0, base_offset: 100} = result
    end

    test "builds record metadata with all fields" do
      result =
        RecordMetadata.build(
          topic: "test-topic",
          partition: 1,
          base_offset: 500,
          log_append_time: 1_234_567_890,
          log_start_offset: 0,
          throttle_time_ms: 10
        )

      assert %RecordMetadata{
               topic: "test-topic",
               partition: 1,
               base_offset: 500,
               log_append_time: 1_234_567_890,
               log_start_offset: 0,
               throttle_time_ms: 10
             } = result
    end

    test "builds record metadata with zero throttle_time_ms" do
      result = RecordMetadata.build(topic: "t", partition: 0, base_offset: 0, throttle_time_ms: 0)
      assert result.throttle_time_ms == 0
    end

    test "builds record metadata with nil optional fields when not provided" do
      result = RecordMetadata.build(topic: "t", partition: 0, base_offset: 0)

      assert result.log_append_time == nil
      assert result.log_start_offset == nil
      assert result.throttle_time_ms == nil
    end

    test "builds record metadata with negative log_append_time (broker not using LogAppendTime)" do
      result = RecordMetadata.build(topic: "t", partition: 0, base_offset: 0, log_append_time: -1)

      assert result.log_append_time == -1
    end

    test "raises when topic is missing" do
      assert_raise KeyError, ~r/key :topic not found/, fn ->
        RecordMetadata.build(partition: 0, base_offset: 0)
      end
    end

    test "raises when partition is missing" do
      assert_raise KeyError, ~r/key :partition not found/, fn ->
        RecordMetadata.build(topic: "t", base_offset: 0)
      end
    end

    test "raises when base_offset is missing" do
      assert_raise KeyError, ~r/key :base_offset not found/, fn ->
        RecordMetadata.build(topic: "t", partition: 0)
      end
    end

    test "ignores extra options" do
      result = RecordMetadata.build(topic: "t", partition: 0, base_offset: 0, extra: "ignored")
      refute Map.has_key?(result, :extra)
    end
  end

  describe "offset/1" do
    test "returns base_offset" do
      record_metadata = RecordMetadata.build(topic: "t", partition: 0, base_offset: 12345)
      assert RecordMetadata.offset(record_metadata) == 12345
    end

    test "returns zero offset" do
      record_metadata = RecordMetadata.build(topic: "t", partition: 0, base_offset: 0)
      assert RecordMetadata.offset(record_metadata) == 0
    end
  end

  describe "timestamp/1" do
    test "returns log_append_time when set" do
      record_metadata =
        RecordMetadata.build(
          topic: "t",
          partition: 0,
          base_offset: 0,
          log_append_time: 1_702_000_000_000
        )

      assert RecordMetadata.timestamp(record_metadata) == 1_702_000_000_000
    end

    test "returns nil when log_append_time is -1" do
      record_metadata =
        RecordMetadata.build(topic: "t", partition: 0, base_offset: 0, log_append_time: -1)

      assert RecordMetadata.timestamp(record_metadata) == nil
    end

    test "returns nil when log_append_time is nil" do
      record_metadata = RecordMetadata.build(topic: "t", partition: 0, base_offset: 0)
      assert RecordMetadata.timestamp(record_metadata) == nil
    end
  end
end
