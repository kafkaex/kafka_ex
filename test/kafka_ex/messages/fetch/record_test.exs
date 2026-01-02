defmodule KafkaEx.Messages.Fetch.RecordTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Messages.Header
  alias KafkaEx.Cluster.TopicPartition

  describe "build/1" do
    test "builds record with required offset" do
      record = Record.build(offset: 100)

      assert %Record{} = record
      assert record.offset == 100
    end

    test "builds record with all fields" do
      record =
        Record.build(
          topic: "test-topic",
          partition: 0,
          offset: 100,
          key: "key",
          value: "value",
          timestamp: 1_234_567_890,
          timestamp_type: :create_time,
          headers: [%Header{key: "h1", value: "v1"}],
          leader_epoch: 5
        )

      assert record.topic == "test-topic"
      assert record.partition == 0
      assert record.offset == 100
      assert record.key == "key"
      assert record.value == "value"
      assert record.timestamp == 1_234_567_890
      assert record.timestamp_type == :create_time
      assert length(record.headers) == 1
      assert record.leader_epoch == 5
    end

    test "computes serialized_key_size from key" do
      record = Record.build(offset: 0, key: "hello")

      assert record.serialized_key_size == 5
    end

    test "computes serialized_value_size from value" do
      record = Record.build(offset: 0, value: "hello world")

      assert record.serialized_value_size == 11
    end

    test "sets serialized size to -1 for nil key/value" do
      record = Record.build(offset: 0, key: nil, value: nil)

      assert record.serialized_key_size == -1
      assert record.serialized_value_size == -1
    end

    test "allows explicit serialized sizes" do
      record = Record.build(offset: 0, serialized_key_size: 10, serialized_value_size: 20)

      assert record.serialized_key_size == 10
      assert record.serialized_value_size == 20
    end

    test "raises on missing offset" do
      assert_raise KeyError, fn ->
        Record.build(key: "test")
      end
    end
  end

  describe "has_value?/1" do
    test "returns false for nil value" do
      record = Record.build(offset: 0, value: nil)

      assert Record.has_value?(record) == false
    end

    test "returns true for non-nil value" do
      record = Record.build(offset: 0, value: "data")

      assert Record.has_value?(record) == true
    end

    test "returns true for empty string value" do
      record = Record.build(offset: 0, value: "")

      assert Record.has_value?(record) == true
    end
  end

  describe "has_key?/1" do
    test "returns false for nil key" do
      record = Record.build(offset: 0, key: nil)

      assert Record.has_key?(record) == false
    end

    test "returns true for non-nil key" do
      record = Record.build(offset: 0, key: "my-key")

      assert Record.has_key?(record) == true
    end

    test "returns true for empty string key" do
      record = Record.build(offset: 0, key: "")

      assert Record.has_key?(record) == true
    end
  end

  describe "has_headers?/1" do
    test "returns false for nil headers" do
      record = Record.build(offset: 0, headers: nil)

      assert Record.has_headers?(record) == false
    end

    test "returns false for empty headers list" do
      record = Record.build(offset: 0, headers: [])

      assert Record.has_headers?(record) == false
    end

    test "returns true when headers present" do
      record = Record.build(offset: 0, headers: [%Header{key: "h", value: "v"}])

      assert Record.has_headers?(record) == true
    end
  end

  describe "get_header/2" do
    test "returns nil for nil headers" do
      record = Record.build(offset: 0, headers: nil)

      assert Record.get_header(record, "any-key") == nil
    end

    test "returns nil when header not found" do
      record = Record.build(offset: 0, headers: [%Header{key: "other", value: "v"}])

      assert Record.get_header(record, "missing") == nil
    end

    test "returns header value when found" do
      record =
        Record.build(
          offset: 0,
          headers: [
            %Header{key: "content-type", value: "application/json"},
            %Header{key: "trace-id", value: "abc123"}
          ]
        )

      assert Record.get_header(record, "content-type") == "application/json"
      assert Record.get_header(record, "trace-id") == "abc123"
    end
  end

  describe "topic_partition/1" do
    test "returns TopicPartition struct" do
      record = Record.build(offset: 0, topic: "my-topic", partition: 5)

      result = Record.topic_partition(record)

      assert %TopicPartition{} = result
      assert result.topic == "my-topic"
      assert result.partition == 5
    end
  end
end
