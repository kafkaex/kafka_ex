defmodule KafkaEx.Messages.Fetch.RecordTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Messages.Header

  describe "Record.build/1" do
    test "builds a record with required fields" do
      record = Record.build(offset: 42, value: "test_value")

      assert record.offset == 42
      assert record.value == "test_value"
    end

    test "builds a record with all optional fields" do
      headers = [Header.new("h1", "v1"), Header.new("h2", "v2")]

      record =
        Record.build(
          offset: 100,
          key: "my_key",
          value: "my_value",
          timestamp: 1_234_567_890,
          timestamp_type: :create_time,
          headers: headers,
          serialized_key_size: 6,
          serialized_value_size: 8,
          leader_epoch: 5,
          attributes: 0,
          crc: 12345,
          topic: "test_topic",
          partition: 0
        )

      assert record.offset == 100
      assert record.key == "my_key"
      assert record.value == "my_value"
      assert record.timestamp == 1_234_567_890
      assert record.timestamp_type == :create_time
      assert record.headers == headers
      assert record.serialized_key_size == 6
      assert record.serialized_value_size == 8
      assert record.leader_epoch == 5
      assert record.attributes == 0
      assert record.crc == 12345
      assert record.topic == "test_topic"
      assert record.partition == 0
    end

    test "allows nil key and value" do
      record = Record.build(offset: 0, key: nil, value: nil)

      assert record.offset == 0
      assert record.key == nil
      assert record.value == nil
    end

    test "computes serialized_key_size when not provided" do
      record = Record.build(offset: 0, key: "hello", value: "world")

      assert record.serialized_key_size == 5
      assert record.serialized_value_size == 5
    end

    test "serialized_key_size is -1 for nil key" do
      record = Record.build(offset: 0, key: nil, value: "test")

      assert record.serialized_key_size == -1
    end

    test "serialized_value_size is -1 for nil value" do
      record = Record.build(offset: 0, key: "key", value: nil)

      assert record.serialized_value_size == -1
    end
  end

  describe "Record.has_value?/1" do
    test "returns true when value is present" do
      record = Record.build(offset: 0, value: "test")
      assert Record.has_value?(record) == true
    end

    test "returns false when value is nil" do
      record = Record.build(offset: 0, value: nil)
      assert Record.has_value?(record) == false
    end
  end

  describe "Record.has_key?/1" do
    test "returns true when key is present" do
      record = Record.build(offset: 0, key: "my_key", value: "test")
      assert Record.has_key?(record) == true
    end

    test "returns false when key is nil" do
      record = Record.build(offset: 0, value: "test")
      assert Record.has_key?(record) == false
    end
  end

  describe "Record.has_headers?/1" do
    test "returns true when headers are present" do
      record = Record.build(offset: 0, value: "test", headers: [Header.new("key", "value")])
      assert Record.has_headers?(record) == true
    end

    test "returns false when headers is nil" do
      record = Record.build(offset: 0, value: "test")
      assert Record.has_headers?(record) == false
    end

    test "returns false when headers is empty list" do
      record = Record.build(offset: 0, value: "test", headers: [])
      assert Record.has_headers?(record) == false
    end
  end

  describe "Record.get_header/2" do
    test "returns header value when found" do
      record =
        Record.build(
          offset: 0,
          value: "test",
          headers: [Header.new("header1", "value1"), Header.new("header2", "value2")]
        )

      assert Record.get_header(record, "header1") == "value1"
      assert Record.get_header(record, "header2") == "value2"
    end

    test "returns nil when header not found" do
      record =
        Record.build(
          offset: 0,
          value: "test",
          headers: [Header.new("header1", "value1")]
        )

      assert Record.get_header(record, "nonexistent") == nil
    end

    test "returns nil when headers is nil" do
      record = Record.build(offset: 0, value: "test")

      assert Record.get_header(record, "any") == nil
    end
  end

  describe "timestamp_type field" do
    test "accepts :create_time" do
      record = Record.build(offset: 0, value: "test", timestamp_type: :create_time)
      assert record.timestamp_type == :create_time
    end

    test "accepts :log_append_time" do
      record = Record.build(offset: 0, value: "test", timestamp_type: :log_append_time)
      assert record.timestamp_type == :log_append_time
    end

    test "defaults to nil when not provided" do
      record = Record.build(offset: 0, value: "test")
      assert record.timestamp_type == nil
    end
  end
end
