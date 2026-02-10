defmodule KafkaEx.Messages.FetchTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.Fetch.Record

  describe "Fetch.build/1" do
    test "builds a fetch result with required fields" do
      records = [
        Record.build(offset: 0, key: "key1", value: "value1"),
        Record.build(offset: 1, key: "key2", value: "value2")
      ]

      fetch =
        Fetch.build(
          topic: "test_topic",
          partition: 0,
          records: records,
          high_watermark: 10
        )

      assert fetch.topic == "test_topic"
      assert fetch.partition == 0
      assert length(fetch.records) == 2
      assert fetch.high_watermark == 10
      assert fetch.last_offset == 1
    end

    test "computes last_offset from records" do
      records = [
        Record.build(offset: 5, value: "value1"),
        Record.build(offset: 10, value: "value2"),
        Record.build(offset: 7, value: "value3")
      ]

      fetch =
        Fetch.build(
          topic: "test_topic",
          partition: 0,
          records: records,
          high_watermark: 20
        )

      assert fetch.last_offset == 10
    end

    test "sets last_offset to nil when no records" do
      fetch =
        Fetch.build(
          topic: "test_topic",
          partition: 0,
          records: [],
          high_watermark: 0
        )

      assert fetch.last_offset == nil
    end

    test "accepts optional fields" do
      fetch =
        Fetch.build(
          topic: "test_topic",
          partition: 0,
          records: [],
          high_watermark: 100,
          last_stable_offset: 95,
          log_start_offset: 50,
          preferred_read_replica: 2,
          throttle_time_ms: 10,
          aborted_transactions: [%{producer_id: 1, first_offset: 60}]
        )

      assert fetch.last_stable_offset == 95
      assert fetch.log_start_offset == 50
      assert fetch.preferred_read_replica == 2
      assert fetch.throttle_time_ms == 10
      assert fetch.aborted_transactions == [%{producer_id: 1, first_offset: 60}]
    end
  end

  describe "Fetch.empty?/1" do
    test "returns true for empty records" do
      fetch =
        Fetch.build(
          topic: "test_topic",
          partition: 0,
          records: [],
          high_watermark: 0
        )

      assert Fetch.empty?(fetch) == true
    end

    test "returns false for non-empty records" do
      fetch =
        Fetch.build(
          topic: "test_topic",
          partition: 0,
          records: [Record.build(offset: 0, value: "test")],
          high_watermark: 1
        )

      assert Fetch.empty?(fetch) == false
    end
  end

  describe "Fetch.record_count/1" do
    test "returns count of records" do
      records = [
        Record.build(offset: 0, value: "value1"),
        Record.build(offset: 1, value: "value2"),
        Record.build(offset: 2, value: "value3")
      ]

      fetch =
        Fetch.build(
          topic: "test_topic",
          partition: 0,
          records: records,
          high_watermark: 10
        )

      assert Fetch.record_count(fetch) == 3
    end
  end

  describe "Fetch.next_offset/1" do
    test "returns last_offset + 1 when records exist" do
      fetch =
        Fetch.build(
          topic: "test_topic",
          partition: 0,
          records: [Record.build(offset: 5, value: "test")],
          high_watermark: 10
        )

      assert Fetch.next_offset(fetch) == 6
    end

    test "returns high_watermark when no records" do
      fetch =
        Fetch.build(
          topic: "test_topic",
          partition: 0,
          records: [],
          high_watermark: 10
        )

      assert Fetch.next_offset(fetch) == 10
    end
  end
end
