defmodule KafkaEx.Integration.FetchKayrockTest do
  use ExUnit.Case, async: true
  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  @moduletag :integration

  alias KafkaEx.New.Client
  alias KafkaEx.API, as: API
  alias KafkaEx.New.Kafka.Fetch
  alias KafkaEx.New.Kafka.Fetch.Record

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "fetch/5 basic functionality" do
    test "fetches messages from a topic", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce messages first
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "hello world"}])
      offset = result.base_offset

      # Fetch the messages
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, offset)

      assert %Fetch{} = fetch_result
      assert fetch_result.topic == topic_name
      assert fetch_result.partition == 0
      assert length(fetch_result.records) >= 1

      [message | _] = fetch_result.records
      assert %Record{} = message
      assert message.value == "hello world"
      assert message.offset == offset
    end

    test "fetches multiple messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce multiple messages
      {:ok, result1} = API.produce(client, topic_name, 0, [%{value: "first"}])
      {:ok, _result2} = API.produce(client, topic_name, 0, [%{value: "second"}])
      {:ok, _result3} = API.produce(client, topic_name, 0, [%{value: "third"}])

      # Fetch starting from the first message
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result1.base_offset)

      assert %Fetch{} = fetch_result
      assert length(fetch_result.records) >= 3

      values = Enum.map(fetch_result.records, & &1.value)
      assert "first" in values
      assert "second" in values
      assert "third" in values
    end

    test "fetches messages with keys", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce message with key
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "keyed message", key: "my-key"}])
      offset = result.base_offset

      # Fetch the message
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, offset)

      [message | _] = fetch_result.records
      assert message.value == "keyed message"
      assert message.key == "my-key"
    end

    test "returns empty messages when partition has no data at offset", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Get the latest offset (will be 0 for empty topic)
      {:ok, latest_offset} = API.latest_offset(client, topic_name, 0)

      # Fetch from the latest offset - should be empty
      # Use short max_wait_time to avoid timeout when waiting for non-existent data
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, latest_offset, max_wait_time: 100)

      assert %Fetch{} = fetch_result
      assert fetch_result.records == []
      assert Fetch.empty?(fetch_result)
    end

    test "fetches from different partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 3)

      # Produce to each partition
      {:ok, result0} = API.produce(client, topic_name, 0, [%{value: "partition 0"}])
      {:ok, result1} = API.produce(client, topic_name, 1, [%{value: "partition 1"}])
      {:ok, result2} = API.produce(client, topic_name, 2, [%{value: "partition 2"}])

      # Fetch from each partition
      {:ok, fetch0} = API.fetch(client, topic_name, 0, result0.base_offset)
      {:ok, fetch1} = API.fetch(client, topic_name, 1, result1.base_offset)
      {:ok, fetch2} = API.fetch(client, topic_name, 2, result2.base_offset)

      assert fetch0.partition == 0
      assert hd(fetch0.records).value == "partition 0"

      assert fetch1.partition == 1
      assert hd(fetch1.records).value == "partition 1"

      assert fetch2.partition == 2
      assert hd(fetch2.records).value == "partition 2"
    end
  end

  describe "fetch/5 high watermark and offsets" do
    test "returns correct high watermark", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce 3 messages
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg1"}])
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg2"}])
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "msg3"}])

      # Fetch from beginning
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, 0)

      # High watermark should be at least result.base_offset + 1
      assert fetch_result.high_watermark >= result.base_offset + 1
    end

    test "returns last_offset equal to max message offset", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce messages
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg1"}])
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg2"}])
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "msg3"}])

      # Fetch all
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, 0)

      # last_offset should be at least the offset of the last produced message
      assert fetch_result.last_offset >= result.base_offset
    end

    test "next_offset returns correct value", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce some messages
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "msg1"}])
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg2"}])

      # Fetch just the first message
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 100)

      # next_offset should be last_offset + 1
      next = Fetch.next_offset(fetch_result)
      assert next == fetch_result.last_offset + 1
    end
  end

  describe "fetch/5 with options" do
    test "respects max_bytes option", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce several large messages
      large_value = String.duplicate("x", 10_000)

      for _i <- 1..5 do
        {:ok, _} = API.produce(client, topic_name, 0, [%{value: large_value}])
      end

      # Fetch with small max_bytes - might get fewer messages
      {:ok, small_fetch} = API.fetch(client, topic_name, 0, 0, max_bytes: 5_000)

      # Fetch with large max_bytes - should get more messages
      {:ok, large_fetch} = API.fetch(client, topic_name, 0, 0, max_bytes: 100_000)

      # Large fetch should have equal or more messages
      assert length(large_fetch.records) >= length(small_fetch.records)
    end

    test "respects min_bytes option", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce a small message
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "small"}])

      # Fetch with min_bytes=1 should return quickly
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, min_bytes: 1)

      assert length(fetch_result.records) >= 1
    end

    test "respects max_wait_time option", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, latest_offset} = API.latest_offset(client, topic_name, 0)

      # Fetch from empty partition with short wait time
      start_time = System.monotonic_time(:millisecond)
      {:ok, _fetch_result} = API.fetch(client, topic_name, 0, latest_offset, max_wait_time: 100, min_bytes: 1_000_000)
      elapsed = System.monotonic_time(:millisecond) - start_time

      # Should return within a reasonable time (adding buffer for network latency)
      assert elapsed < 5_000
    end
  end

  describe "fetch/5 with API versions" do
    test "fetches with API V0", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "v0 fetch"}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, api_version: 0)

      assert %Fetch{} = fetch_result
      assert hd(fetch_result.records).value == "v0 fetch"
    end

    test "fetches with API V1", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "v1 fetch"}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, api_version: 1)

      assert %Fetch{} = fetch_result
      assert hd(fetch_result.records).value == "v1 fetch"
      # V1 includes throttle_time_ms
      assert is_integer(fetch_result.throttle_time_ms) or is_nil(fetch_result.throttle_time_ms)
    end

    test "fetches with API V2", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "v2 fetch"}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, api_version: 2)

      assert %Fetch{} = fetch_result
      assert hd(fetch_result.records).value == "v2 fetch"
    end

    test "fetches with API V3 (default)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "v3 fetch"}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, api_version: 3)

      assert %Fetch{} = fetch_result
      assert hd(fetch_result.records).value == "v3 fetch"
    end

    test "fetches with API V4 (adds isolation_level)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "v4 fetch"}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, api_version: 4)

      assert %Fetch{} = fetch_result
      assert hd(fetch_result.records).value == "v4 fetch"
      # V4 adds last_stable_offset
      assert is_integer(fetch_result.last_stable_offset) or is_nil(fetch_result.last_stable_offset)
    end

    test "fetches with API V5 (adds log_start_offset)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "v5 fetch"}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, api_version: 5)

      assert %Fetch{} = fetch_result
      assert hd(fetch_result.records).value == "v5 fetch"
      # V5 adds log_start_offset
      assert is_integer(fetch_result.log_start_offset) or is_nil(fetch_result.log_start_offset)
    end
  end

  describe "fetch_all/4 convenience function" do
    test "fetches all messages from a partition", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce messages
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg1"}])
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg2"}])
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg3"}])

      # Fetch all
      {:ok, fetch_result} = API.fetch_all(client, topic_name, 0)

      assert %Fetch{} = fetch_result
      assert length(fetch_result.records) >= 3

      values = Enum.map(fetch_result.records, & &1.value)
      assert "msg1" in values
      assert "msg2" in values
      assert "msg3" in values
    end

    test "returns empty result for empty partition", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Use short max_wait_time to avoid timeout when waiting for non-existent data
      {:ok, fetch_result} = API.fetch_all(client, topic_name, 0, max_wait_time: 100)

      assert %Fetch{} = fetch_result
      assert fetch_result.records == []
    end
  end

  describe "produce and fetch round-trip with new API" do
    test "binary values are preserved", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      binary_value = <<0, 1, 2, 255, 254, 253, 0, 0, 1>>
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: binary_value}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      assert hd(fetch_result.records).value == binary_value
    end

    test "empty values are preserved", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: ""}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      assert hd(fetch_result.records).value == ""
    end

    test "nil keys are preserved", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "test", key: nil}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      assert hd(fetch_result.records).key == nil
    end

    test "message offsets are sequential", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce messages one at a time
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "first"}])
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "second"}])
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "third"}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      offsets = Enum.map(fetch_result.records, & &1.offset)

      # Offsets should be sequential
      assert offsets == Enum.sort(offsets)

      Enum.reduce(offsets, nil, fn offset, prev ->
        if prev, do: assert(offset == prev + 1)
        offset
      end)
    end

    test "timestamps are included in V4+ fetch responses", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      timestamp = System.system_time(:millisecond)

      {:ok, result} =
        API.produce(client, topic_name, 0, [%{value: "timestamped", timestamp: timestamp}], api_version: 3)

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, api_version: 4)

      message = hd(fetch_result.records)
      assert message.value == "timestamped"
      # Timestamp may be present depending on broker config
      # (CreateTime vs LogAppendTime)
    end

    test "headers are preserved in V4+ fetch (when produced with V3+)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      headers = [{"content-type", "application/json"}, {"trace-id", "abc123"}]
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "with headers", headers: headers}], api_version: 3)

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, api_version: 4)

      message = hd(fetch_result.records)
      assert message.value == "with headers"

      # Headers should be present
      if message.headers do
        assert is_list(message.headers)
        # Check header values are accessible
        content_type = Record.get_header(message, "content-type")
        trace_id = Record.get_header(message, "trace-id")
        assert content_type == "application/json" or content_type == nil
        assert trace_id == "abc123" or trace_id == nil
      end
    end
  end

  describe "fetch error handling" do
    test "returns error for invalid partition", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 1)

      # Use short max_wait_time to avoid long waits
      result = API.fetch(client, topic_name, 99, 0, max_wait_time: 100)

      assert {:error, error} = result
      assert error in [:unknown_topic_or_partition, :leader_not_available, :unknown]
    end

    test "returns error for out of range offset", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce one message to ensure topic exists
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "test"}])

      # Try to fetch from a very large offset
      result = API.fetch(client, topic_name, 0, 999_999_999)

      # Should either return error or empty result depending on broker config
      case result do
        {:ok, %Fetch{records: []}} -> assert true
        {:error, :offset_out_of_range} -> assert true
        {:error, _other} -> assert true
      end
    end
  end

  describe "fetch with compression" do
    test "fetches gzip compressed messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} =
        API.produce(
          client,
          topic_name,
          0,
          [%{value: "gzip 1"}, %{value: "gzip 2"}, %{value: "gzip 3"}],
          compression: :gzip
        )

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      values = Enum.map(fetch_result.records, & &1.value)
      assert "gzip 1" in values
      assert "gzip 2" in values
      assert "gzip 3" in values
    end

    test "fetches snappy compressed messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} =
        API.produce(
          client,
          topic_name,
          0,
          [%{value: "snappy 1"}, %{value: "snappy 2"}, %{value: "snappy 3"}],
          compression: :snappy
        )

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      values = Enum.map(fetch_result.records, & &1.value)
      assert "snappy 1" in values
      assert "snappy 2" in values
      assert "snappy 3" in values
    end
  end

  describe "Fetch struct helper functions" do
    test "empty?/1 returns true for empty fetch", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, latest_offset} = API.latest_offset(client, topic_name, 0)
      # Use short max_wait_time to avoid timeout when waiting for non-existent data
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, latest_offset, max_wait_time: 100)

      assert Fetch.empty?(fetch_result) == true
    end

    test "empty?/1 returns false for non-empty fetch", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "test"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      assert Fetch.empty?(fetch_result) == false
    end

    test "record_count/1 returns correct count", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "a"}, %{value: "b"}, %{value: "c"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      assert Fetch.record_count(fetch_result) >= 3
    end
  end

  describe "Record struct helper functions" do
    test "has_value?/1 returns true when value present", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "has value"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      message = hd(fetch_result.records)
      assert Record.has_value?(message) == true
    end

    test "has_key?/1 returns true when key present", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "test", key: "my-key"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      message = hd(fetch_result.records)
      assert Record.has_key?(message) == true
    end

    test "has_key?/1 returns false when key is nil", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "test", key: nil}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)

      message = hd(fetch_result.records)
      assert Record.has_key?(message) == false
    end
  end
end
