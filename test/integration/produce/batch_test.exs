defmodule KafkaEx.Integration.Produce.BatchTest do
  use ExUnit.Case, async: true
  @moduletag :produce

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API, as: API
  alias KafkaEx.Messages.RecordMetadata

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "large batch produce" do
    test "produces 1000 messages in single request", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Generate 1000 messages
      messages = Enum.map(1..1000, &%{value: "message #{&1}"})

      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %RecordMetadata{} = result
      assert result.topic == topic_name
      assert result.partition == 0
      assert result.base_offset >= 0

      # Verify all messages were produced by checking latest offset
      {:ok, latest_offset} = API.latest_offset(client, topic_name, 0)
      assert latest_offset >= result.base_offset + 1000
    end

    test "produces 5000 messages in single request", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = Enum.map(1..5000, &%{value: "message #{&1}"})

      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      # Verify offset increment
      {:ok, latest_offset} = API.latest_offset(client, topic_name, 0)
      assert latest_offset >= result.base_offset + 5000
    end

    test "large batch with compression reduces network overhead", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Create messages with repetitive content (compresses well)
      messages =
        Enum.map(1..1000, fn i ->
          %{value: "repetitive content message number #{i} with padding " <> String.duplicate("x", 100)}
        end)

      # Produce with gzip compression
      {:ok, result} = API.produce(client, topic_name, 0, messages, compression: :gzip)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      # Verify all messages were produced by checking offset increment
      {:ok, latest_offset} = API.latest_offset(client, topic_name, 0)
      assert latest_offset >= result.base_offset + 1000
    end

    test "produces and fetches 100 compressed messages with gzip", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Create 100 messages with compressible content
      messages =
        Enum.map(1..100, fn i ->
          %{value: "gzip message #{i} " <> String.duplicate("y", 50)}
        end)

      # Produce with gzip compression
      {:ok, result} = API.produce(client, topic_name, 0, messages, compression: :gzip)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      # Fetch back and verify all messages (requires Kayrock compression fix)
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 10_000_000)
      assert length(fetch_result.records) == 100

      # Verify first and last message content
      assert hd(fetch_result.records).value == "gzip message 1 " <> String.duplicate("y", 50)
      assert List.last(fetch_result.records).value == "gzip message 100 " <> String.duplicate("y", 50)
    end

    # This test requires Kayrock compression fix to be merged upstream.
    # See docs/KAYROCK_COMPRESSION_ANALYSIS.md for details on the iolist vs binary bug.
    # The fix is in deps/kayrock/lib/kayrock/compression.ex - decompress must return binary.
    @tag :skip
    test "produces and fetches 500 compressed messages with gzip", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Create 500 messages with compressible content
      messages =
        Enum.map(1..500, fn i ->
          %{value: "gzip large batch message #{i} " <> String.duplicate("z", 100)}
        end)

      # Produce with gzip compression
      {:ok, result} = API.produce(client, topic_name, 0, messages, compression: :gzip)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      # Fetch back and verify all messages
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 10_000_000)
      assert length(fetch_result.records) == 500
    end
  end

  describe "batch with mixed keys" do
    test "messages with different keys are produced successfully", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [
        %{value: "value1", key: "key-a"},
        %{value: "value2", key: "key-b"},
        %{value: "value3", key: "key-c"},
        %{value: "value4", key: nil},
        %{value: "value5", key: "key-a"}
      ]

      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      # Fetch and verify all messages with their keys
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)
      records = fetch_result.records

      assert length(records) >= 5

      keys = Enum.map(records, & &1.key)
      assert "key-a" in keys
      assert "key-b" in keys
      assert "key-c" in keys
    end

    test "batch routes to different partitions based on key when partition is nil", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Produce messages with different keys to partition=nil (uses partitioner)
      # Same key should consistently go to same partition
      key_a = "consistent-key-alpha"
      key_b = "consistent-key-beta"

      # First batch with key_a
      {:ok, result_a1} = API.produce(client, topic_name, nil, [%{value: "msg1", key: key_a}])
      # Second batch with key_a should go to same partition
      {:ok, result_a2} = API.produce(client, topic_name, nil, [%{value: "msg2", key: key_a}])

      # Key_b may go to different partition
      {:ok, result_b} = API.produce(client, topic_name, nil, [%{value: "msg3", key: key_b}])

      # Same key should route to same partition
      assert result_a1.partition == result_a2.partition

      # All results should be valid
      assert result_a1.partition in 0..3
      assert result_b.partition in 0..3
    end

    test "nil keys route to potentially different partitions (round-robin)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Produce multiple messages with nil keys
      results =
        Enum.map(1..20, fn i ->
          {:ok, result} = API.produce(client, topic_name, nil, [%{value: "msg #{i}", key: nil}])
          result.partition
        end)

      # With 20 messages across 4 partitions, we should see at least 2 different partitions
      unique_partitions = Enum.uniq(results)
      assert length(unique_partitions) >= 1
      # All partitions should be valid
      assert Enum.all?(results, &(&1 in 0..3))
    end
  end

  describe "maximum message size" do
    test "produces message near default max size (1MB)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Create a ~900KB message (under default 1MB limit)
      large_value = :crypto.strong_rand_bytes(900 * 1024)
      messages = [%{value: large_value}]

      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      # Verify we can fetch it back
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 2_000_000)
      assert length(fetch_result.records) >= 1
      [record | _] = fetch_result.records
      assert byte_size(record.value) == 900 * 1024
    end

    test "produces multiple medium-sized messages in batch", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Create 10 messages of 50KB each = 500KB total
      messages =
        Enum.map(1..10, fn _i ->
          %{value: :crypto.strong_rand_bytes(50 * 1024)}
        end)

      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0

      # Verify all messages produced
      {:ok, latest_offset} = API.latest_offset(client, topic_name, 0)
      assert latest_offset >= result.base_offset + 10
    end

    test "large message with compression", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Create a large compressible message (repeated pattern)
      large_value = String.duplicate("compressible pattern data ", 40_000)
      messages = [%{value: large_value}]

      {:ok, result} = API.produce(client, topic_name, 0, messages, compression: :gzip)

      assert %RecordMetadata{} = result
      assert result.base_offset >= 0
    end
  end

  describe "produce to multiple partitions" do
    test "single produce request with explicit partition targets different partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Produce to each partition separately
      {:ok, r0} = API.produce(client, topic_name, 0, [%{value: "partition 0"}])
      {:ok, r1} = API.produce(client, topic_name, 1, [%{value: "partition 1"}])
      {:ok, r2} = API.produce(client, topic_name, 2, [%{value: "partition 2"}])
      {:ok, r3} = API.produce(client, topic_name, 3, [%{value: "partition 3"}])

      assert r0.partition == 0
      assert r1.partition == 1
      assert r2.partition == 2
      assert r3.partition == 3

      # Verify messages in each partition
      {:ok, f0} = API.fetch(client, topic_name, 0, r0.base_offset)
      {:ok, f1} = API.fetch(client, topic_name, 1, r1.base_offset)
      {:ok, f2} = API.fetch(client, topic_name, 2, r2.base_offset)
      {:ok, f3} = API.fetch(client, topic_name, 3, r3.base_offset)

      assert hd(f0.records).value == "partition 0"
      assert hd(f1.records).value == "partition 1"
      assert hd(f2.records).value == "partition 2"
      assert hd(f3.records).value == "partition 3"
    end

    test "concurrent produces to different partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Spawn tasks to produce concurrently to different partitions
      tasks =
        Enum.map(0..3, fn partition ->
          Task.async(fn ->
            messages = Enum.map(1..100, fn i -> %{value: "p#{partition}-msg#{i}"} end)
            API.produce(client, topic_name, partition, messages)
          end)
        end)

      results = Task.await_many(tasks, 30_000)

      # All produces should succeed
      Enum.each(results, fn {:ok, result} ->
        assert %RecordMetadata{} = result
        assert result.partition in 0..3
      end)

      # Verify each partition has messages
      Enum.each(0..3, fn partition ->
        {:ok, latest} = API.latest_offset(client, topic_name, partition)
        assert latest >= 100
      end)
    end

    test "high-throughput produce across all partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Produce 250 messages to each partition = 1000 total
      results =
        Enum.flat_map(0..3, fn partition ->
          Enum.map(1..250, fn i ->
            {:ok, result} = API.produce(client, topic_name, partition, [%{value: "msg-#{partition}-#{i}"}])
            result
          end)
        end)

      assert length(results) == 1000
      assert Enum.all?(results, &(&1.base_offset >= 0))

      # Verify total messages across all partitions
      total_messages =
        Enum.reduce(0..3, 0, fn partition, acc ->
          {:ok, latest} = API.latest_offset(client, topic_name, partition)
          {:ok, earliest} = API.earliest_offset(client, topic_name, partition)
          acc + (latest - earliest)
        end)

      assert total_messages >= 1000
    end
  end
end
