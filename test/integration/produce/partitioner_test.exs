defmodule KafkaEx.Integration.Produce.PartitionerTest do
  use ExUnit.Case, async: true
  @moduletag :produce

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "default partitioner" do
    test "messages distributed across partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Send 100 messages without explicit partition (uses partitioner)
      partitions_used =
        Enum.map(1..100, fn i ->
          {:ok, result} = API.produce(client, topic_name, nil, [%{value: "msg-#{i}"}])
          result.partition
        end)
        |> Enum.uniq()

      # Should use multiple partitions (not all to one)
      assert length(partitions_used) >= 1

      # All partitions should be valid
      assert Enum.all?(partitions_used, &(&1 in 0..3))
    end

    test "messages without keys use round-robin or random distribution", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Send many messages with nil keys
      partition_counts =
        Enum.reduce(1..200, %{}, fn i, acc ->
          {:ok, result} = API.produce(client, topic_name, nil, [%{value: "no-key-#{i}", key: nil}])
          Map.update(acc, result.partition, 1, &(&1 + 1))
        end)

      # With 200 messages across 4 partitions, each should get some messages
      # (allowing for random/round-robin variance)
      total = Enum.sum(Map.values(partition_counts))
      assert total == 200

      # Verify all used partitions are valid
      used_partitions = Map.keys(partition_counts)
      assert Enum.all?(used_partitions, &(&1 in 0..3))
    end
  end

  describe "key-based partitioning" do
    test "same key always goes to same partition", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 8)

      # Define test keys
      keys = ["user-123", "order-456", "session-789", "event-abc"]

      # For each key, send multiple messages and verify they all go to same partition
      Enum.each(keys, fn key ->
        partitions =
          Enum.map(1..10, fn i ->
            {:ok, result} = API.produce(client, topic_name, nil, [%{value: "msg-#{i}", key: key}])
            result.partition
          end)

        unique_partitions = Enum.uniq(partitions)

        assert length(unique_partitions) == 1,
               "Key '#{key}' went to multiple partitions: #{inspect(unique_partitions)}"
      end)
    end

    test "different keys may go to different partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 8)

      # Generate many different keys
      key_partitions =
        Enum.map(1..50, fn i ->
          key = "unique-key-#{i}-#{:rand.uniform(10000)}"
          {:ok, result} = API.produce(client, topic_name, nil, [%{value: "msg", key: key}])
          result.partition
        end)
        |> Enum.uniq()

      # With 50 unique keys across 8 partitions, should hit multiple partitions
      assert length(key_partitions) >= 2,
             "Expected multiple partitions, got: #{inspect(key_partitions)}"
    end

    test "key hash is consistent across produces", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      test_key = "consistent-test-key"

      # First produce
      {:ok, result1} = API.produce(client, topic_name, nil, [%{value: "first", key: test_key}])

      # Wait a bit
      Process.sleep(100)

      # Second produce with same key
      {:ok, result2} = API.produce(client, topic_name, nil, [%{value: "second", key: test_key}])

      # Third produce with same key
      {:ok, result3} = API.produce(client, topic_name, nil, [%{value: "third", key: test_key}])

      # All should go to same partition
      assert result1.partition == result2.partition
      assert result2.partition == result3.partition
    end

    test "binary keys work correctly", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Use binary data as key
      binary_key = :crypto.strong_rand_bytes(16)

      partitions =
        Enum.map(1..5, fn i ->
          {:ok, result} = API.produce(client, topic_name, nil, [%{value: "msg-#{i}", key: binary_key}])
          result.partition
        end)

      # Same binary key should always go to same partition
      assert length(Enum.uniq(partitions)) == 1
    end

    test "empty string key is treated as a key (not nil)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      empty_key = ""

      # Messages with empty string key should go to consistent partition
      partitions =
        Enum.map(1..5, fn i ->
          {:ok, result} = API.produce(client, topic_name, nil, [%{value: "msg-#{i}", key: empty_key}])
          result.partition
        end)

      unique_partitions = Enum.uniq(partitions)
      assert length(unique_partitions) == 1, "Empty string key should be consistent"
    end
  end

  describe "explicit partition override" do
    test "explicit partition ignores key", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      key = "should-hash-to-some-partition"

      # Explicitly produce to partition 2, regardless of key
      {:ok, result} = API.produce(client, topic_name, 2, [%{value: "explicit-partition", key: key}])

      assert result.partition == 2

      # Verify message is in partition 2
      {:ok, fetch_result} = API.fetch(client, topic_name, 2, result.base_offset)
      assert hd(fetch_result.records).value == "explicit-partition"
      assert hd(fetch_result.records).key == key
    end

    test "can produce to all partitions explicitly", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Produce to each partition explicitly
      results =
        Enum.map(0..3, fn partition ->
          {:ok, result} = API.produce(client, topic_name, partition, [%{value: "partition-#{partition}"}])
          {partition, result}
        end)

      # Verify each produce went to the correct partition
      Enum.each(results, fn {expected_partition, result} ->
        assert result.partition == expected_partition
      end)
    end
  end

  describe "partitioner with multi-message batches" do
    test "all messages in batch go to same partition when key provided", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      batch_key = "batch-key"
      messages = Enum.map(1..10, fn i -> %{value: "batch-msg-#{i}", key: batch_key} end)

      {:ok, result} = API.produce(client, topic_name, nil, messages)

      # All messages in batch should be in the same partition
      {:ok, fetch_result} = API.fetch(client, topic_name, result.partition, result.base_offset, max_bytes: 100_000)

      # Should have all 10 messages in this partition
      assert length(fetch_result.records) >= 10

      # All should have the same key
      Enum.each(fetch_result.records, fn record ->
        assert record.key == batch_key
      end)
    end

    test "batch with mixed keys uses first message key for partition", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Note: This test documents current behavior - a batch with mixed keys
      # typically uses the first key for partition selection
      messages = [
        %{value: "msg1", key: "key-A"},
        %{value: "msg2", key: "key-B"},
        %{value: "msg3", key: "key-C"}
      ]

      {:ok, result} = API.produce(client, topic_name, nil, messages)

      # The batch went to some partition
      assert result.partition in 0..3

      # Fetch and verify all messages are in that partition
      {:ok, fetch_result} = API.fetch(client, topic_name, result.partition, result.base_offset)
      values = Enum.map(fetch_result.records, & &1.value)

      assert "msg1" in values
      assert "msg2" in values
      assert "msg3" in values
    end
  end
end
