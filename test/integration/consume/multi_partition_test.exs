defmodule KafkaEx.Integration.Consume.MultiPartitionTest do
  @moduledoc """
  Tests for multi-partition fetch operations.

  These tests verify that the library correctly handles fetching from
  multiple partitions. Tests that verify Kafka behavior (ordering guarantees,
  no duplication) have been consolidated - we trust Kafka's guarantees.
  """
  use ExUnit.Case, async: true
  @moduletag :consume

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "multi-partition fetch" do
    test "fetch from each partition returns correct messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Produce to each partition
      Enum.each(0..3, fn partition ->
        messages = Enum.map(1..10, fn i -> %{value: "p#{partition}-msg-#{i}"} end)
        {:ok, _} = API.produce(client, topic_name, partition, messages)
      end)

      # Fetch from each partition
      results =
        Enum.map(0..3, fn partition ->
          {:ok, earliest} = API.earliest_offset(client, topic_name, partition)
          {:ok, fetch_result} = API.fetch(client, topic_name, partition, earliest, max_bytes: 100_000)
          {partition, fetch_result.records}
        end)

      # Each partition should have 10 messages with correct prefix
      Enum.each(results, fn {partition, records} ->
        assert length(records) == 10, "Partition #{partition} has #{length(records)} records"
        assert hd(records).value == "p#{partition}-msg-1"
      end)
    end

    test "fetch from specific partition returns only that partition's messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Produce unique messages to each partition
      Enum.each(0..3, fn partition ->
        messages = [%{value: "unique-partition-#{partition}"}]
        {:ok, _} = API.produce(client, topic_name, partition, messages)
      end)

      # Fetch from partition 2 only
      {:ok, earliest} = API.earliest_offset(client, topic_name, 2)
      {:ok, fetch_result} = API.fetch(client, topic_name, 2, earliest, max_bytes: 100_000)

      # Should only get partition 2 message
      assert length(fetch_result.records) == 1
      assert hd(fetch_result.records).value == "unique-partition-2"
    end

    test "aggregating fetches from all partitions", %{client: client} do
      topic_name = generate_random_string()
      partition_count = 4
      messages_per_partition = 10
      _ = create_topic(client, topic_name, partitions: partition_count)

      # Produce to all partitions
      Enum.each(0..(partition_count - 1), fn partition ->
        messages = Enum.map(1..messages_per_partition, fn i -> %{value: "agg-p#{partition}-#{i}"} end)
        {:ok, _} = API.produce(client, topic_name, partition, messages)
      end)

      # Fetch from all partitions and aggregate
      all_messages =
        Enum.flat_map(0..(partition_count - 1), fn partition ->
          {:ok, earliest} = API.earliest_offset(client, topic_name, partition)
          {:ok, fetch_result} = API.fetch(client, topic_name, partition, earliest, max_bytes: 100_000)
          fetch_result.records
        end)

      assert length(all_messages) == partition_count * messages_per_partition
    end
  end

  describe "offset queries across partitions" do
  end

  describe "concurrent multi-partition operations" do
    test "concurrent fetch from multiple partitions", %{client: client} do
      topic_name = generate_random_string()
      partition_count = 8
      messages_per_partition = 100
      _ = create_topic(client, topic_name, partitions: partition_count)

      # Produce to all partitions
      Enum.each(0..(partition_count - 1), fn partition ->
        messages = Enum.map(1..messages_per_partition, fn i -> %{value: "large-p#{partition}-m#{i}"} end)
        {:ok, _} = API.produce(client, topic_name, partition, messages)
      end)

      # Fetch from all partitions concurrently
      tasks =
        Enum.map(0..(partition_count - 1), fn partition ->
          Task.async(fn ->
            {:ok, earliest} = API.earliest_offset(client, topic_name, partition)
            {:ok, fetch_result} = API.fetch(client, topic_name, partition, earliest, max_bytes: 1_000_000)
            length(fetch_result.records)
          end)
        end)

      counts = Task.await_many(tasks, 60_000)

      # Verify each partition has correct count
      assert Enum.all?(counts, &(&1 == messages_per_partition))
      assert Enum.sum(counts) == partition_count * messages_per_partition
    end
  end
end
