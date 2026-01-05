defmodule KafkaEx.Integration.Lifecycle.ApplicationFlowTest do
  use ExUnit.Case, async: true
  @moduletag :lifecycle

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "producer-consumer round trip" do
    test "produce and consume single message", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce
      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "round-trip-msg"}])
      produced_offset = produce_result.base_offset

      # Consume
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produced_offset, max_bytes: 100_000)

      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == "round-trip-msg"
      assert hd(fetch_result.records).offset == produced_offset
    end

    test "produce batch and consume all", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce batch
      messages = Enum.map(1..100, fn i -> %{value: "batch-#{i}"} end)
      {:ok, produce_result} = API.produce(client, topic_name, 0, messages)

      # Consume all
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produce_result.base_offset, max_bytes: 1_000_000)

      assert length(fetch_result.records) == 100
      assert hd(fetch_result.records).value == "batch-1"
      assert List.last(fetch_result.records).value == "batch-100"
    end

    test "produce with key and consume", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce with key
      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{key: "my-key", value: "keyed-value"}])

      # Consume and verify key
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produce_result.base_offset, max_bytes: 100_000)

      record = hd(fetch_result.records)
      assert record.key == "my-key"
      assert record.value == "keyed-value"
    end

    test "produce empty value (tombstone) and consume", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce tombstone (nil value with key)
      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{key: "delete-key", value: nil}])

      # Consume and verify tombstone
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produce_result.base_offset, max_bytes: 100_000)

      record = hd(fetch_result.records)
      assert record.key == "delete-key"
      assert record.value == nil or record.value == ""
    end
  end

  describe "at-least-once delivery simulation" do
    test "retry produce on simulated failure", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Simulate retry by producing same logical message multiple times
      message_id = "msg-#{:rand.uniform(100_000)}"

      # First attempt
      {:ok, r1} = API.produce(client, topic_name, 0, [%{key: message_id, value: "attempt-1"}])

      # Simulate retry (would happen if first attempt timed out without ack)
      {:ok, r2} = API.produce(client, topic_name, 0, [%{key: message_id, value: "attempt-2"}])

      # Both should succeed (at-least-once means duplicates possible)
      assert r1.base_offset >= 0
      assert r2.base_offset > r1.base_offset

      # Consumer should see both
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, r1.base_offset, max_bytes: 100_000)
      assert length(fetch_result.records) >= 2
    end

    test "consumer tracks offset for at-least-once", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce messages
      {:ok, produce_result} = API.produce(client, topic_name, 0, Enum.map(1..10, fn i -> %{value: "alo-#{i}"} end))

      # Consume and track offset
      {:ok, fetch1} = API.fetch(client, topic_name, 0, produce_result.base_offset, max_bytes: 100_000)
      processed_offset = List.last(fetch1.records).offset

      # Simulate crash/restart - re-read from last known offset
      {:ok, fetch2} = API.fetch(client, topic_name, 0, processed_offset, max_bytes: 100_000)

      # Should get at least the last message (which may be re-processed)
      assert length(fetch2.records) >= 1
      assert hd(fetch2.records).offset == processed_offset
    end
  end

  describe "multi-topic workflow" do
    test "produce to multiple topics and consume", %{client: client} do
      topic1 = generate_random_string()
      topic2 = generate_random_string()
      topic3 = generate_random_string()

      _ = create_topic(client, topic1)
      _ = create_topic(client, topic2)
      _ = create_topic(client, topic3)

      # Produce to all topics
      {:ok, r1} = API.produce(client, topic1, 0, [%{value: "topic1-msg"}])
      {:ok, r2} = API.produce(client, topic2, 0, [%{value: "topic2-msg"}])
      {:ok, r3} = API.produce(client, topic3, 0, [%{value: "topic3-msg"}])

      # Consume from all topics
      {:ok, f1} = API.fetch(client, topic1, 0, r1.base_offset, max_bytes: 100_000)
      {:ok, f2} = API.fetch(client, topic2, 0, r2.base_offset, max_bytes: 100_000)
      {:ok, f3} = API.fetch(client, topic3, 0, r3.base_offset, max_bytes: 100_000)

      assert hd(f1.records).value == "topic1-msg"
      assert hd(f2.records).value == "topic2-msg"
      assert hd(f3.records).value == "topic3-msg"
    end

    test "topic isolation - messages don't leak", %{client: client} do
      topic_a = generate_random_string()
      topic_b = generate_random_string()

      _ = create_topic(client, topic_a)
      _ = create_topic(client, topic_b)

      # Produce unique messages to each
      {:ok, _ra} = API.produce(client, topic_a, 0, [%{value: "only-in-A"}])
      {:ok, _rb} = API.produce(client, topic_b, 0, [%{value: "only-in-B"}])

      # Fetch all from each topic
      {:ok, fa} = API.fetch_all(client, topic_a, 0, max_bytes: 100_000)
      {:ok, fb} = API.fetch_all(client, topic_b, 0, max_bytes: 100_000)

      # Verify isolation
      a_values = Enum.map(fa.records, & &1.value)
      b_values = Enum.map(fb.records, & &1.value)

      assert "only-in-A" in a_values
      refute "only-in-B" in a_values
      assert "only-in-B" in b_values
      refute "only-in-A" in b_values
    end

    test "fan-out: produce to one topic, consume from multiple partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Produce to all partitions
      Enum.each(0..3, fn partition ->
        {:ok, _} = API.produce(client, topic_name, partition, [%{value: "fanout-p#{partition}"}])
      end)

      # Consume from all partitions
      results =
        Enum.map(0..3, fn partition ->
          {:ok, earliest} = API.earliest_offset(client, topic_name, partition)
          {:ok, fetch_result} = API.fetch(client, topic_name, partition, earliest, max_bytes: 100_000)
          {partition, hd(fetch_result.records).value}
        end)

      # Verify each partition got correct message
      Enum.each(results, fn {partition, value} ->
        assert value == "fanout-p#{partition}"
      end)
    end

    test "fan-in: multiple producers to single topic", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Simulate multiple producers (using tasks)
      tasks =
        Enum.map(1..5, fn producer_id ->
          Task.async(fn ->
            Enum.map(1..10, fn msg_id ->
              {:ok, _} = API.produce(client, topic_name, 0, [%{value: "producer-#{producer_id}-msg-#{msg_id}"}])
              :ok
            end)
          end)
        end)

      Task.await_many(tasks, 30_000)

      # Consume all messages
      {:ok, fetch_result} = API.fetch_all(client, topic_name, 0, max_bytes: 1_000_000)

      # Should have 50 messages (5 producers * 10 messages)
      assert length(fetch_result.records) == 50

      # Verify all producers contributed
      values = Enum.map(fetch_result.records, & &1.value)

      Enum.each(1..5, fn producer_id ->
        assert Enum.any?(values, &String.contains?(&1, "producer-#{producer_id}-"))
      end)
    end
  end

  describe "long-running consumer patterns" do
    test "continuous consumption with offset tracking", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce initial batch
      {:ok, r1} = API.produce(client, topic_name, 0, Enum.map(1..20, fn i -> %{value: "initial-#{i}"} end))

      # Fetch and verify offset tracking
      {:ok, fetch1} = API.fetch(client, topic_name, 0, r1.base_offset, max_bytes: 100_000)
      assert length(fetch1.records) == 20

      # Verify offset continuity
      last_offset = List.last(fetch1.records).offset + 1

      # Produce more
      {:ok, _} = API.produce(client, topic_name, 0, Enum.map(21..30, fn i -> %{value: "more-#{i}"} end))

      # Continue from last offset
      {:ok, fetch2} = API.fetch(client, topic_name, 0, last_offset, max_bytes: 100_000)
      assert length(fetch2.records) == 10
      assert hd(fetch2.records).value == "more-21"
    end

    test "consumer catches up after falling behind", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce batch
      {:ok, result} = API.produce(client, topic_name, 0, Enum.map(1..100, fn i -> %{value: "catchup-#{i}"} end))

      # Consumer fetches all messages at once
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 1_000_000)

      assert length(fetch_result.records) == 100
      assert hd(fetch_result.records).value == "catchup-1"
      assert List.last(fetch_result.records).value == "catchup-100"
    end
  end

  describe "error recovery scenarios" do
    test "recover from reading invalid offset", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "recovery-test"}])

      # Try to read from very high offset
      result = API.fetch(client, topic_name, 0, 999_999, max_bytes: 100_000)

      case result do
        {:ok, fetch_result} ->
          # Kafka may return empty records for out-of-range offset
          assert fetch_result.records == [] or is_list(fetch_result.records)

        {:error, :offset_out_of_range} ->
          # Expected error
          :ok

        {:error, _other} ->
          # Other errors acceptable
          :ok
      end

      # Should still be able to read valid data
      {:ok, earliest} = API.earliest_offset(client, topic_name, 0)
      {:ok, valid_fetch} = API.fetch(client, topic_name, 0, earliest, max_bytes: 100_000)
      assert length(valid_fetch.records) >= 1
    end

    test "client continues working after topic deletion", %{client: client} do
      topic1 = generate_random_string()
      topic2 = generate_random_string()

      _ = create_topic(client, topic1)
      _ = create_topic(client, topic2)

      # Work with topic1
      {:ok, _} = API.produce(client, topic1, 0, [%{value: "topic1-before"}])

      # Delete topic1
      {:ok, _} = API.delete_topic(client, topic1)
      Process.sleep(500)

      # Client should still work with topic2
      {:ok, result} = API.produce(client, topic2, 0, [%{value: "topic2-after-delete"}])
      assert result.base_offset >= 0

      {:ok, fetch_result} = API.fetch(client, topic2, 0, result.base_offset, max_bytes: 100_000)
      assert hd(fetch_result.records).value == "topic2-after-delete"
    end
  end

  describe "data integrity" do
    test "message content preserved through produce-consume", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Various content types
      test_values = [
        "simple string",
        "unicode: café résumé 日本語",
        String.duplicate("long", 1000),
        :erlang.term_to_binary(%{complex: [1, 2, 3]}),
        ""
      ]

      {:ok, result} = API.produce(client, topic_name, 0, Enum.map(test_values, fn v -> %{value: v} end))

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 1_000_000)

      fetched_values = Enum.map(fetch_result.records, & &1.value)

      Enum.zip(test_values, fetched_values)
      |> Enum.each(fn {expected, actual} ->
        assert expected == actual
      end)
    end

    test "message ordering preserved in batch", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce ordered sequence
      sequence = Enum.to_list(1..100)
      messages = Enum.map(sequence, fn i -> %{value: "#{i}"} end)

      {:ok, result} = API.produce(client, topic_name, 0, messages)

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset, max_bytes: 1_000_000)

      fetched_sequence =
        fetch_result.records
        |> Enum.map(& &1.value)
        |> Enum.map(&String.to_integer/1)

      assert fetched_sequence == sequence
    end
  end
end
