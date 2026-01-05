defmodule KafkaEx.Integration.ConsumerGroup.SyncConsumerGroupTest do
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.TestConsumers.SyncTestConsumer

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)

    on_exit(fn ->
      if Process.alive?(client), do: GenServer.stop(client)
    end)

    {:ok, %{client: client}}
  end

  describe "sync consumer consumes messages" do
    @tag timeout: 60_000
    test "start consumer, produce messages, verify received", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      opts = consumer_group_opts()
      start_consumer_group(consumer_group, [topic_name], opts)

      # Wait for consumer to join and become stable
      Process.sleep(3_000)

      # Produce messages
      messages = ["message-1", "message-2", "message-3"]
      Enum.each(messages, &API.produce(client, topic_name, 0, [%{value: &1}]))

      # Wait for messages to be consumed
      received = receive_messages_until(length(messages), 10_000)

      # Verify all messages were received
      assert Enum.all?(messages, &(&1 in received))
    end
  end

  describe "sync consumer commits offset immediately" do
    @tag timeout: 60_000
    test "consume and verify offset committed - restart consumer to verify", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce messages first
      Enum.each(1..5, &API.produce(client, topic_name, 0, [%{value: "msg-#{&1}"}]))

      opts = consumer_group_opts(commit_threshold: 1, auto_offset_reset: :earliest)
      consumer_pid = start_consumer_group(consumer_group, [topic_name], opts)

      # Wait for messages to be consumed (sync consumer commits after each message)
      received1 = receive_messages_until(5, 15_000)
      assert length(received1) >= 5

      # Sync consumer commits immediately, but wait a bit for safety
      Process.sleep(2_000)

      # Stop consumer
      Supervisor.stop(consumer_pid)
      Process.sleep(1_000)

      # Produce more messages while consumer is stopped
      Enum.each(6..8, &API.produce(client, topic_name, 0, [%{value: "msg-#{&1}"}]))

      # Restart consumer with same group - should resume from committed offset
      start_consumer_group(consumer_group, [topic_name], opts)

      # Should only receive new messages (6-8), not old ones (1-5)
      Process.sleep(5_000)
      received2 = collect_all_messages()

      # Verify we got all new messages but none of the old ones
      Enum.each(6..8, fn i ->
        assert "msg-#{i}" in received2, "Should receive msg-#{i} after restart"
      end)

      Enum.each(1..5, fn i ->
        refute "msg-#{i}" in received2, "Should not re-receive msg-#{i} - offset was committed"
      end)
    end
  end

  describe "sync consumer crash recovery" do
    @tag timeout: 90_000
    test "consumer retries max_restarts times before giving up", %{client: client} do
      Process.flag(:trap_exit, true)
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      max_restarts = 3
      opts = consumer_group_opts(commit_threshold: 1, max_restarts: max_restarts, max_seconds: 60)
      {:ok, consumer_pid} = ConsumerGroup.start_link(SyncTestConsumer, consumer_group, [topic_name], opts)

      Process.sleep(3_000)
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "CRASH"}])

      receive do
        {:EXIT, ^consumer_pid, _reason} -> :ok
      after
        30_000 -> flunk("Expected supervisor to shut down after #{max_restarts} restarts")
      end

      refute Process.alive?(consumer_pid)
    end
  end

  describe "auto offset reset" do
    @tag timeout: 60_000
    test "earliest: consumes messages produced before consumer started", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      pre_messages = ["pre-1", "pre-2", "pre-3"]
      Enum.each(pre_messages, &API.produce(client, topic_name, 0, [%{value: &1}]))
      Process.sleep(500)

      opts = consumer_group_opts(auto_offset_reset: :earliest)
      start_consumer_group(consumer_group, [topic_name], opts)

      Process.sleep(5_000)
      received = collect_all_messages()

      Enum.each(pre_messages, fn msg ->
        assert msg in received, "Expected to receive '#{msg}' from earliest offset"
      end)
    end

    @tag timeout: 60_000
    test "latest: only consumes messages produced after consumer started", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "old-message"}])
      Process.sleep(500)

      opts = consumer_group_opts(auto_offset_reset: :latest)
      {:ok, consumer_pid} = ConsumerGroup.start_link(SyncTestConsumer, consumer_group, [topic_name], opts)

      Process.sleep(5_000)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "new-message"}])

      received = receive_messages_until(1, 10_000)

      refute "old-message" in received, "Should not receive old message with :latest offset"
      assert "new-message" in received, "Should receive new message"

      Supervisor.stop(consumer_pid)
    end
  end

  describe "multiple consumers rebalancing" do
    @tag timeout: 90_000
    test "partitions are redistributed when second consumer joins", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      opts = consumer_group_opts(commit_interval: 2_000)
      {:ok, consumer1} = ConsumerGroup.start_link(SyncTestConsumer, consumer_group, [topic_name], opts)
      Process.sleep(5_000)

      # Produce messages to all partitions
      Enum.each(0..3, &API.produce(client, topic_name, &1, [%{value: "phase1-p#{&1}"}]))

      # Collect partition info - single consumer should handle all 4 partitions
      partitions_before = collect_partition_messages(4, 10_000)

      assert MapSet.size(MapSet.new(Map.keys(partitions_before))) == 4

      # Start second consumer - triggers rebalance
      {:ok, consumer2} = ConsumerGroup.start_link(SyncTestConsumer, consumer_group, [topic_name], opts)
      Process.sleep(5_000)

      # Produce more messages after rebalance
      Enum.each(0..3, &API.produce(client, topic_name, &1, [%{value: "phase2-p#{&1}"}]))

      # Collect partition info - partitions should be distributed between consumers
      partitions_after = collect_partition_messages(4, 10_000)

      assert MapSet.size(MapSet.new(Map.keys(partitions_after))) == 4

      # Both consumers should still be alive
      assert Process.alive?(consumer1)
      assert Process.alive?(consumer2)

      Supervisor.stop(consumer1)
      Supervisor.stop(consumer2)
    end

    @tag timeout: 90_000
    test "partitions are redistributed when consumer leaves", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      opts = [
        heartbeat_interval: 1_000,
        session_timeout: 10_000,
        commit_interval: 2_000,
        auto_offset_reset: :earliest,
        extra_consumer_args: [test_pid: self(), include_partition: true]
      ]

      # Start two consumers - partitions should be split
      {:ok, consumer1} = ConsumerGroup.start_link(SyncTestConsumer, consumer_group, [topic_name], opts)
      {:ok, consumer2} = ConsumerGroup.start_link(SyncTestConsumer, consumer_group, [topic_name], opts)
      Process.sleep(5_000)

      # Produce messages to all partitions
      Enum.each(0..3, &API.produce(client, topic_name, &1, [%{value: "phase1-p#{&1}"}]))

      # Collect - should have all partitions covered
      partitions_before = collect_partition_messages(4, 10_000)
      assert MapSet.size(MapSet.new(Map.keys(partitions_before))) == 4

      # Stop first consumer - triggers rebalance
      Supervisor.stop(consumer1)
      Process.sleep(5_000)

      # Produce more messages - remaining consumer should get all partitions
      Enum.each(0..3, &API.produce(client, topic_name, &1, [%{value: "phase2-p#{&1}"}]))

      # Remaining consumer should handle all 4 partitions
      partitions_after = collect_partition_messages(4, 15_000)
      assert MapSet.size(MapSet.new(Map.keys(partitions_after))) == 4

      refute Process.alive?(consumer1)
      assert Process.alive?(consumer2)

      Supervisor.stop(consumer2)
    end
  end

  describe "sync consumer with multiple topics" do
    @tag timeout: 60_000
    test "subscribe to multiple topics, receive from all", %{client: client} do
      topic1 = generate_random_string()
      topic2 = generate_random_string()
      consumer_group = generate_random_string()

      _ = create_topic(client, topic1)
      _ = create_topic(client, topic2)

      opts = consumer_group_opts()
      start_consumer_group(consumer_group, [topic1, topic2], opts)

      Process.sleep(5_000)

      {:ok, _} = API.produce(client, topic1, 0, [%{value: "from-topic1"}])
      {:ok, _} = API.produce(client, topic2, 0, [%{value: "from-topic2"}])

      received = receive_messages_until(2, 15_000)

      assert "from-topic1" in received
      assert "from-topic2" in received
    end
  end

  describe "long-running sync produce/consume" do
    @tag timeout: 120_000
    @tag :long_process
    test "60 second stress test - continuous produce and sync consume", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      opts = consumer_group_opts(commit_interval: 2_000)
      {:ok, consumer_pid} = ConsumerGroup.start_link(SyncTestConsumer, consumer_group, [topic_name], opts)

      Process.sleep(5_000)

      produced_count = :counters.new(1, [:atomics])
      start_time = System.monotonic_time(:millisecond)
      duration_ms = 60_000

      producer_task = Task.async(fn -> produce_loop(client, topic_name, produced_count, start_time, duration_ms) end)
      Task.await(producer_task, 70_000)

      total_produced = :counters.get(produced_count, 1)

      Process.sleep(5_000)

      received_messages = collect_all_messages()
      total_received = length(received_messages)

      assert total_received > 0
      assert total_produced > 0
      assert total_received == total_produced, "Expected #{total_produced} messages, got #{total_received}"
      assert Process.alive?(consumer_pid)

      Process.sleep(1_000)
      partitions = Enum.map(0..3, fn p -> %{partition_num: p} end)
      {:ok, offset_responses} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions)

      total_committed =
        offset_responses
        |> Enum.flat_map(& &1.partition_offsets)
        |> Enum.map(& &1.offset)
        |> Enum.filter(&(&1 > 0))
        |> Enum.sum()

      assert total_committed > 0

      Supervisor.stop(consumer_pid)
    end

    @tag timeout: 120_000
    @tag :long_process
    test "60 second test with multiple sync consumers", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      opts = consumer_group_opts(commit_interval: 2_000)

      {:ok, consumer1} = ConsumerGroup.start_link(SyncTestConsumer, consumer_group, [topic_name], opts)
      Process.sleep(2_000)
      {:ok, consumer2} = ConsumerGroup.start_link(SyncTestConsumer, consumer_group, [topic_name], opts)
      Process.sleep(5_000)

      produced_count = :counters.new(1, [:atomics])
      start_time = System.monotonic_time(:millisecond)
      duration_ms = 60_000

      producer_task = Task.async(fn -> produce_loop(client, topic_name, produced_count, start_time, duration_ms) end)
      Task.await(producer_task, 70_000)

      total_produced = :counters.get(produced_count, 1)

      Process.sleep(5_000)

      received_messages = collect_all_messages()
      total_received = length(received_messages)

      assert Process.alive?(consumer1)
      assert Process.alive?(consumer2)

      assert total_received > 0
      assert total_produced > 0
      assert total_received == total_produced, "Expected #{total_produced} messages, got #{total_received}"

      Supervisor.stop(consumer1)
      Supervisor.stop(consumer2)
    end
  end

  # Helper functions
  defp receive_messages_until(count, timeout), do: receive_messages_until(count, timeout, [])
  defp receive_messages_until(count, _timeout, acc) when length(acc) >= count, do: acc

  defp receive_messages_until(count, timeout, acc) do
    receive do
      {:messages_received, messages} -> receive_messages_until(count, timeout, acc ++ messages)
    after
      timeout -> acc
    end
  end

  defp collect_all_messages, do: collect_all_messages([])

  defp collect_all_messages(acc) do
    receive do
      {:messages_received, messages} -> collect_all_messages(acc ++ messages)
      {:partition_messages, _partition, messages} -> collect_all_messages(acc ++ messages)
    after
      1_000 -> acc
    end
  end

  defp collect_partition_messages(count, timeout), do: collect_partition_messages(count, timeout, %{})

  defp collect_partition_messages(count, _timeout, acc) when map_size(acc) >= count, do: acc

  defp collect_partition_messages(count, timeout, acc) do
    receive do
      {:partition_messages, partition, messages} ->
        updated = Map.update(acc, partition, messages, &(&1 ++ messages))
        collect_partition_messages(count, timeout, updated)
    after
      timeout -> acc
    end
  end

  defp produce_loop(client, topic_name, counter, start_time, duration_ms) do
    now = System.monotonic_time(:millisecond)
    elapsed = now - start_time

    if elapsed < duration_ms do
      partition = rem(:rand.uniform(1000), 4)
      msg_num = :counters.get(counter, 1) + 1

      case API.produce(client, topic_name, partition, [%{value: "stress-msg-#{msg_num}"}]) do
        {:ok, _} -> :counters.add(counter, 1, 1)
        {:error, _} -> :ok
      end

      Process.sleep(10)
      produce_loop(client, topic_name, counter, start_time, duration_ms)
    else
      :ok
    end
  end

  defp consumer_group_opts(opts \\ []) do
    [
      heartbeat_interval: 1_000,
      session_timeout: 10_000,
      commit_interval: 1_000,
      extra_consumer_args: [test_pid: self()]
    ]
    |> Keyword.merge(opts)
  end

  defp start_consumer_group(name, topics, opts) do
    {:ok, consumer_pid} = ConsumerGroup.start_link(SyncTestConsumer, name, topics, opts)

    on_exit(fn ->
      try do
        if Process.alive?(consumer_pid), do: Supervisor.stop(consumer_pid)
      catch
        :exit, _ -> :ok
      end
    end)

    consumer_pid
  end
end
