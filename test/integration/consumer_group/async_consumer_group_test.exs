defmodule KafkaEx.Integration.ConsumerGroup.AsyncConsumerGroupTest do
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.TestConsumers.AsyncTestConsumer

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)

    {:ok, %{client: client}}
  end

  describe "async consumer consumes messages" do
    @tag timeout: 60_000
    test "start consumer, produce messages, verify received", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      opts = consumer_group_opts()
      start_consumer_group(consumer_group, [topic_name], opts)

      # Wait for consumer to join and become stable
      Process.sleep(3_000)

      messages = ["message-1", "message-2", "message-3"]
      Enum.each(messages, &API.produce(client, topic_name, 0, [%{value: &1}]))

      received = receive_messages_until(length(messages), 10_000)
      assert Enum.all?(messages, &(&1 in received))
    end
  end

  describe "async consumer auto-commits offset" do
    @tag timeout: 60_000
    test "consume and verify offset committed - restart consumer to verify", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce messages first
      Enum.each(1..5, fn i -> {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg-#{i}"}]) end)

      opts = consumer_group_opts(commit_threshold: 1, auto_offset_reset: :earliest)
      consumer_pid = start_consumer_group(consumer_group, [topic_name], opts)

      # Wait for messages to be consumed
      received1 = receive_messages_until(5, 15_000)
      assert length(received1) >= 5

      # Wait for commit
      Process.sleep(2_000)

      # Stop consumer
      Supervisor.stop(consumer_pid)
      Process.sleep(1_000)

      # Produce more messages while consumer is stopped
      Enum.each(6..8, fn i -> {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg-#{i}"}]) end)

      # Restart consumer with same group - should resume from committed offset
      start_consumer_group(consumer_group, [topic_name], opts)
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

  describe "async consumer crash recovery" do
    @tag timeout: 90_000
    test "consumer retries max_restarts times before giving up", %{client: client} do
      Process.flag(:trap_exit, true)
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      max_restarts = 3
      opts = consumer_group_opts(commit_threshold: 1, max_restarts: max_restarts, max_seconds: 60)
      {:ok, consumer_pid} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)

      Process.sleep(3_000)
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "CRASH"}])

      receive do
        {:EXIT, ^consumer_pid, _reason} -> :ok
      after
        30_000 -> flunk("Expected supervisor to shut down after #{max_restarts} restarts")
      end

      # Verify consumer group is no longer running
      refute Process.alive?(consumer_pid)
    end
  end

  describe "async consumer with multiple topics" do
    @tag timeout: 60_000
    test "subscribe to multiple topics, receive from all", %{client: client} do
      topic1 = generate_random_string()
      topic2 = generate_random_string()
      consumer_group = generate_random_string()

      _ = create_topic(client, topic1)
      _ = create_topic(client, topic2)

      opts = consumer_group_opts()
      consumer_pid = start_consumer_group(consumer_group, [topic1, topic2], opts)

      # Wait for consumer to become active
      wait_for_consumer_active(consumer_pid)

      # Produce to both topics
      {:ok, _} = API.produce(client, topic1, 0, [%{value: "from-topic1"}])
      {:ok, _} = API.produce(client, topic2, 0, [%{value: "from-topic2"}])

      # Wait for messages
      received = receive_messages_until(2, 15_000)

      # Should have received from both topics
      assert "from-topic1" in received
      assert "from-topic2" in received
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

      # Produce messages BEFORE starting consumer
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "old-message"}])
      Process.sleep(500)

      opts = consumer_group_opts(auto_offset_reset: :latest)
      consumer_pid = start_consumer_group(consumer_group, [topic_name], opts)

      Process.sleep(5_000)

      # Produce NEW messages after consumer started
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

      # Consumer 1 with unique ID
      opts1 = [
        heartbeat_interval: 1_000,
        session_timeout: 10_000,
        commit_interval: 2_000,
        extra_consumer_args: [test_pid: self(), consumer_id: :consumer1]
      ]

      {:ok, consumer1} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts1)
      Process.sleep(5_000)

      # Produce messages to all partitions
      Enum.each(0..3, fn p -> {:ok, _} = API.produce(client, topic_name, p, [%{value: "phase1-p#{p}"}]) end)

      # Collect - single consumer should handle all 4 partitions
      {partitions_before, consumers_before} = collect_consumer_partitions(4, 10_000)

      assert MapSet.size(MapSet.new(Map.keys(partitions_before))) == 4,
             "Single consumer should handle all 4 partitions"

      assert consumers_before == MapSet.new([:consumer1]),
             "Only consumer1 should be active before rebalance"

      # Consumer 2 with unique ID - triggers rebalance
      opts2 = [
        heartbeat_interval: 1_000,
        session_timeout: 10_000,
        commit_interval: 2_000,
        extra_consumer_args: [test_pid: self(), consumer_id: :consumer2]
      ]

      {:ok, consumer2} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts2)
      Process.sleep(5_000)

      # Produce more messages after rebalance
      Enum.each(0..3, fn p -> {:ok, _} = API.produce(client, topic_name, p, [%{value: "phase2-p#{p}"}]) end)

      # Collect - partitions should be distributed between both consumers
      {partitions_after, consumers_after} = collect_consumer_partitions(4, 10_000)

      assert MapSet.size(MapSet.new(Map.keys(partitions_after))) == 4,
             "Both consumers should cover all 4 partitions"

      assert MapSet.size(consumers_after) == 2,
             "Both consumers should be handling partitions after rebalance, got: #{inspect(consumers_after)}"

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

      # Two consumers with unique IDs
      opts1 = [
        heartbeat_interval: 1_000,
        session_timeout: 10_000,
        commit_interval: 2_000,
        auto_offset_reset: :earliest,
        extra_consumer_args: [test_pid: self(), consumer_id: :consumer1]
      ]

      opts2 = [
        heartbeat_interval: 1_000,
        session_timeout: 10_000,
        commit_interval: 2_000,
        auto_offset_reset: :earliest,
        extra_consumer_args: [test_pid: self(), consumer_id: :consumer2]
      ]

      # Start two consumers - partitions should be split
      {:ok, consumer1} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts1)
      wait_for_consumer_active(consumer1)
      {:ok, consumer2} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts2)
      wait_for_consumer_active(consumer2)
      # Extra settling time for rebalance to complete
      Process.sleep(2_000)

      # Produce messages to all partitions
      Enum.each(0..3, fn p -> {:ok, _} = API.produce(client, topic_name, p, [%{value: "phase1-p#{p}"}]) end)

      # Collect - both consumers should be handling partitions
      {partitions_before, consumers_before} = collect_consumer_partitions(4, 10_000)

      assert MapSet.size(MapSet.new(Map.keys(partitions_before))) == 4,
             "All 4 partitions should receive messages"

      assert MapSet.size(consumers_before) == 2,
             "Both consumers should be handling partitions, got: #{inspect(consumers_before)}"

      # Stop first consumer - triggers rebalance
      Supervisor.stop(consumer1)
      Process.sleep(5_000)

      # Produce more messages - remaining consumer should get all partitions
      Enum.each(0..3, fn p -> {:ok, _} = API.produce(client, topic_name, p, [%{value: "phase2-p#{p}"}]) end)

      # Remaining consumer should handle all 4 partitions
      {partitions_after, consumers_after} = collect_consumer_partitions(4, 15_000)

      assert MapSet.size(MapSet.new(Map.keys(partitions_after))) == 4,
             "All 4 partitions should receive messages after rebalance"

      assert consumers_after == MapSet.new([:consumer2]),
             "Only consumer2 should be handling partitions after consumer1 leaves, got: #{inspect(consumers_after)}"

      refute Process.alive?(consumer1)
      assert Process.alive?(consumer2)

      Supervisor.stop(consumer2)
    end
  end

  describe "long-running async produce/consume" do
    @tag timeout: 120_000
    @tag :long_process
    test "60 second stress test - continuous produce and consume", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      opts = consumer_group_opts(commit_interval: 2_000)
      {:ok, consumer_pid} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)

      # Wait for consumer to be ready
      Process.sleep(5_000)

      # Track statistics
      produced_count = :counters.new(1, [:atomics])
      start_time = System.monotonic_time(:millisecond)
      duration_ms = 60_000

      # Start producer task
      producer_task = Task.async(fn -> produce_loop(client, topic_name, produced_count, start_time, duration_ms) end)

      # Wait for producer to finish
      Task.await(producer_task, 70_000)

      total_produced = :counters.get(produced_count, 1)

      # Wait for remaining messages to be consumed
      Process.sleep(5_000)

      # Collect all messages
      received_messages = collect_all_messages()

      total_received = length(received_messages)

      # Verify we received all messages
      assert total_received > 0
      assert total_produced > 0
      assert total_received == total_produced, "Expected #{total_produced} messages, got #{total_received}"

      # Verify consumer should still be healthy
      assert Process.alive?(consumer_pid)

      Supervisor.stop(consumer_pid)
    end

    @tag timeout: 120_000
    @tag :long_process
    test "60 second test with multiple consumers", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      opts = consumer_group_opts(commit_interval: 2_000)

      {:ok, consumer1} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)
      Process.sleep(2_000)
      {:ok, consumer2} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)

      # Wait for both to join and partition assignment
      Process.sleep(5_000)

      # Track statistics
      produced_count = :counters.new(1, [:atomics])
      start_time = System.monotonic_time(:millisecond)
      duration_ms = 60_000

      # Start producer
      producer_task = Task.async(fn -> produce_loop(client, topic_name, produced_count, start_time, duration_ms) end)

      # Wait for producer to finish
      Task.await(producer_task, 70_000)

      total_produced = :counters.get(produced_count, 1)

      # Wait for remaining messages to be consumed
      Process.sleep(5_000)

      # Collect all messages
      received_messages = collect_all_messages()

      total_received = length(received_messages)

      # Both consumers should still be alive
      assert Process.alive?(consumer1)
      assert Process.alive?(consumer2)

      # Should have received all messages
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

  # Collects messages with consumer tracking - returns {partition_map, consumer_set}
  defp collect_consumer_partitions(count, timeout) do
    collect_consumer_partitions(count, timeout, %{}, MapSet.new())
  end

  defp collect_consumer_partitions(count, _timeout, partitions, consumers) when map_size(partitions) >= count do
    {partitions, consumers}
  end

  defp collect_consumer_partitions(count, timeout, partitions, consumers) do
    receive do
      {:consumer_partition, consumer_id, partition, messages} ->
        updated_partitions = Map.update(partitions, partition, messages, &(&1 ++ messages))
        updated_consumers = MapSet.put(consumers, consumer_id)
        collect_consumer_partitions(count, timeout, updated_partitions, updated_consumers)
    after
      timeout -> {partitions, consumers}
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
    {:ok, consumer_pid} = ConsumerGroup.start_link(AsyncTestConsumer, name, topics, opts)
    Process.unlink(consumer_pid)

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
