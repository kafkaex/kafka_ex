defmodule KafkaEx.Integration.ConsumerGroup.ResilienceTest do
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers
  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.TestConsumers.AsyncTestConsumer

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> stop_safely(client) end)

    {:ok, %{client: client}}
  end

  describe "consumer group resilience - KAFKA-6829 pattern" do
    @tag timeout: 90_000
    test "consumer survives multiple rapid rebalances", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 4)

      # Start first consumer
      opts = consumer_group_opts()
      {:ok, consumer1} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)
      wait_for_consumer_active(consumer1)

      # Rapidly add and remove consumers to trigger rebalances
      Enum.each(1..3, fn i ->
        {:ok, temp_consumer} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)
        Process.sleep(1_000)

        # Produce messages during rebalance
        {:ok, _} = API.produce(client, topic_name, rem(i, 4), [%{value: "rebalance-msg-#{i}"}])

        # Kill the throwaway consumer abruptly (crash-style departure, the KAFKA-6829
        # pattern) rather than a graceful stop: while it is still mid-join its Manager
        # cannot process a graceful shutdown, so GenServer.stop would block the full
        # child shutdown timeout (~25s). Unlink first so the :kill does not reach us.
        Process.unlink(temp_consumer)
        Process.exit(temp_consumer, :kill)

        Process.sleep(1_000)
      end)

      # Wait for consumer1 to stabilize after rebalances
      wait_for_consumer_active(consumer1)

      # Original consumer should still be alive and working
      assert Process.alive?(consumer1), "Consumer should survive rapid rebalances"

      # Produce final message and verify consumer receives it
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "final-message"}])
      # Generous budget: the abruptly-killed temp consumers linger as ghosts until
      # their session times out (~session_timeout), so consumer1 fully reclaims the
      # partitions only after that. The absolute deadline returns as soon as the
      # message arrives (~14s observed).
      received = receive_messages_until_found("final-message", 30_000)
      assert "final-message" in received, "Consumer should receive messages after rebalances"

      stop_safely(consumer1)
    end

    @tag timeout: 60_000
    test "consumer handles unknown_topic_or_partition gracefully", %{client: client} do
      # Start consumer group subscribing to non-existent topic
      nonexistent_topic = "nonexistent-#{generate_random_string()}"
      existing_topic = generate_random_string()
      consumer_group = generate_random_string()

      # Create only one topic initially
      _ = create_topic(client, existing_topic, partitions: 1)

      # Consumer subscribes to both - should gracefully handle missing topic
      opts = consumer_group_opts()

      {:ok, consumer} =
        ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [existing_topic, nonexistent_topic], opts)

      # Wait for consumer to stabilize (it should retry and eventually give up on missing topic)
      wait_for_consumer_active(consumer, 20_000)

      # Consumer should still be alive despite missing topic
      assert Process.alive?(consumer), "Consumer should survive missing topic (KAFKA-6829 pattern)"

      # Produce to existing topic - consumer should receive
      {:ok, _} = API.produce(client, existing_topic, 0, [%{value: "test-message"}])
      received = receive_messages_until(1, 10_000)
      assert "test-message" in received, "Consumer should receive from available topic"

      stop_safely(consumer)
    end

    @tag timeout: 60_000
    test "consumer recovers when coordinator changes during rebalance", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 2)

      # Start two consumers with short heartbeat interval
      opts = [
        heartbeat_interval: 1_000,
        session_timeout: 10_000,
        commit_interval: 2_000,
        extra_consumer_args: [test_pid: self()]
      ]

      {:ok, consumer1} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)
      Process.sleep(3_000)
      {:ok, consumer2} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)
      Process.sleep(5_000)

      # Both consumers should be alive and partitions distributed
      assert Process.alive?(consumer1)
      assert Process.alive?(consumer2)

      # Produce messages and verify receipt
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg-p0"}])
      {:ok, _} = API.produce(client, topic_name, 1, [%{value: "msg-p1"}])

      received = receive_messages_until(2, 10_000)
      assert length(received) == 2, "Both partitions should receive messages"

      stop_safely(consumer1)
      stop_safely(consumer2)
    end

    @tag timeout: 60_000
    test "heartbeat errors trigger rejoin, not crash", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      opts = [
        heartbeat_interval: 1_000,
        session_timeout: 10_000,
        commit_interval: 2_000,
        extra_consumer_args: [test_pid: self()]
      ]

      {:ok, consumer} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)
      Process.sleep(5_000)

      # Send many messages over time to verify continuous operation
      Enum.each(1..5, fn i ->
        {:ok, _} = API.produce(client, topic_name, 0, [%{value: "heartbeat-test-#{i}"}])
        Process.sleep(2_000)
      end)

      # Consumer should still be alive after multiple heartbeat cycles
      assert Process.alive?(consumer), "Consumer should survive through heartbeat cycles"

      received = receive_messages_until(5, 10_000)
      assert length(received) >= 5, "All messages should be received"

      stop_safely(consumer)
    end
  end

  describe "exponential backoff behavior" do
    @tag timeout: 30_000
    test "consumer group starts successfully even with brief topic unavailability", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()

      # Create topic
      _ = create_topic(client, topic_name)

      # Start consumer with aggressive retry settings
      opts = consumer_group_opts()
      {:ok, consumer} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic_name], opts)

      # Should successfully join
      Process.sleep(5_000)
      assert Process.alive?(consumer)

      # Produce and consume to verify working
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "backoff-test"}])
      received = receive_messages_until(1, 5_000)
      assert "backoff-test" in received

      stop_safely(consumer)
    end
  end

  # Helper functions
  #
  # `timeout` is an ABSOLUTE budget for the whole wait, not a per-message idle
  # timeout: we compute a monotonic deadline once and shrink the `receive ... after`
  # window as messages arrive. A per-message reset would loop indefinitely under a
  # steady message stream (e.g. re-delivery during rapid rebalances).
  defp receive_messages_until(count, timeout) do
    receive_messages_until(count, System.monotonic_time(:millisecond) + timeout, [])
  end

  defp receive_messages_until(count, _deadline, acc) when length(acc) >= count, do: acc

  defp receive_messages_until(count, deadline, acc) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      acc
    else
      receive do
        {:messages_received, messages} -> receive_messages_until(count, deadline, acc ++ messages)
      after
        remaining -> acc
      end
    end
  end

  defp receive_messages_until_found(target, timeout) do
    receive_messages_until_found(target, System.monotonic_time(:millisecond) + timeout, [])
  end

  defp receive_messages_until_found(target, deadline, acc) do
    remaining = deadline - System.monotonic_time(:millisecond)

    cond do
      target in acc ->
        acc

      remaining <= 0 ->
        acc

      true ->
        receive do
          {:messages_received, messages} ->
            receive_messages_until_found(target, deadline, acc ++ messages)
        after
          remaining -> acc
        end
    end
  end

  defp consumer_group_opts(opts \\ []) do
    [
      heartbeat_interval: 1_000,
      session_timeout: 10_000,
      commit_interval: 1_000,
      auto_offset_reset: :earliest,
      extra_consumer_args: [test_pid: self()]
    ]
    |> Keyword.merge(opts)
  end
end
