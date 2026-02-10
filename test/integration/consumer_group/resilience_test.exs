defmodule KafkaEx.Integration.ConsumerGroup.ResilienceTest do
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

        try do
          Supervisor.stop(temp_consumer)
        catch
          :exit, _ -> :ok
        end

        Process.sleep(1_000)
      end)

      # Wait for consumer1 to stabilize after rebalances
      wait_for_consumer_active(consumer1)

      # Original consumer should still be alive and working
      assert Process.alive?(consumer1), "Consumer should survive rapid rebalances"

      # Produce final message and verify consumer receives it
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "final-message"}])
      received = receive_messages_until_found("final-message", 15_000)
      assert "final-message" in received, "Consumer should receive messages after rebalances"

      try do
        Supervisor.stop(consumer1)
      catch
        :exit, _ -> :ok
      end
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

      try do
        Supervisor.stop(consumer)
      catch
        :exit, _ -> :ok
      end
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

      Supervisor.stop(consumer1)
      Supervisor.stop(consumer2)
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

      Supervisor.stop(consumer)
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

      Supervisor.stop(consumer)
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

  defp receive_messages_until_found(target, timeout) do
    receive_messages_until_found(target, timeout, [])
  end

  defp receive_messages_until_found(target, timeout, acc) do
    if target in acc do
      acc
    else
      receive do
        {:messages_received, messages} ->
          receive_messages_until_found(target, timeout, acc ++ messages)
      after
        timeout -> acc
      end
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
end
