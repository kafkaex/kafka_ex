defmodule KafkaEx.Integration.ConsumerGroup.TopicDeletionResilienceTest do
  @moduledoc """
  Deleting one of a group's subscribed topics must not take down consumption
  of the group's other topics (metadata-tracking fix: narrowing costs latency, not data).
  """
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import ExUnit.CaptureLog
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

  describe "one subscribed topic deleted mid-flight" do
    @tag timeout: 90_000
    test "surviving topics keep being consumed and the group stays alive", %{client: client} do
      topic1 = generate_random_string()
      topic2 = generate_random_string()
      topic3 = generate_random_string()
      consumer_group = generate_random_string()

      _ = create_topic(client, topic1)
      _ = create_topic(client, topic2)
      _ = create_topic(client, topic3)

      opts = [
        heartbeat_interval: 1_000,
        session_timeout: 10_000,
        commit_interval: 1_000,
        auto_offset_reset: :earliest,
        extra_consumer_args: [test_pid: self()]
      ]

      {:ok, consumer_pid} = ConsumerGroup.start_link(AsyncTestConsumer, consumer_group, [topic1, topic2, topic3], opts)
      Process.unlink(consumer_pid)
      on_exit(fn -> stop_safely(consumer_pid) end)

      wait_for_consumer_active(consumer_pid)

      # Baseline: prove the group is healthy across all three topics before touching anything.
      {:ok, _} = API.produce(client, topic1, 0, [%{value: "baseline-1"}])
      {:ok, _} = API.produce(client, topic2, 0, [%{value: "baseline-2"}])
      {:ok, _} = API.produce(client, topic3, 0, [%{value: "baseline-3"}])

      baseline = receive_messages_until(3, 15_000)
      assert "baseline-1" in baseline
      assert "baseline-2" in baseline
      assert "baseline-3" in baseline

      log =
        capture_log(fn ->
          {:ok, _} = API.delete_topic(client, topic2)
          Process.sleep(1_000)

          {:ok, _} = API.produce(client, topic1, 0, [%{value: "after-delete-1"}])
          {:ok, _} = API.produce(client, topic3, 0, [%{value: "after-delete-3"}])

          survivors = receive_messages_until(2, 20_000)
          assert "after-delete-1" in survivors
          assert "after-delete-3" in survivors
        end)

      # A bounded warning about the deleted topic is expected and allowed; a flood of
      # errors is not — count lines rather than assert on the warning's presence/timing.
      error_lines = log |> String.split("\n") |> Enum.count(&(&1 =~ "[error]"))
      assert error_lines < 10, "expected no error storm, got #{error_lines} error lines:\n#{log}"

      assert Process.alive?(consumer_pid)
    end
  end

  defp receive_messages_until(count, timeout), do: receive_messages_until(count, timeout, [])
  defp receive_messages_until(count, _timeout, acc) when length(acc) >= count, do: acc

  defp receive_messages_until(count, timeout, acc) do
    receive do
      {:messages_received, messages} -> receive_messages_until(count, timeout, acc ++ messages)
    after
      timeout -> acc
    end
  end
end
