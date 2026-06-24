defmodule KafkaEx.Consumer.ConsumerGroup.ManagerRecoverableErrorTest do
  @moduledoc """
  Regression for PR-2: once the client preserves the real transport atom (C-1), a
  TCP-level coordinator failure reaches the manager as `:closed` / `:not_connected`
  instead of `:unknown`. The heartbeat-exit handler must treat those as recoverable
  (-> rejoin), not stop the manager — previously they arrived as `:unknown`, which
  was already recoverable. Exercised through the real `handle_info/2` path; no
  private function is exposed for testing.

  Flagged independently by the Kafka-protocol and Java-client code reviews.
  """
  use ExUnit.Case, async: false

  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Consumer.ConsumerGroup.Manager.State
  alias KafkaEx.Test.MockClient

  defp heartbeat_exit(timer, reason), do: {:EXIT, timer, {:shutdown, {:error, reason}}}

  # A pid that has already exited — the heartbeat-error EXIT clause only acts on
  # the CURRENT heartbeat_timer, so the EXIT pid must equal state.heartbeat_timer.
  # A dead pid keeps stop_heartbeat_timer/1 (during rebalance) a no-op.
  defp dead_pid do
    {pid, ref} = spawn_monitor(fn -> :ok end)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    end

    pid
  end

  test "a fatal heartbeat error stops the manager" do
    timer = dead_pid()
    state = %State{group_name: "g", member_id: "m", generation_id: 1, heartbeat_timer: timer}

    assert {:stop, {:shutdown, {:error, :topic_authorization_failed}}, ^state} =
             Manager.handle_info(heartbeat_exit(timer, :topic_authorization_failed), state)
  end

  test "a :closed transport error is recoverable and drives a rejoin, not a stop" do
    # join_group fails non-recoverably so the rejoin raises immediately (no retry
    # backoff sleeps, no consumer startup). Reaching join at all proves :closed was
    # routed to the recoverable rejoin path rather than {:stop, ...}.
    {:ok, client} = MockClient.start_link(%{join_group: {:error, :illegal_generation}})
    on_exit(fn -> stop_safely(client) end)

    {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)
    on_exit(fn -> stop_safely(sup) end)

    timer = dead_pid()

    state = %State{
      client: client,
      supervisor_pid: sup,
      group_name: "g",
      topics: ["t"],
      member_id: "m",
      generation_id: 1,
      session_timeout: 1000,
      session_timeout_padding: 0,
      rebalance_timeout: 1000,
      heartbeat_timer: timer
    }

    assert_raise KafkaEx.JoinGroupError, fn ->
      Manager.handle_info(heartbeat_exit(timer, :closed), state)
    end

    # the recoverable path actually attempted a rejoin against the coordinator
    assert {:join_group, "g", "m"} in MockClient.get_calls(client)
  end

  test "a stale heartbeat error EXIT (non-current timer) is dropped, not acted on" do
    # A heartbeat that was already replaced during a rebalance can deliver its
    # error EXIT late. It must NOT drive a rebalance off the current state — it is
    # dropped by the stale-timer clause. (No client/supervisor needed: a spurious
    # rebalance here would crash on the nil supervisor_pid.)
    current = dead_pid()
    stale = dead_pid()

    state = %State{group_name: "g", member_id: "m", generation_id: 1, heartbeat_timer: current}

    assert {:noreply, ^state} =
             Manager.handle_info(heartbeat_exit(stale, :coordinator_not_available), state)
  end
end
