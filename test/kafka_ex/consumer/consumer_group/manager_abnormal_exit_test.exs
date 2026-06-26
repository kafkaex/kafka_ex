defmodule KafkaEx.Consumer.ConsumerGroup.ManagerAbnormalExitTest do
  @moduledoc """
  Regression for the abnormal-EXIT teardown gap: when the linked Heartbeat dies
  with a reason no structured clause matched (e.g. :killed, or a crash before
  heartbeat.ex's try-wrapper), the Manager previously raised FunctionClauseError
  and was torn down by the one_for_all / max_restarts: 0 consumer-group
  supervisor with no restart.

  Now an abnormal EXIT from the CURRENT heartbeat drives a bounded rejoin
  (keeping member_id — no protocol reason to reset it), and an abnormal EXIT
  from a STALE (already-replaced) heartbeat is dropped. Exercised through the
  real handle_info/2; no private function is exposed for testing.
  """
  use ExUnit.Case, async: false

  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Consumer.ConsumerGroup.Manager.State
  alias KafkaEx.Test.MockClient

  # A pid that has already exited — mirrors reality, where the heartbeat is dead
  # by the time its {:EXIT, _, _} reaches the Manager. As heartbeat_timer it also
  # makes stop_heartbeat_timer/1 a no-op via its Process.alive?/1 guard.
  defp dead_pid do
    {pid, ref} = spawn_monitor(fn -> :ok end)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    end

    pid
  end

  test "an abnormal EXIT from the current heartbeat rejoins (keeps member_id), not a crash" do
    # join_group fails non-recoverably so the rejoin raises immediately (no retry
    # sleeps, no consumer startup). Reaching join at all proves the abnormal EXIT
    # was routed to the rejoin path rather than crashing the Manager.
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
      Manager.handle_info({:EXIT, timer, :killed}, state)
    end

    # An abnormal crash carries no identity-reset semantics -> keep member_id.
    assert {:join_group, "g", "m"} in MockClient.get_calls(client)
  end

  test "an abnormal EXIT from a stale (non-current) heartbeat is dropped" do
    # No client/supervisor: a spurious rebalance here would crash on nil
    # supervisor_pid, so {:noreply, state} also proves nothing was acted on.
    current = dead_pid()
    stale = dead_pid()

    state = %State{group_name: "g", member_id: "m", generation_id: 1, heartbeat_timer: current}

    assert {:noreply, ^state} = Manager.handle_info({:EXIT, stale, :killed}, state)
  end
end
