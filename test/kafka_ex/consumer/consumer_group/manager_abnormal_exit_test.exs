defmodule KafkaEx.Consumer.ConsumerGroup.ManagerAbnormalExitTest.JoinResetMockClient do
  @moduledoc false
  # A client whose JoinGroup response is keyed on the member_id: the stale "m"
  # gets :unknown_member_id (broker forgot us); the fresh "" rejoin (after a
  # reset) gets a non-recoverable error so the retry loop halts after one
  # reset+retry instead of exhausting @max_join_retries.
  use GenServer

  def start_link, do: GenServer.start_link(__MODULE__, nil)
  def get_calls(pid), do: GenServer.call(pid, :get_calls)

  def init(_), do: {:ok, []}

  def handle_call(:get_calls, _from, calls), do: {:reply, Enum.reverse(calls), calls}

  def handle_call({:join_group, group, "m", _opts}, _from, calls) do
    {:reply, {:error, :unknown_member_id}, [{:join_group, group, "m"} | calls]}
  end

  def handle_call({:join_group, group, member_id, _opts}, _from, calls) do
    {:reply, {:error, :illegal_generation}, [{:join_group, group, member_id} | calls]}
  end
end

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

  defp terminate_state do
    {:ok, client} = MockClient.start_link(%{leave_group: {:ok, %KafkaEx.Messages.LeaveGroup{}}})
    %State{client: client, group_name: "g", member_id: "m", generation_id: 1}
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

  alias KafkaEx.Consumer.ConsumerGroup.ManagerAbnormalExitTest.JoinResetMockClient

  test "a JoinGroup :unknown_member_id resets member_id and rejoins fresh, not an immediate crash" do
    # The broker forgot us (session expired during the outage that killed the
    # heartbeat): JoinGroup with the stale "m" returns :unknown_member_id. The
    # fix must RESET member_id and rejoin with "" instead of raising on the spot.
    # Before the fix, :unknown_member_id raised JoinGroupError immediately and ""
    # was never tried — re-creating the no-restart teardown via the join path.
    {:ok, client} = JoinResetMockClient.start_link()
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

    calls = JoinResetMockClient.get_calls(client)
    assert {:join_group, "g", "m"} in calls, "the stale member_id must be tried first"
    assert {:join_group, "g", ""} in calls, ":unknown_member_id must reset member_id and rejoin fresh"
  end

  describe "member_terminated telemetry (via terminate/2)" do
    # terminate/2 is the single choke point every permanent member death flows
    # through (clean {:stop, _} returns AND callback raises); a graceful
    # :normal/:shutdown must stay silent. State carries a real MockClient so the
    # terminate path's leave/unlink/stop run.
    setup do
      test_pid = self()
      handler_id = {__MODULE__, :member_terminated, make_ref()}

      :telemetry.attach(
        handler_id,
        [:kafka_ex, :consumer, :member_terminated],
        fn _event, measurements, metadata, _ -> send(test_pid, {:member_terminated, measurements, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)
      :ok
    end

    test "terminal error emits the terminal cause" do
      Manager.terminate({:shutdown, {:terminal, :fenced_instance_id}}, terminate_state())

      assert_receive {:member_terminated, %{count: 1}, %{group_id: "g", member_id: "m", reason: :fenced_instance_id}}
    end

    test "non-recoverable heartbeat {:error, _} emits {:error, _}" do
      Manager.terminate({:shutdown, {:error, :some_fatal}}, terminate_state())

      assert_receive {:member_terminated, _, %{reason: {:error, :some_fatal}}}
    end

    test "client death emits {:client_died, _}" do
      Manager.terminate({:shutdown, {:client_died, :killed}}, terminate_state())

      assert_receive {:member_terminated, _, %{reason: {:client_died, :killed}}}
    end

    test "a callback raise / give-up emits {:crashed, module}" do
      Manager.terminate({%RuntimeError{message: "boom"}, []}, terminate_state())

      assert_receive {:member_terminated, _, %{reason: {:crashed, RuntimeError}}}
    end

    test "a graceful :shutdown does NOT emit" do
      Manager.terminate(:shutdown, terminate_state())

      refute_receive {:member_terminated, _, _}, 100
    end

    test "a graceful :normal does NOT emit" do
      Manager.terminate(:normal, terminate_state())

      refute_receive {:member_terminated, _, _}, 100
    end
  end
end
