defmodule KafkaEx.Consumer.ConsumerGroup.ManagerFatalRejoinTest do
  @moduledoc """
  Regression for H-3: the broker's fatal consumer-group signals must rejoin (or
  cleanly stop), never crash the Manager.

  Before this fix, `:illegal_generation` / `:fenced_instance_id` reached the
  Manager as `{:shutdown, {:error, _}}` (heartbeat) or an immediate
  `SyncGroupError` raise (sync) — both tore down the one_for_all /
  max_restarts: 0 consumer-group supervisor, escalating one member's stale
  generation into a group-wide rebalance.

  Now:
    * `:illegal_generation` -> rejoin keeping member_id (stale generation only)
    * `:unknown_member_id`  -> rejoin after resetting member_id (coordinator
      forgot us)
    * `:fenced_instance_id` -> clean terminal stop (another member holds the
      static group.instance.id; rejoining would split-brain / be re-fenced)

  Exercised through the real `handle_info/2` and the full Manager start path; no
  private function is exposed for testing.
  """
  use ExUnit.Case, async: false

  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Consumer.ConsumerGroup.Manager.State
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Messages.LeaveGroup
  alias KafkaEx.Test.MockClient
  alias KafkaEx.TestSupport.ProcessHelpers
  alias KafkaEx.TestSupport.TestGenConsumer

  # A pid that has already exited — mirrors reality, where the heartbeat process
  # is dead by the time its `{:EXIT, _, _}` reaches the Manager. Using it as
  # `heartbeat_timer` makes `stop_heartbeat_timer/1` (during rebalance) a no-op
  # via its `Process.alive?/1` guard.
  defp dead_pid do
    {pid, ref} = spawn_monitor(fn -> :ok end)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    end

    pid
  end

  defp heartbeat_state(client, timer) do
    {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)
    on_exit(fn -> ProcessHelpers.stop_safely(sup) end)

    %State{
      client: client,
      supervisor_pid: sup,
      group_name: "g",
      topics: ["t"],
      member_id: "m",
      generation_id: 7,
      session_timeout: 1000,
      session_timeout_padding: 0,
      rebalance_timeout: 1000,
      heartbeat_timer: timer
    }
  end

  describe "heartbeat EXIT routing" do
    test ":rejoin(:illegal_generation) rejoins keeping member_id, not a stop" do
      # join_group fails non-recoverably so the rejoin raises immediately (no
      # retry sleeps, no consumer startup). Reaching join at all proves the EXIT
      # was routed to the rejoin path rather than {:stop, ...}.
      {:ok, client} = MockClient.start_link(%{join_group: {:error, :illegal_generation}})
      on_exit(fn -> ProcessHelpers.stop_safely(client) end)

      timer = dead_pid()
      state = heartbeat_state(client, timer)

      assert_raise KafkaEx.JoinGroupError, fn ->
        Manager.handle_info({:EXIT, timer, {:shutdown, {:rejoin, :illegal_generation}}}, state)
      end

      # :illegal_generation keeps member_id -> rejoin uses "m"
      assert {:join_group, "g", "m"} in MockClient.get_calls(client)
    end

    test ":rejoin(:unknown_member_id) rejoins after resetting member_id" do
      {:ok, client} = MockClient.start_link(%{join_group: {:error, :illegal_generation}})
      on_exit(fn -> ProcessHelpers.stop_safely(client) end)

      timer = dead_pid()
      state = heartbeat_state(client, timer)

      assert_raise KafkaEx.JoinGroupError, fn ->
        Manager.handle_info({:EXIT, timer, {:shutdown, {:rejoin, :unknown_member_id}}}, state)
      end

      calls = MockClient.get_calls(client)
      # :unknown_member_id clears member_id -> rejoin uses "" (broker assigns fresh)
      assert {:join_group, "g", ""} in calls
      refute {:join_group, "g", "m"} in calls
    end

    test ":terminal(:fenced_instance_id) stops the manager cleanly without rejoining" do
      {:ok, client} = MockClient.start_link(%{})
      on_exit(fn -> ProcessHelpers.stop_safely(client) end)

      timer = dead_pid()
      state = heartbeat_state(client, timer)

      assert {:stop, {:shutdown, {:terminal, :fenced_instance_id}}, ^state} =
               Manager.handle_info({:EXIT, timer, {:shutdown, {:terminal, :fenced_instance_id}}}, state)

      refute Enum.any?(MockClient.get_calls(client), &match?({:join_group, _, _}, &1))
    end
  end

  describe "stale heartbeat-timer EXIT routing (#555)" do
    # A {:shutdown, _} EXIT whose pid is NOT the current heartbeat_timer comes
    # from a timer we already swapped out during a rebalance, so it is obsolete
    # by construction. The Manager drops it ({:noreply, state}) instead of acting
    # on a stale signal; only the current timer drives state transitions. For the
    # one terminal case (:fenced_instance_id) the condition is persistent, so the
    # live timer re-detects it and stops on its next cycle.

    test "stale {:terminal, _} is dropped, not stopped or rejoined" do
      {:ok, client} = MockClient.start_link(%{})
      on_exit(fn -> ProcessHelpers.stop_safely(client) end)

      current = dead_pid()
      stale = dead_pid()
      state = heartbeat_state(client, current)

      assert {:noreply, ^state} =
               Manager.handle_info({:EXIT, stale, {:shutdown, {:terminal, :fenced_instance_id}}}, state)

      refute Enum.any?(MockClient.get_calls(client), &match?({:join_group, _, _}, &1))
    end

    test "stale {:rejoin, _} is dropped, no rejoin attempted" do
      # join_group is scripted to fail non-recoverably: if the stale EXIT were
      # (wrongly) routed to the rejoin path, join would run and we'd see a call.
      {:ok, client} = MockClient.start_link(%{join_group: {:error, :illegal_generation}})
      on_exit(fn -> ProcessHelpers.stop_safely(client) end)

      current = dead_pid()
      stale = dead_pid()
      state = heartbeat_state(client, current)

      assert {:noreply, ^state} =
               Manager.handle_info({:EXIT, stale, {:shutdown, {:rejoin, :illegal_generation}}}, state)

      refute Enum.any?(MockClient.get_calls(client), &match?({:join_group, _, _}, &1))
    end

    test "stale {:shutdown, :rebalance} is dropped, no rejoin attempted" do
      {:ok, client} = MockClient.start_link(%{join_group: {:error, :illegal_generation}})
      on_exit(fn -> ProcessHelpers.stop_safely(client) end)

      current = dead_pid()
      stale = dead_pid()
      state = heartbeat_state(client, current)

      assert {:noreply, ^state} =
               Manager.handle_info({:EXIT, stale, {:shutdown, :rebalance}}, state)

      refute Enum.any?(MockClient.get_calls(client), &match?({:join_group, _, _}, &1))
    end

    test "current {:terminal, _} still stops (contrast: only the live timer acts)" do
      {:ok, client} = MockClient.start_link(%{})
      on_exit(fn -> ProcessHelpers.stop_safely(client) end)

      current = dead_pid()
      state = heartbeat_state(client, current)

      assert {:stop, {:shutdown, {:terminal, :fenced_instance_id}}, ^state} =
               Manager.handle_info({:EXIT, current, {:shutdown, {:terminal, :fenced_instance_id}}}, state)
    end
  end

  describe "SyncGroup error routing" do
    # Drives a real Manager against a scripted MockClient (the :client injection
    # seam): JoinGroup succeeds (leader_id != member_id, so the member skips
    # assignment and goes straight to SyncGroup), then SyncGroup always returns
    # the error under test.
    #
    # Each {:sync_error, _} rebalance is captured live from telemetry (counter +
    # the emitted member_id forwarded to the test pid) so the reset-vs-keep
    # evidence survives the Manager terminating its client on give-up.
    defp start_manager_with_sync_error(sync_error) do
      Process.flag(:trap_exit, true)
      test_pid = self()
      rejoins = :counters.new(1, [:atomics])
      handler_id = {__MODULE__, make_ref()}

      :telemetry.attach(
        handler_id,
        [:kafka_ex, :consumer, :rebalance],
        fn _event, _measurements, %{reason: reason, member_id: member_id}, _ ->
          case reason do
            {:sync_error, _} ->
              :counters.add(rejoins, 1, 1)
              send(test_pid, {:rejoin_member_id, member_id})

            _ ->
              :ok
          end
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      term_handler_id = {__MODULE__, :member_terminated, make_ref()}

      :telemetry.attach(
        term_handler_id,
        [:kafka_ex, :consumer, :member_terminated],
        fn _event, _measurements, %{reason: reason}, _ -> send(test_pid, {:member_terminated, reason}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(term_handler_id) end)

      {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)
      on_exit(fn -> ProcessHelpers.stop_safely(sup) end)

      {:ok, mock} =
        MockClient.start_link(%{
          join_group: {:ok, %JoinGroup{member_id: "m1", leader_id: "m2", generation_id: 1, members: []}},
          sync_group: {:error, sync_error},
          leave_group: {:ok, %LeaveGroup{}}
        })

      opts = [
        supervisor_pid: sup,
        client: mock,
        heartbeat_interval: 60_000,
        session_timeout: 10_000,
        sync_retry_backoff_ms: 5
      ]

      {:ok, manager} =
        Manager.start_link({{GenConsumer, TestGenConsumer}, "fatal-sync-group", ["t"], opts})

      {manager, rejoins}
    end

    test ":illegal_generation rejoins in place (keeps member_id) rather than crashing, bounded" do
      {manager, rejoins} = start_manager_with_sync_error(:illegal_generation)

      # Rejoins (does NOT raise SyncGroupError immediately); :illegal_generation
      # keeps member_id, so every rejoin still carries "m1".
      assert_receive {:rejoin_member_id, "m1"}, 30_000

      # The bound still escalates a persistent failure to give-up.
      assert_receive {:EXIT, ^manager, {%KafkaEx.SyncGroupRetriesExhaustedError{}, _stacktrace}}, 30_000
      assert :counters.get(rejoins, 1) == 3

      # The give-up raise is a permanent death -> member_terminated fires.
      assert_receive {:member_terminated, {:crashed, KafkaEx.SyncGroupRetriesExhaustedError}}, 30_000
    end

    test ":unknown_member_id rejoins after resetting member_id, bounded" do
      {manager, rejoins} = start_manager_with_sync_error(:unknown_member_id)

      # The give-up EXIT only fires after all 3 bounded rejoins, so once we have
      # it every {:rejoin_member_id, _} telemetry message is already delivered.
      assert_receive {:EXIT, ^manager, {%KafkaEx.SyncGroupRetriesExhaustedError{}, _stacktrace}}, 30_000
      assert :counters.get(rejoins, 1) == 3

      # :unknown_member_id clears member_id before every rejoin -> "" is emitted,
      # never "m1".
      assert_received {:rejoin_member_id, ""}
      refute_received {:rejoin_member_id, "m1"}
    end

    test ":fenced_instance_id stops terminally without rejoining or raising" do
      {manager, rejoins} = start_manager_with_sync_error(:fenced_instance_id)

      assert_receive {:EXIT, ^manager, {:shutdown, {:terminal, :fenced_instance_id}}}, 30_000
      assert :counters.get(rejoins, 1) == 0
      assert_receive {:member_terminated, :fenced_instance_id}, 30_000
    end

    test ":group_authorization_failed stops terminally without rejoining or raising" do
      {manager, rejoins} = start_manager_with_sync_error(:group_authorization_failed)

      assert_receive {:EXIT, ^manager, {:shutdown, {:terminal, :group_authorization_failed}}}, 30_000
      assert :counters.get(rejoins, 1) == 0
      assert_receive {:member_terminated, :group_authorization_failed}, 30_000
    end
  end
end
