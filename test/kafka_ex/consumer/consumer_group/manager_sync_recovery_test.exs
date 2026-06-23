defmodule KafkaEx.Consumer.ConsumerGroup.ManagerSyncRecoveryTest do
  @moduledoc """
  Unit test for the Manager's SyncGroup give-up bound (`handle_sync_error/3`).

  Drives a real Manager against a scripted `MockClient` (via the `:client`
  injection seam in `init/1`): JoinGroup succeeds, SyncGroup always returns a
  recoverable `:unknown`. The fix routes a recoverable sync error to a rejoin
  rather than crashing, but bounds the rejoin loop at `@max_sync_retries`
  consecutive failures so a *persistent* failure escalates (raises
  `SyncGroupRetriesExhaustedError` → supervisor rebuild) instead of looping forever.

  This also exercises the core of the recovery behavior: each recoverable sync
  error triggers a *rejoin, not a crash* (3 attempts here, each emitting a
  `{:sync_error, _}` rebalance), and pins the bound — give up on the next
  failure. End-to-end rebalance convergence (the `:rebalance_in_progress` path)
  is covered by the integration test
  `KafkaEx.Integration.ConsumerGroup.RebalanceRecoveryTest`.
  """
  use ExUnit.Case, async: false

  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Test.MockClient
  alias KafkaEx.TestSupport.TestGenConsumer

  test "gives up after a bounded number of consecutive recoverable SyncGroup failures" do
    Process.flag(:trap_exit, true)

    # Each rejoin attempt emits a {:sync_error, _} rebalance. Counted via a shared
    # atomics ref (survives the manager terminating its client on give-up).
    rejoins = :counters.new(1, [:atomics])
    handler_id = {:give_up_test, make_ref()}

    :telemetry.attach(
      handler_id,
      [:kafka_ex, :consumer, :rebalance],
      fn _event, _measurements, %{reason: reason}, _ ->
        case reason do
          {:sync_error, _} -> :counters.add(rejoins, 1, 1)
          _ -> :ok
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    # Bare supervisor: rebalance/2 -> stop_consumer/1 calls
    # Supervisor.terminate_child(sup, :consumer), which returns {:error, :not_found}
    # here (no consumer child) -> :ok. No real consumers start because sync never
    # succeeds.
    {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)

    on_exit(fn ->
      try do
        if Process.alive?(sup), do: Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    {:ok, mock} =
      MockClient.start_link(%{
        # leader_id != member_id → this member is not the leader, so it skips
        # partition assignment/metadata and goes straight to SyncGroup.
        join_group: {:ok, %JoinGroup{member_id: "m1", leader_id: "m2", generation_id: 1, members: []}},
        sync_group: {:error, :unknown}
      })

    opts = [
      supervisor_pid: sup,
      client: mock,
      heartbeat_interval: 60_000,
      session_timeout: 10_000,
      # keep the unit test fast — no real 1s backoffs between rejoin attempts
      sync_retry_backoff_ms: 5
    ]

    {:ok, manager} =
      Manager.start_link({{GenConsumer, TestGenConsumer}, "give-up-group", ["t"], opts})

    # The manager must terminate (give up) with SyncGroupRetriesExhaustedError
    # rather than rejoining forever. (Backoff is set to 5ms above, so this is fast.)
    assert_receive {:EXIT, ^manager, {%KafkaEx.SyncGroupRetriesExhaustedError{}, _stacktrace}},
                   30_000

    # @max_sync_retries (3) rejoin attempts each emit a {:sync_error, _} rebalance;
    # the next consecutive failure gives up (raises) without rebalancing.
    assert :counters.get(rejoins, 1) == 3,
           "expected the rejoin loop bounded to 3 attempts, got #{:counters.get(rejoins, 1)}"
  end
end
