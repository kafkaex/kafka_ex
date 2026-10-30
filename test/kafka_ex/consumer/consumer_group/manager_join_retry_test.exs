defmodule KafkaEx.Consumer.ConsumerGroup.ManagerJoinRetryTest do
  @moduledoc """
  Unit test for the Manager's JoinGroup retry loop (`handle_recoverable_join_error/4`).

  Drives a real Manager against a scripted `MockClient` (via the `:client` injection
  seam in `init/1`): JoinGroup always returns a recoverable `:not_coordinator`.

  A recoverable join error must retry until it clears. It previously gave up after
  `@max_join_retries` (6) attempts, about 25 s, and raised
  `JoinGroupRetriesExhaustedError` — which takes the whole consumer group down via
  the `one_for_all` supervisor above it. A coordinator move regularly lasts longer
  than that, so giving up caused the disruption the retries exist to avoid.

  Unlike a sync retry, a join retry re-sends JoinGroup rather than driving a full
  rejoin, and its backoff escalates to a cap, so an unbounded loop is rate-limited
  and has no side effects. The sync loop stays bounded; see
  `ManagerSyncRecoveryTest`.
  """
  use ExUnit.Case, async: false

  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Messages.LeaveGroup
  alias KafkaEx.Messages.SyncGroup
  alias KafkaEx.Test.MockClient
  alias KafkaEx.TestSupport.TestGenConsumer

  test "keeps retrying a recoverable JoinGroup error instead of giving up" do
    Process.flag(:trap_exit, true)

    {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)
    on_exit(fn -> stop_safely(sup) end)

    {:ok, mock} =
      MockClient.start_link(%{
        join_group: {:error, :not_coordinator}
      })

    opts = [
      supervisor_pid: sup,
      client: mock,
      heartbeat_interval: 60_000,
      session_timeout: 10_000,
      # keep the unit test fast — the real ladder is 1s doubling to 10s
      join_retry_base_delay_ms: 5,
      join_retry_max_delay_ms: 5
    ]

    {:ok, manager} =
      Manager.start_link({{GenConsumer, TestGenConsumer}, "join-retry-group", ["t"], opts})

    # Well past the old 6-attempt bound: at 5ms per attempt this is hundreds of
    # retries. The manager must still be alive, with no exhaustion exit.
    refute_receive {:EXIT, ^manager, _reason}, 2_000

    assert Process.alive?(manager)
  end

  @tag capture_log: true
  test "joins successfully after more than 1025 recoverable failures" do
    Process.flag(:trap_exit, true)

    {:ok, sup} = Supervisor.start_link([], strategy: :one_for_one)
    on_exit(fn -> stop_safely(sup) end)

    {:ok, mock} =
      MockClient.start_link(%{
        join_group: {:error, :not_coordinator},
        sync_group: {:ok, %SyncGroup{partition_assignments: []}},
        leave_group: {:ok, %LeaveGroup{}}
      })

    opts = [
      supervisor_pid: sup,
      client: mock,
      heartbeat_interval: 60_000,
      session_timeout: 10_000,
      join_retry_base_delay_ms: 0,
      join_retry_max_delay_ms: 0
    ]

    {:ok, manager} =
      Manager.start_link({{GenConsumer, TestGenConsumer}, "long-join-retry-group", ["t"], opts})

    MockClient.wait_for_calls(mock, 1100)

    :ok =
      MockClient.put_response(
        mock,
        :join_group,
        {:ok, %JoinGroup{member_id: "m1", leader_id: "m2", generation_id: 7, members: []}}
      )

    assert GenServer.call(manager, :generation_id) == 7
    assert GenServer.call(manager, :member_id) == "m1"
    assert {:sync_group, "long-join-retry-group", 7, "m1"} in MockClient.get_calls(mock)

    stop_safely(manager)
  end
end
