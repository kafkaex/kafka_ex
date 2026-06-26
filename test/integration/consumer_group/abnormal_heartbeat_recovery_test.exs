defmodule KafkaEx.Integration.ConsumerGroup.AbnormalHeartbeatRecoveryTest do
  @moduledoc """
  Exercises the abnormal-EXIT recovery against the shared integration cluster
  (start it with `./scripts/docker_up.sh`).

  Hard-kills the live Heartbeat process (Process.exit/2 :kill) — an abnormal
  reason heartbeat.ex cannot catch. The Manager must treat it as a lost
  heartbeat and rejoin in place (keeping member_id), NOT crash the one_for_all /
  max_restarts: 0 consumer-group supervisor.

  Before this fix, the killed heartbeat's {:EXIT, _, :killed} matched no Manager
  clause -> FunctionClauseError -> the whole member subtree went down with no
  restart.
  """
  use ExUnit.Case, async: false

  @moduletag :consumer_group
  @moduletag timeout: 120_000

  import KafkaEx.IntegrationHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.TestSupport.ConsumerGroupHelpers
  alias KafkaEx.TestSupport.ProcessHelpers
  alias KafkaEx.TestSupport.TestGenConsumer

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    uris = Keyword.fetch!(args, :uris)

    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> ProcessHelpers.stop_safely(client) end)

    topic = "cg_abnormal_hb_#{:rand.uniform(1_000_000)}"
    create_topic(client, topic, partitions: 1)
    wait_for_topic_in_metadata(client, topic)

    {:ok, %{client: client, uris: uris, topic: topic}}
  end

  test "a hard-killed heartbeat rejoins in place (keeps member_id), stays alive, resumes consuming",
       %{client: client, uris: uris, topic: topic} do
    Process.flag(:trap_exit, true)
    group = "cg_abnormal_hb_grp_#{:rand.uniform(1_000_000)}"

    opts = [
      uris: uris,
      heartbeat_interval: 1_000,
      session_timeout: 30_000,
      commit_interval: 1_000,
      auto_offset_reset: :earliest,
      extra_consumer_args: %{test_pid: self()}
    ]

    {:ok, cg} = ConsumerGroup.start_link(TestGenConsumer, group, [topic], opts)
    on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg) end)

    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg, timeout: 60_000)
    assert {:ok, _} = ConsumerGroupHelpers.wait_for_assignments(cg, timeout: 30_000)

    gen0 = ConsumerGroup.generation_id(cg)
    member0 = ConsumerGroup.member_id(cg)
    assert is_integer(gen0) and is_binary(member0) and member0 != ""

    # Consuming before the fault.
    {:ok, _} = API.produce(client, topic, 0, [%{value: "before-fault"}])
    assert_receive {:messages_received, _}, 30_000

    # Hard-kill the live Heartbeat: an abnormal reason heartbeat.ex cannot catch.
    manager = ConsumerGroup.get_manager_pid(cg)
    heartbeat = :sys.get_state(manager).heartbeat_timer
    assert is_pid(heartbeat) and Process.alive?(heartbeat)
    Process.exit(heartbeat, :kill)

    # The member rejoins in place: a NEW generation, the SAME member_id, and the
    # supervisor never went down.
    assert {:ok, gen1} = ConsumerGroupHelpers.wait_for_generation_change(cg, gen0, timeout: 60_000)
    assert gen1 != gen0
    assert ConsumerGroup.member_id(cg) == member0, "an abnormal heartbeat crash must keep member_id"
    assert Process.alive?(cg), "the consumer-group supervisor must survive the rejoin"

    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg, timeout: 60_000)
    assert {:ok, _} = ConsumerGroupHelpers.wait_for_assignments(cg, timeout: 30_000)

    # Consumption resumes after the in-place rejoin.
    {:ok, _} = API.produce(client, topic, 0, [%{value: "after-rejoin"}])
    assert_receive {:messages_received, _}, 30_000
  end
end
