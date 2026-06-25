defmodule KafkaEx.Integration.ConsumerGroup.FatalRejoinRecoveryTest do
  @moduledoc """
  Exercises the H-3 in-place rejoin against the shared integration cluster
  (start it with `./scripts/docker_up.sh`).

  Forces a real `ILLEGAL_GENERATION` by corrupting the running Heartbeat
  process's `generation_id` to a value the broker never issued. The next
  heartbeat is rejected; the heartbeat stops with `{:shutdown, {:rejoin,
  :illegal_generation}}`, which the Manager turns into an in-place rejoin
  (keeping member_id, per `reset_generation/2`) — NOT a crash of the
  one_for_all / max_restarts: 0 consumer-group supervisor.

  Before this fix, `:illegal_generation` reached the Manager as an
  unrecoverable `{:shutdown, {:error, _}}` and stopped it, taking the whole
  member subtree (and triggering a group-wide rebalance) down.
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

    topic = "cg_fatal_rejoin_#{:rand.uniform(1_000_000)}"
    create_topic(client, topic, partitions: 1)
    wait_for_topic_in_metadata(client, topic)

    {:ok, %{client: client, uris: uris, topic: topic}}
  end

  test "a heartbeat :illegal_generation rejoins in place (keeps member_id), stays alive, resumes consuming",
       %{client: client, uris: uris, topic: topic} do
    Process.flag(:trap_exit, true)
    group = "cg_fatal_rejoin_grp_#{:rand.uniform(1_000_000)}"

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

    # Corrupt the live Heartbeat's generation so its next HeartbeatRequest is
    # rejected with ILLEGAL_GENERATION by the real coordinator.
    manager = ConsumerGroup.get_manager_pid(cg)
    heartbeat = :sys.get_state(manager).heartbeat_timer
    assert is_pid(heartbeat) and Process.alive?(heartbeat)

    :sys.replace_state(heartbeat, fn hb_state ->
      %{hb_state | generation_id: hb_state.generation_id + 1_000}
    end)

    # The member rejoins in place: a NEW (valid) generation, the SAME member_id,
    # and the supervisor never went down.
    assert {:ok, gen1} = ConsumerGroupHelpers.wait_for_generation_change(cg, gen0, timeout: 60_000)
    assert gen1 != gen0
    assert ConsumerGroup.member_id(cg) == member0, "illegal_generation must keep member_id"
    assert Process.alive?(cg), "the consumer-group supervisor must survive the rejoin"

    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg, timeout: 60_000)
    assert {:ok, _} = ConsumerGroupHelpers.wait_for_assignments(cg, timeout: 30_000)

    # Consumption resumes after the in-place rejoin.
    {:ok, _} = API.produce(client, topic, 0, [%{value: "after-rejoin"}])
    assert_receive {:messages_received, _}, 30_000
  end
end
