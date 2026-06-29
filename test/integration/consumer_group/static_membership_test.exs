defmodule KafkaEx.Integration.ConsumerGroup.StaticMembershipTest do
  @moduledoc """
  KIP-345 static membership against the shared integration cluster
  (start it with `./scripts/docker_up.sh`).

  A static member (group_instance_id set) keeps its partition assignment across
  a graceful restart within session_timeout — the broker does NOT rebalance, so
  the generation is unchanged. The broker may issue a fresh member_id when the
  restarted process rejoins with an empty member_id; the preserved invariant is
  the assignment/generation (no rebalance), not member_id identity. The dynamic
  control proves the retention is caused by static membership: without
  group_instance_id the graceful stop sends LeaveGroup and the restart advances
  the generation.
  """
  use ExUnit.Case, async: false

  @moduletag :consumer_group
  @moduletag timeout: 120_000

  import KafkaEx.IntegrationHelpers

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

    topic = "cg_static_#{:rand.uniform(1_000_000)}"
    create_topic(client, topic, partitions: 1)
    wait_for_topic_in_metadata(client, topic)

    {:ok, %{uris: uris, topic: topic}}
  end

  defp base_opts(uris) do
    [
      uris: uris,
      heartbeat_interval: 1_000,
      session_timeout: 30_000,
      commit_interval: 1_000,
      auto_offset_reset: :earliest
    ]
  end

  defp start_active(group, topic, opts) do
    {:ok, cg} = ConsumerGroup.start_link(TestGenConsumer, group, [topic], opts)
    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg, timeout: 60_000)
    assert {:ok, _} = ConsumerGroupHelpers.wait_for_assignments(cg, timeout: 30_000)
    cg
  end

  test "a static member keeps its assignment and generation across a restart (no rebalance)",
       %{uris: uris, topic: topic} do
    group = "cg_static_grp_#{:rand.uniform(1_000_000)}"
    opts = Keyword.put(base_opts(uris), :group_instance_id, "inst-1")

    cg1 = start_active(group, topic, opts)
    gen0 = ConsumerGroup.generation_id(cg1)
    member0 = ConsumerGroup.member_id(cg1)
    assert is_integer(gen0) and is_binary(member0) and member0 != ""

    # Graceful stop: a static member does NOT send LeaveGroup, so the broker
    # holds the assignment until session_timeout.
    ConsumerGroupHelpers.stop_consumer_group(cg1)

    # Restart with the SAME instance id, well within session_timeout.
    cg2 = start_active(group, topic, opts)
    on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg2) end)

    # KIP-345 preserves the ASSIGNMENT and GENERATION (no rebalance), not
    # member_id identity: a process restarting with an empty member_id is
    # recognized via group_instance_id and keeps the assignment, but the broker
    # (Kafka 3.1.0) issues a fresh member_id. The generation staying put is the
    # proof no rebalance occurred.
    assert ConsumerGroup.generation_id(cg2) == gen0,
           "a static rejoin within session_timeout must NOT trigger a rebalance"

    member2 = ConsumerGroup.member_id(cg2)

    assert is_binary(member2) and member2 != "",
           "the rejoined static member must still hold a valid member_id"
  end

  test "a dynamic member triggers a rebalance across a restart (negative control)",
       %{uris: uris, topic: topic} do
    group = "cg_dynamic_grp_#{:rand.uniform(1_000_000)}"
    opts = base_opts(uris)

    cg1 = start_active(group, topic, opts)
    gen0 = ConsumerGroup.generation_id(cg1)

    # Graceful stop: a dynamic member sends LeaveGroup -> rebalance.
    ConsumerGroupHelpers.stop_consumer_group(cg1)

    cg2 = start_active(group, topic, opts)
    on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg2) end)

    assert ConsumerGroup.generation_id(cg2) > gen0,
           "a dynamic restart must advance the generation (rebalance occurred)"
  end
end
