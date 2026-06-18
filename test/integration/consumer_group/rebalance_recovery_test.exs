defmodule KafkaEx.Integration.ConsumerGroup.RebalanceRecoveryTest do
  @moduledoc """
  Exercises a real multi-member rebalance against the shared integration cluster
  (start it with `./scripts/docker_up.sh`).

  When a second member joins the group, the first member's in-flight SyncGroup /
  Heartbeat receives `:rebalance_in_progress`. With the `sync_group` /
  `heartbeat` retryable predicates that error bubbles straight to the manager's
  rejoin (instead of being blind-retried in the client loop), and both members
  converge to a clean, disjoint partition split. Regression that the predicate
  change does not break normal rebalance convergence.
  """
  use ExUnit.Case, async: false

  @moduletag :consumer_group
  @moduletag timeout: 180_000

  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.TestSupport.ConsumerGroupHelpers
  alias KafkaEx.TestSupport.TestGenConsumer

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    uris = Keyword.fetch!(args, :uris)

    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)

    topic = "cg_rebalance_recovery_#{:rand.uniform(1_000_000)}"
    create_topic(client, topic, partitions: 2)
    wait_for_topic_in_metadata(client, topic)

    {:ok, %{uris: uris, topic: topic}}
  end

  test "a second member joining converges both members to a disjoint partition split", %{
    uris: uris,
    topic: topic
  } do
    Process.flag(:trap_exit, true)
    group = "cg_rebalance_recovery_grp_#{:rand.uniform(1_000_000)}"

    {:ok, cg1} = start_member(uris, topic, group)
    on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg1) end)

    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg1, timeout: 60_000)
    assert {:ok, _} = ConsumerGroupHelpers.wait_for_assignments(cg1, timeout: 30_000)

    # Second member joins the SAME group → triggers a rebalance; member 1's
    # in-flight SyncGroup/Heartbeat sees :rebalance_in_progress, which now
    # bubbles to the manager's rejoin instead of being blind-retried.
    {:ok, cg2} = start_member(uris, topic, group)
    on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg2) end)

    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg1, timeout: 60_000)
    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg2, timeout: 60_000)
    assert {:ok, a1} = ConsumerGroupHelpers.wait_for_assignments(cg1, timeout: 30_000)
    assert {:ok, a2} = ConsumerGroupHelpers.wait_for_assignments(cg2, timeout: 30_000)

    # Both survived the rebalance, and the 2 partitions are split disjointly.
    assert Process.alive?(cg1) and Process.alive?(cg2)

    p1 = MapSet.new(for {^topic, p} <- a1, do: p)
    p2 = MapSet.new(for {^topic, p} <- a2, do: p)

    assert MapSet.size(p1) > 0 and MapSet.size(p2) > 0,
           "both members must hold partitions after the rebalance (got #{inspect(p1)} / #{inspect(p2)})"

    assert MapSet.disjoint?(p1, p2), "assignments must not overlap"
    assert MapSet.union(p1, p2) == MapSet.new([0, 1]), "both partitions must be covered"
  end

  defp start_member(uris, topic, group) do
    opts = [
      uris: uris,
      heartbeat_interval: 1_000,
      session_timeout: 30_000,
      commit_interval: 1_000,
      auto_offset_reset: :earliest,
      extra_consumer_args: %{test_pid: self()}
    ]

    ConsumerGroup.start_link(TestGenConsumer, group, [topic], opts)
  end
end
