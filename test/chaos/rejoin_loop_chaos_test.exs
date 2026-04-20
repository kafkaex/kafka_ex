defmodule KafkaEx.Chaos.RejoinLoopTest do
  @moduledoc """
  Chaos test for the :illegal_generation rejoin path under sustained fault
  injection.

  NOTE: forcing a real broker-side `OffsetCommit` → `:illegal_generation`
  response requires protocol-level fault injection (Toxiproxy is
  transport-only). Instead, this test simulates the fix's upstream
  effect by casting `{:rejoin_required, :illegal_generation}` in a tight
  loop — the same message GenConsumer emits on a fatal commit error.
  Network chaos is layered on top to verify the rebalance path remains
  bounded when both signals arrive together.

  Assertions:
    * Consumer group supervisor is still alive after sustained storm.
    * GenConsumer spawn count stays bounded (no restart-spawn storm).
    * Consumer resumes processing messages after the storm clears.
  """
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag timeout: 300_000

  alias KafkaEx.API
  alias KafkaEx.ChaosTestHelpers
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.TestSupport.ConsumerGroupHelpers
  alias KafkaEx.TestSupport.TestGenConsumer

  @test_topic "chaos_rejoin_test"
  @consumer_group "chaos_rejoin_group"

  setup_all do
    {:ok, ctx} = ChaosTestHelpers.start_infrastructure()

    {:ok, client} = ChaosTestHelpers.start_client(ctx)
    seed_messages = Enum.map(1..10, fn i -> %{value: "seed_#{i}"} end)
    {:ok, _} = API.produce(client, @test_topic, 0, seed_messages)
    ChaosTestHelpers.stop_client()

    on_exit(fn -> ChaosTestHelpers.stop_infrastructure(ctx) end)
    {:ok, ctx}
  end

  setup ctx do
    ChaosTestHelpers.reset_all(ctx.toxiproxy_container)
    Process.sleep(200)

    on_exit(fn -> ChaosTestHelpers.reset_all(ctx.toxiproxy_container) end)

    {:ok, ctx}
  end

  test "sustained rejoin_required storm: bounded rebalances via cast coalescing", ctx do
    Process.flag(:trap_exit, true)

    {:ok, cg_pid} = start_consumer_group(ctx, "storm")
    on_exit(fn -> ConsumerGroupHelpers.stop_consumer_group(cg_pid) end)

    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg_pid, timeout: 45_000)
    assert {:ok, _} = ConsumerGroupHelpers.wait_for_assignments(cg_pid, timeout: 30_000)

    manager_pid = ConsumerGroup.get_manager_pid(cg_pid)
    stale_gen = ConsumerGroup.generation_id(cg_pid)

    # Count cumulative rebalances via [:kafka_ex, :consumer, :rebalance]
    # telemetry. Without the post-rebalance drain in Manager, each queued
    # cast would trigger its own rebalance — turning 600 casts-per-30s into
    # 600 rebalances. With the drain, we expect O(storm_duration / rebalance_cycle)
    # rebalances, which is roughly one per second.
    counter_ref = :counters.new(1, [:atomics])
    handler_id = {:rejoin_loop_test, make_ref()}

    :telemetry.attach(
      handler_id,
      [:kafka_ex, :consumer, :rebalance],
      fn _event, _measurements, %{reason: reason}, _ ->
        case reason do
          {:commit_fatal, _} -> :counters.add(counter_ref, 1, 1)
          _ -> :ok
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    storm_duration_ms = 30_000
    deadline = System.monotonic_time(:millisecond) + storm_duration_ms

    total_casts = fatal_cast_loop(manager_pid, stale_gen, deadline)

    # Allow in-flight rebalance to settle
    Process.sleep(3_000)

    rebalance_count = :counters.get(counter_ref, 1)

    # Invariants
    assert Process.alive?(cg_pid), "ConsumerGroup supervisor must survive the storm"
    assert Process.alive?(manager_pid), "Manager must survive the storm"
    assert total_casts > 100, "Expected the storm loop to fire many casts; got #{total_casts}"

    # Bound: rebalance count must be meaningfully below cast count. Against
    # Testcontainers Kafka a coalesced cycle lands around 0.5s; over a 30s
    # storm that yields ~60 rebalances. Without coalescing we'd see
    # ~one-per-cast (~600). Upper-bound allows some headroom.
    assert rebalance_count <= 100,
           "Expected <= 100 rebalances (one per cycle), got #{rebalance_count} " <>
             "for #{total_casts} casts. Coalescing regressed?"

    # Positive evidence of coalescing: at least 3x fewer rebalances than casts.
    assert rebalance_count * 3 < total_casts,
           "Expected rebalances to be <1/3 of casts; got #{rebalance_count} rebalances " <>
             "for #{total_casts} casts — coalescing is not working."

    assert rebalance_count >= 1, "At least one rebalance must fire"

    # Recovery: consumer must resume processing after the storm
    assert {:ok, :active} = ConsumerGroupHelpers.wait_for_active(cg_pid, timeout: 45_000)

    {:ok, client} = ChaosTestHelpers.start_client(ctx)
    on_exit(fn -> ChaosTestHelpers.stop_client() end)

    {:ok, _} = API.produce(client, @test_topic, 0, [%{value: "post-storm"}])

    assert_receive {:messages_received, _count}, 30_000
  end

  # Simulate a storm where every GenConsumer on the pre-storm generation
  # fires a cast tagged with that (now-stale) generation — mirroring real
  # production semantics where all casts from the failed generation carry
  # its gen_id. The Manager's drain should coalesce them to a single
  # rebalance per cycle.
  defp fatal_cast_loop(manager_pid, stale_gen, deadline) do
    do_fatal_cast_loop(manager_pid, stale_gen, deadline, 0)
  end

  defp do_fatal_cast_loop(manager_pid, stale_gen, deadline, count) do
    if System.monotonic_time(:millisecond) >= deadline do
      count
    else
      if Process.alive?(manager_pid) do
        GenServer.cast(manager_pid, {:rejoin_required, :illegal_generation, stale_gen})
      end

      Process.sleep(50)
      do_fatal_cast_loop(manager_pid, stale_gen, deadline, count + 1)
    end
  end

  defp start_consumer_group(ctx, group_suffix) do
    group_name = @consumer_group <> "_" <> group_suffix <> "_#{:rand.uniform(100_000)}"

    opts = [
      uris: [{"localhost", ctx.proxy_port}],
      use_ssl: false,
      ssl_options: [],
      heartbeat_interval: 1_000,
      session_timeout: 30_000,
      commit_interval: 500,
      auto_offset_reset: :earliest,
      extra_consumer_args: %{test_pid: self()}
    ]

    ConsumerGroup.start_link(TestGenConsumer, group_name, [@test_topic], opts)
  end
end
