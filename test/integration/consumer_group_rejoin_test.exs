defmodule KafkaEx.Integration.ConsumerGroupRejoinTest do
  @moduledoc """
  End-to-end integration tests for the :illegal_generation rejoin path.

  Triggering a real :illegal_generation commit response requires broker-
  protocol-level manipulation (external LeaveGroup with an active
  member_id), which is not trivial to express through the public API.

  Instead, these tests directly cast `{:rejoin_required, reason}` to the
  Manager (the same message GenConsumer now emits on fatal commit
  errors). That exercises the full Manager → rebalance → start_consumer
  loop against a live broker and verifies consumers resume correctly.

  The GenConsumer self-stop + :transient restart path (B5) is covered by
  unit tests in gen_consumer_commit_test.exs.
  """
  use ExUnit.Case, async: false
  @moduletag :consumer_group
  @moduletag :integration

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers
  import KafkaEx.TestSupport.ConsumerGroupHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Consumer.ConsumerGroup

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)

    {:ok, %{client: client}}
  end

  @tag timeout: 120_000
  test "rejoin_required cast triggers rebalance and consumer resumes processing", %{client: client} do
    Process.flag(:trap_exit, true)
    topic_name = generate_random_string()
    group_prefix = generate_random_string()
    _ = create_topic(client, topic_name)

    {:ok, cg_pid} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic_name],
        group_prefix: group_prefix,
        heartbeat_interval: 1_000,
        commit_interval: 500,
        auto_offset_reset: :earliest
      )

    register_consumer_group_cleanup(cg_pid)

    assert {:ok, :active} = wait_for_active(cg_pid)
    assert {:ok, _} = wait_for_assignments(cg_pid)

    # Produce the first batch and confirm consumption before disturbance
    {:ok, _} = API.produce(client, topic_name, 0, [%{value: "pre-rejoin"}])
    assert wait_for_message_count(1, timeout: 15_000) >= 1

    gen_before = ConsumerGroup.generation_id(cg_pid)
    member_before = ConsumerGroup.member_id(cg_pid)
    assert is_integer(gen_before) and gen_before >= 0
    assert is_binary(member_before) and member_before != ""

    # Fire the same cast that GenConsumer now emits on fatal commit errors.
    # Tag with the stale generation so the Manager drains only duplicates
    # from this pre-rebalance generation.
    manager_pid = ConsumerGroup.get_manager_pid(cg_pid)
    GenServer.cast(manager_pid, {:rejoin_required, :illegal_generation, gen_before})

    # Wait for a new generation to settle. Manager is busy during rebalance
    # so we use a short call timeout and catch exits while polling.
    wait_for_new_generation(cg_pid, gen_before, 30_000)

    # Confirm the consumer is active again after the rebalance
    assert {:ok, :active} = wait_for_active(cg_pid)

    # Produce another batch and confirm the resumed consumer handles it
    {:ok, _} = API.produce(client, topic_name, 0, [%{value: "post-rejoin"}])
    assert wait_for_message_count(2, timeout: 15_000) >= 2
  end

  @tag timeout: 180_000
  test "storm of rejoin_required casts: post-rebalance drain coalesces duplicates", %{client: client} do
    Process.flag(:trap_exit, true)
    topic_name = generate_random_string()
    group_prefix = generate_random_string()
    _ = create_topic(client, topic_name)

    {:ok, cg_pid} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic_name],
        group_prefix: group_prefix,
        heartbeat_interval: 1_000,
        commit_interval: 500,
        auto_offset_reset: :earliest
      )

    register_consumer_group_cleanup(cg_pid)

    assert {:ok, :active} = wait_for_active(cg_pid)
    assert {:ok, _} = wait_for_assignments(cg_pid)

    manager_pid = ConsumerGroup.get_manager_pid(cg_pid)
    gen_before = ConsumerGroup.generation_id(cg_pid)

    # Fire a storm of fatal casts all tagged with the SAME stale generation.
    # The first cast triggers rebalance; the remaining 19 queue in the
    # mailbox and are drained together (same stale_gen tag matches).
    Enum.each(1..20, fn _ ->
      GenServer.cast(manager_pid, {:rejoin_required, :illegal_generation, gen_before})
    end)

    # Settle and confirm we still have a live, active consumer
    wait_for_new_generation(cg_pid, gen_before, 30_000)

    assert {:ok, :active} = wait_for_active(cg_pid)
    assert Process.alive?(cg_pid)
    assert Process.alive?(manager_pid)

    # Verify consumer still processes messages end-to-end
    {:ok, _} = API.produce(client, topic_name, 0, [%{value: "after-storm"}])
    assert wait_for_message_count(1, timeout: 15_000) >= 1
  end

  # Poll generation_id with a short call-timeout; the Manager is busy during
  # rebalance and cannot respond to the default 5s timeout. Catches exits so
  # the poll keeps trying until the rebalance completes and a new generation
  # is assigned.
  defp wait_for_new_generation(cg_pid, gen_before, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_new_generation(cg_pid, gen_before, deadline)
  end

  defp do_wait_for_new_generation(cg_pid, gen_before, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      flunk("generation_id did not change from #{inspect(gen_before)} within timeout")
    end

    result =
      try do
        ConsumerGroup.generation_id(cg_pid, 500)
      catch
        :exit, _ -> :call_timeout
      end

    cond do
      is_integer(result) and result != gen_before ->
        :ok

      true ->
        Process.sleep(200)
        do_wait_for_new_generation(cg_pid, gen_before, deadline)
    end
  end
end
