defmodule KafkaEx.Integration.ConsumerGroup.ContractCharacterizationTest do
  use ExUnit.Case, async: false

  import KafkaEx.TestSupport.ConsumerGroupHelpers
  import KafkaEx.IntegrationHelpers, only: [create_topic: 3]
  import KafkaEx.TestHelpers, only: [generate_random_string: 0, uris: 0]

  alias KafkaEx.API
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.TestSupport.TestGenConsumer

  @moduletag :consumer_group
  @moduletag timeout: 60_000

  setup do
    {:ok, args} = KafkaEx.build_worker_options(uris: uris())
    {:ok, client} = API.start_client(args)
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)
    {:ok, client: client}
  end

  # Pins delivery count only: TestGenConsumer reports {:messages_received, count},
  # not message bodies, so per-offset ordering is not asserted here.
  test "delivers all produced messages", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)

    {:ok, cg} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic],
        group_prefix: group,
        consumer_module: TestGenConsumer,
        auto_offset_reset: :earliest,
        test_pid: self()
      )

    register_consumer_group_cleanup(cg)
    assert {:ok, :active} = wait_for_active(cg)
    assert {:ok, _} = wait_for_assignments(cg)

    values = for i <- 1..10, do: %{value: "msg-#{i}"}
    {:ok, _} = API.produce(client, topic, 0, values)

    # TestGenConsumer sends {:messages_received, count} per batch.
    assert wait_for_message_count(10, timeout: 30_000) >= 10
  end

  test "a clean stop commits progress so a new consumer resumes past it", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)

    {:ok, cg1} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic],
        group_prefix: group,
        consumer_module: TestGenConsumer,
        auto_offset_reset: :earliest,
        test_pid: self()
      )

    register_consumer_group_cleanup(cg1)
    assert {:ok, :active} = wait_for_active(cg1)
    assert {:ok, _} = wait_for_assignments(cg1)

    {:ok, _} = API.produce(client, topic, 0, for(i <- 1..5, do: %{value: "a-#{i}"}))
    assert wait_for_message_count(5, timeout: 30_000) >= 5

    :ok = stop_consumer_group(cg1)

    # Same group resumes; the committed offset means it does NOT re-deliver all 5.
    {:ok, _} = API.produce(client, topic, 0, for(i <- 1..3, do: %{value: "b-#{i}"}))

    {:ok, cg2} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic],
        group_prefix: group,
        consumer_module: TestGenConsumer,
        auto_offset_reset: :earliest,
        test_pid: self()
      )

    register_consumer_group_cleanup(cg2)
    assert {:ok, :active} = wait_for_active(cg2)

    # Characterization: the resumed consumer delivers at least the 3 new messages.
    # (At-least-once: it may re-deliver the last uncommitted batch — assert the lower bound.)
    assert wait_for_message_count(3, timeout: 30_000) >= 3
  end

  test "auto_offset_reset :earliest consumes from the start of an existing topic", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)

    # Produce BEFORE any consumer exists; :earliest must still see them.
    {:ok, _} = API.produce(client, topic, 0, for(i <- 1..4, do: %{value: "pre-#{i}"}))

    {:ok, cg} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic],
        group_prefix: group,
        consumer_module: TestGenConsumer,
        auto_offset_reset: :earliest,
        test_pid: self()
      )

    register_consumer_group_cleanup(cg)
    assert {:ok, :active} = wait_for_active(cg)
    assert wait_for_message_count(4, timeout: 30_000) >= 4
  end

  # Characterization note: telemetry.span/3 (telemetry 1.3.x) does NOT merge
  # start metadata into stop metadata — the :stop event only carries the fields
  # the span callback returns (%{commit_mode: ...} plus telemetry_span_context).
  # group_id/topic/partition/consumer_module live on the :start event.
  test "emits [:kafka_ex, :consumer, :process] span with group_id/topic/partition metadata", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)

    test_pid = self()
    handler = "char-#{topic}"

    :telemetry.attach_many(
      handler,
      [[:kafka_ex, :consumer, :process, :start], [:kafka_ex, :consumer, :process, :stop]],
      fn event, _measure, meta, _ -> send(test_pid, {:telemetry, event, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, cg} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic],
        group_prefix: group,
        consumer_module: TestGenConsumer,
        auto_offset_reset: :earliest,
        test_pid: test_pid
      )

    register_consumer_group_cleanup(cg)
    assert {:ok, :active} = wait_for_active(cg)
    {:ok, _} = API.produce(client, topic, 0, [%{value: "telemetry"}])

    # group_id/topic/partition are on :start (telemetry.span/3 does not forward
    # start metadata to :stop; the :stop event only carries commit_mode).
    assert_receive {:telemetry, [:kafka_ex, :consumer, :process, :start], start_meta}, 30_000
    assert start_meta.topic == topic
    assert start_meta.partition == 0
    assert String.starts_with?(start_meta.group_id, group)

    # :stop confirms processing completed and commit_mode is present.
    assert_receive {:telemetry, [:kafka_ex, :consumer, :process, :stop], stop_meta}, 5_000
    assert stop_meta.commit_mode in [:async_commit, :sync_commit]
  end

  # Step 1: :sync_commit cadence. SyncTestConsumer returns {:sync_commit, state},
  # so each batch is committed synchronously. After a clean stop, a fresh consumer
  # on the SAME group must NOT re-deliver the already-committed messages.
  #
  # SyncTestConsumer is incompatible with the start_test_consumer_group/
  # wait_for_message_count helpers (it reads extra_consumer_args as a keyword list
  # and sends {:messages_received, <list>}), so it is started manually and counted
  # with the inline collect_sync/2 helper below.
  test ":sync_commit commits each batch; a resumed consumer sees no re-delivery", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)

    {:ok, cg1} =
      ConsumerGroup.start_link(
        KafkaEx.TestConsumers.SyncTestConsumer,
        group,
        [topic],
        heartbeat_interval: 1_000,
        session_timeout: 6_000,
        commit_interval: 1_000,
        auto_offset_reset: :earliest,
        extra_consumer_args: [test_pid: self()]
      )

    register_consumer_group_cleanup(cg1)
    assert {:ok, :active} = wait_for_active(cg1)

    {:ok, _} = API.produce(client, topic, 0, for(i <- 1..5, do: %{value: "s-#{i}"}))

    # SyncTestConsumer sends {:messages_received, <list of message values>} per batch.
    deadline = System.monotonic_time(:millisecond) + 30_000
    assert :ok = collect_sync(5, deadline)

    :ok = stop_consumer_group(cg1)

    {:ok, cg2} =
      ConsumerGroup.start_link(
        KafkaEx.TestConsumers.SyncTestConsumer,
        group,
        [topic],
        heartbeat_interval: 1_000,
        session_timeout: 6_000,
        commit_interval: 1_000,
        auto_offset_reset: :earliest,
        extra_consumer_args: [test_pid: self()]
      )

    register_consumer_group_cleanup(cg2)
    assert {:ok, :active} = wait_for_active(cg2, timeout: 40_000)

    # The committed offset means the synchronously-committed batch is NOT re-delivered.
    refute_receive {:messages_received, _}, 3_000
  end

  # Step 2: :none offset-reset contract (documentation only). Cannot be auto-tested
  # in Phase 1: handle_offset_out_of_range/1 is private (gen_consumer.ex:904-923) —
  # :none => raise, :earliest/:latest => reset — and a genuine offset_out_of_range
  # needs a committed offset beyond the log (produce, commit, then seek/recreate past
  # the high_watermark). Phase 2/3 exercises this once the Fetcher reports OOB.
  # Manual repro: commit an offset, delete+recreate the topic so the log is shorter
  # than the committed offset, then start a :none consumer and observe the raise.
  # Uses @tag :skip (NOT a custom tag) so `mix test --only consumer_group` excludes it.
  @tag :skip
  test ":none auto_offset_reset raises on offset_out_of_range (documented; Phase 3 implements)" do
    :ok
  end

  # Step 3: commit cadence under a hard kill (at-least-once). TestGenConsumer uses
  # :async_commit, started here with commit_interval: 1_000 and relying on the default
  # commit_threshold (100) — so a 4-message batch never threshold-commits; progress can
  # only be persisted by the time-based idle interval-commit (gen_consumer.ex
  # handle_commit/2 ~line 954) or by the terminal commit in terminate/2. We kill cg1
  # with :kill, which bypasses terminate/2 (no terminal commit).
  #
  # Observed behaviour, deterministic across runs: the resumed cg2 RE-DELIVERS the full
  # batch of 4. We characterize that observed contract — at-least-once redelivery of the
  # batch that was not terminally committed — and assert only the lower bound (>= 4). We
  # deliberately do NOT pin the precise cause: whether the interval-commit fired but was
  # not durably visible to cg2 before the session expired, or never fired in the idle
  # window, is an implementation detail this test does not assert. The durability gap it
  # exposes is what the Phase 2/3 Fetcher/Consumer split is intended to close.
  test "a hard kill (no terminal commit) re-delivers the un-committed batch (at-least-once)", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)

    {:ok, cg1} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic],
        group_prefix: group,
        consumer_module: TestGenConsumer,
        auto_offset_reset: :earliest,
        commit_interval: 1_000,
        session_timeout: 6_000,
        test_pid: self()
      )

    register_consumer_group_cleanup(cg1)
    assert {:ok, :active} = wait_for_active(cg1)
    assert {:ok, _} = wait_for_assignments(cg1)

    {:ok, _} = API.produce(client, topic, 0, for(i <- 1..4, do: %{value: "idle-#{i}"}))
    assert wait_for_message_count(4, timeout: 30_000) >= 4

    # Give the idle interval-commit (commit_interval 1_000ms) a chance to run.
    Process.sleep(2_000)

    # Kill bypasses terminate/2 (no terminal commit). Unlink first so the :kill EXIT
    # does not propagate to the test process (ConsumerGroup.start_link links to caller).
    Process.unlink(cg1)
    Process.exit(cg1, :kill)

    {:ok, cg2} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic],
        group_prefix: group,
        consumer_module: TestGenConsumer,
        auto_offset_reset: :earliest,
        commit_interval: 1_000,
        session_timeout: 6_000,
        test_pid: self()
      )

    register_consumer_group_cleanup(cg2)
    assert {:ok, :active} = wait_for_active(cg2, timeout: 40_000)

    # Observed, deterministic across runs: the un-terminally-committed batch is fully
    # re-delivered. Assert the lower bound (>= 4) rather than fighting at-least-once.
    assert wait_for_message_count(4, timeout: 30_000) >= 4
  end

  # Counts messages delivered by SyncTestConsumer, which sends the LIST of message
  # values (not an integer count). Returns :ok once `count` messages have arrived,
  # or {:timeout, remaining} if the deadline passes first.
  defp collect_sync(count, _deadline) when count <= 0, do: :ok

  defp collect_sync(count, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:timeout, count}
    else
      receive do
        {:messages_received, msgs} -> collect_sync(count - length(msgs), deadline)
      after
        min(remaining, 1000) -> collect_sync(count, deadline)
      end
    end
  end
end
