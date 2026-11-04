defmodule KafkaEx.Kafka4.GroupProtocolMigrationTest do
  @moduledoc """
  `KafkaEx.Consumer.ConsumerGroup.start_link/4` with `group_protocol:`
  against a Kafka 4.4.0 broker: an in-place rolling migration of a group
  from `:classic` to `:consumer` while records are produced continuously.

  The broker's default `group.consumer.migration.policy` (`bidirectional`)
  converts the classic group into a consumer group when the first
  `:consumer` member joins; the classic member stays in it (as a member of
  type `:classic`) until it leaves. Asserted:

    * a classic member and a `:consumer` member share the group, and the
      partitions, at the same time
    * after the classic member leaves, the `:consumer` member owns every
      partition and the group has no classic member left
    * the committed offsets are handed over: the `:consumer` member starts
      with `auto_offset_reset: :latest` (the GenConsumer default), so it can
      only see the records produced before it joined through the classic
      member's commits. On every partition it resumes at or after the offset
      the classic member had committed before the join (not from 0) and no
      later than just after the last record the classic member consumed
      there (no gap)
    * no record is lost across the whole roll
    * the `:consumer` member commits: at the end the committed offset of
      every partition is its end offset
  """
  use ExUnit.Case, async: false

  @moduletag :kafka4
  @moduletag timeout: 600_000

  import KafkaEx.TestHelpers, only: [generate_random_string: 0]

  alias KafkaEx.API
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Kafka4TestHelpers
  alias KafkaEx.Messages.ConsumerGroupDescription

  @partitions 4

  defmodule RecordingConsumer do
    @moduledoc false
    use KafkaEx.Consumer.GenConsumer

    def init(topic, partition, %{test_pid: test_pid, member: member}) do
      {:ok, %{test_pid: test_pid, member: member, tp: {topic, partition}}}
    end

    def handle_message_set(records, state) do
      send(state.test_pid, {:consumed, state.member, state.tp, Enum.map(records, &{&1.offset, &1.value})})
      {:async_commit, state}
    end
  end

  setup_all do
    {:ok, ctx} = Kafka4TestHelpers.start_kafka()
    on_exit(fn -> Kafka4TestHelpers.stop_kafka(ctx) end)
    {:ok, ctx}
  end

  setup ctx do
    {:ok, client} = Kafka4TestHelpers.start_client(ctx)
    on_exit(fn -> KafkaEx.TestSupport.ProcessHelpers.stop_safely(client) end)
    topic = "kafka4_migrate_" <> generate_random_string()
    {:ok, _} = API.create_topic(client, topic, num_partitions: @partitions, replication_factor: 1)

    wait_until(fn ->
      case API.metadata(client, [topic], []) do
        {:ok, %ClusterMetadata{topics: %{^topic => %Topic{topic_id: <<_::128>>, partitions: ps}}}} ->
          length(ps) == @partitions and Enum.all?(ps, &(&1.leader >= 0))

        _ ->
          false
      end
    end)

    {:ok, Map.merge(ctx, %{client: client, topic: topic, group: "kafka4_migrate_" <> generate_random_string()})}
  end

  # --- helpers ----------------------------------------------------------------

  defp start_member(ctx, name, protocol_opts) do
    opts =
      Kafka4TestHelpers.consumer_group_opts(
        ctx,
        [
          extra_consumer_args: %{test_pid: self(), member: name},
          commit_interval: 200,
          commit_threshold: 20,
          fetch_options: [max_wait_time: 500]
        ] ++ protocol_opts
      )

    {:ok, sup} = ConsumerGroup.start_link(RecordingConsumer, ctx.group, [ctx.topic], opts)
    Process.unlink(sup)
    on_exit(fn -> stop_member(sup) end)
    sup
  end

  # Kafka4TestHelpers.consumer_group_opts/2 sets the classic timing options;
  # a :consumer member is started without them (they would only be ignored).
  # auto_offset_reset is :latest (the GenConsumer default), not the helpers'
  # :earliest: a member that misses the classic member's committed offsets
  # then skips records instead of hiding the defect by re-reading from 0.
  defp start_consumer_member(ctx, name) do
    opts =
      [
        uris: ctx.uris,
        use_ssl: false,
        ssl_options: [],
        auto_offset_reset: :latest,
        group_protocol: :consumer,
        server_assignor: "uniform",
        extra_consumer_args: %{test_pid: self(), member: name},
        commit_interval: 200,
        commit_threshold: 20,
        fetch_options: [max_wait_time: 500],
        leave_timeout_ms: 5_000
      ]

    {:ok, sup} = ConsumerGroup.start_link(RecordingConsumer, ctx.group, [ctx.topic], opts)
    Process.unlink(sup)
    on_exit(fn -> stop_member(sup) end)
    sup
  end

  defp stop_member(sup) do
    if Process.alive?(sup) do
      try do
        Supervisor.stop(sup, :shutdown, 60_000)
      catch
        :exit, _ -> :ok
      end
    end
  end

  defp assignments(sup) do
    ConsumerGroup.assignments(sup)
  catch
    :exit, _ -> []
  end

  defp wait_until(fun, timeout \\ 90_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("condition not met in time")

      true ->
        Process.sleep(200)
        do_wait_until(fun, deadline)
    end
  end

  defp start_producer(ctx) do
    test_pid = self()

    spawn_link(fn ->
      {:ok, client} = Kafka4TestHelpers.start_client(ctx)
      produce_loop(client, ctx.topic, 0, test_pid)
    end)
  end

  defp produce_loop(client, topic, i, test_pid) do
    receive do
      :stop ->
        send(test_pid, {:produced, i})
        GenServer.stop(client)
    after
      0 ->
        case API.produce(client, topic, rem(i, @partitions), [%{value: Integer.to_string(i)}]) do
          {:ok, _} ->
            Process.sleep(5)
            produce_loop(client, topic, i + 1, test_pid)

          {:error, _} ->
            Process.sleep(100)
            produce_loop(client, topic, i, test_pid)
        end
    end
  end

  defp stop_producer(pid) do
    send(pid, :stop)

    receive do
      {:produced, n} -> n
    after
      30_000 -> flunk("producer did not stop")
    end
  end

  defp drain(acc) do
    receive do
      {:consumed, member, {_topic, partition}, records} ->
        offsets = Enum.map(records, &elem(&1, 0))
        key = {member, partition}

        drain(%{
          acc
          | values: Enum.reduce(records, acc.values, fn {_, v}, set -> MapSet.put(set, v) end),
            by: Map.update(acc.by, member, length(records), &(&1 + length(records))),
            first: if(offsets == [], do: acc.first, else: Map.put_new(acc.first, key, Enum.min(offsets))),
            last:
              if(offsets == [],
                do: acc.last,
                else: Map.update(acc.last, key, Enum.max(offsets), &max(&1, Enum.max(offsets)))
              )
        })
    after
      0 -> acc
    end
  end

  defp collect_until(acc, fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn -> :ok end)
    |> Enum.reduce_while(acc, fn _, acc ->
      acc = drain(acc)

      cond do
        fun.(acc) -> {:halt, acc}
        System.monotonic_time(:millisecond) > deadline -> {:halt, acc}
        true -> Process.sleep(100) && {:cont, acc}
      end
    end)
  end

  defp committed(ctx) do
    partitions = Enum.map(0..(@partitions - 1), &%{partition_num: &1})

    case API.fetch_committed_offset(ctx.client, ctx.group, ctx.topic, partitions) do
      {:ok, offsets} -> for %{partition_offsets: [po]} <- offsets, into: %{}, do: {po.partition, po.offset}
      {:error, _} -> %{}
    end
  end

  defp end_offsets(ctx) do
    for p <- 0..(@partitions - 1), into: %{} do
      {:ok, offset} = API.latest_offset(ctx.client, ctx.topic, p)
      {p, offset}
    end
  end

  defp member_types(ctx) do
    case API.describe_consumer_group(ctx.client, ctx.group) do
      {:ok, %ConsumerGroupDescription{members: members}} -> members |> Enum.map(& &1.member_type) |> Enum.sort()
      {:error, _} -> nil
    end
  end

  # --- tests ------------------------------------------------------------------

  test "rolling migration classic -> consumer: both coexist in one group, then classic leaves, no record lost",
       ctx do
    producer = start_producer(ctx)

    # 1. The group starts classic.
    classic = start_member(ctx, :classic, group_protocol: :classic)
    assert ConsumerGroup.group_protocol(classic) == :classic
    wait_until(fn -> length(assignments(classic)) == @partitions end)

    acc =
      collect_until(
        %{values: MapSet.new(), by: %{}, first: %{}, last: %{}},
        &(Map.get(&1.by, :classic, 0) > 50),
        60_000
      )

    assert Map.get(acc.by, :classic, 0) > 0, "the classic member consumed nothing"

    # The classic member has committed on every partition; this is the floor
    # the :consumer member must resume from.
    wait_until(fn ->
      c = committed(ctx)
      map_size(c) == @partitions and Enum.all?(c, fn {_, o} -> o > 0 end)
    end)

    classic_committed = committed(ctx)

    # A classic group is not described by ConsumerGroupDescribe.
    assert {:error, :group_id_not_found} = API.describe_consumer_group(ctx.client, ctx.group)

    # 2. A :consumer member joins the same group: the broker converts the
    # group, and both members hold partitions at the same time.
    consumer = start_consumer_member(ctx, :consumer)
    assert ConsumerGroup.group_protocol(consumer) == :consumer

    wait_until(fn ->
      a = assignments(classic)
      b = assignments(consumer)
      a != [] and b != [] and length(a ++ b) == @partitions and length(Enum.uniq(a ++ b)) == @partitions
    end)

    assert member_types(ctx) == [:classic, :consumer]

    acc = collect_until(acc, &(Map.get(&1.by, :consumer, 0) > 20), 60_000)
    assert Map.get(acc.by, :consumer, 0) > 0, "the :consumer member consumed nothing while sharing the group"

    # 3. The classic member leaves (graceful stop: LeaveGroup), the
    # :consumer member takes over every partition.
    :ok = Supervisor.stop(classic, :shutdown, 60_000)
    wait_until(fn -> length(assignments(consumer)) == @partitions end)
    wait_until(fn -> member_types(ctx) == [:consumer] end)

    # 4. Every record produced during the roll is consumed.
    produced = stop_producer(producer)
    expected = MapSet.new(0..(produced - 1), &Integer.to_string/1)
    acc = collect_until(acc, &MapSet.subset?(expected, &1.values), 90_000)
    missing = MapSet.difference(expected, acc.values)

    assert MapSet.size(missing) == 0,
           "#{MapSet.size(missing)} of #{produced} records were never consumed, e.g. " <>
             inspect(missing |> Enum.take(10))

    # 5. The committed offsets were handed over on every partition: the
    # :consumer member resumed from the classic member's commits, not from 0
    # and not past records the classic member never consumed.
    for p <- 0..(@partitions - 1) do
      first = Map.fetch!(acc.first, {:consumer, p})
      classic_last = Map.fetch!(acc.last, {:classic, p})

      assert first >= classic_committed[p],
             "partition #{p}: the :consumer member started at offset #{first}, below the classic member's " <>
               "committed offset #{classic_committed[p]} (it did not use the committed offset)"

      assert first <= classic_last + 1,
             "partition #{p}: the :consumer member started at offset #{first}, past the classic member's " <>
               "last consumed offset #{classic_last} (records in between were skipped)"
    end

    # 6. The :consumer member commits what it consumed.
    ends = end_offsets(ctx)
    wait_until(fn -> committed(ctx) == ends end, 30_000)

    {:ok, %ConsumerGroupDescription{type: :consumer}} = API.describe_consumer_group(ctx.client, ctx.group)
  end

  test "an invalid group_protocol is rejected before connecting", ctx do
    assert {:error, {:invalid_option, :invalid_group_protocol}} =
             ConsumerGroup.start_link(RecordingConsumer, ctx.group, [ctx.topic], group_protocol: :next)
  end
end

defmodule KafkaEx.Kafka4.GroupProtocolUnsupportedTest do
  @moduledoc """
  `group_protocol: :consumer` against a Kafka 4.4.0 broker with the consumer
  group protocol disabled (`group.coordinator.rebalance.protocols=classic`).

  Such a broker still advertises ConsumerGroupHeartbeat in ApiVersions (so
  the start-up check passes) and answers the join with UNSUPPORTED_VERSION.
  As in Java (`AbstractHeartbeatRequestManager`, fatal with
  `CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG`) that is terminal: the group stops
  at its first heartbeat with a clear error and does not fall back to the
  classic protocol.
  """
  use ExUnit.Case, async: false

  @moduletag :kafka4
  @moduletag timeout: 600_000

  import ExUnit.CaptureLog
  import KafkaEx.TestHelpers, only: [generate_random_string: 0]

  alias KafkaEx.API
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Kafka4TestHelpers
  alias KafkaEx.Messages.ApiVersions

  defmodule NoopConsumer do
    @moduledoc false
    use KafkaEx.Consumer.GenConsumer
    def handle_message_set(_records, state), do: {:async_commit, state}
  end

  setup_all do
    {:ok, ctx} =
      Kafka4TestHelpers.start_kafka(env: [{"KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic"}])

    on_exit(fn -> Kafka4TestHelpers.stop_kafka(ctx) end)
    {:ok, ctx}
  end

  test "the group stops at its join with a terminal error, without a classic fallback", ctx do
    {:ok, client} = Kafka4TestHelpers.start_client(ctx)
    topic = "kafka4_noproto_" <> generate_random_string()
    {:ok, _} = API.create_topic(client, topic, num_partitions: 1, replication_factor: 1)
    group = "kafka4_noproto_" <> generate_random_string()

    # What makes the start-up check pass: the API is still advertised.
    {:ok, versions} = API.api_versions(client)
    assert {:ok, _} = ApiVersions.max_version_for_api(versions, 68)

    test_pid = self()
    handler = "noproto-#{System.unique_integer()}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :consumer, :member_terminated],
      fn _, _, metadata, _ -> send(test_pid, {:terminated, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    log =
      capture_log(fn ->
        {:ok, sup} =
          ConsumerGroup.start_link(NoopConsumer, group, [topic],
            uris: ctx.uris,
            use_ssl: false,
            ssl_options: [],
            group_protocol: :consumer
          )

        Process.unlink(sup)
        ref = Process.monitor(sup)
        assert_receive {:DOWN, ^ref, :process, ^sup, _reason}, 30_000
        assert_receive {:terminated, %{group_id: ^group, reason: :unsupported_version}}
      end)

    assert log =~ "failed with terminal :unsupported_version"
    assert log =~ "may not support the KIP-848 consumer group protocol"
    assert log =~ "group_protocol: :classic"

    # No classic fallback: the group was never joined.
    assert {:ok, %{members: []}} = API.describe_group(client, group)
    GenServer.stop(client)
  end
end
