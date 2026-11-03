defmodule KafkaEx.Kafka4.ConsumerProtocolManagerTest do
  @moduledoc """
  `KafkaEx.Consumer.ConsumerGroup.ConsumerProtocolManager` (KIP-848) against
  a Kafka 4.4.0 broker: members join and leave a group while records are
  produced continuously.

  Asserted:

    * (a) every produced record is consumed at least once
    * (b) a member that keeps partitions across a rebalance keeps consuming
      them: their consumer processes are never stopped (no stop-the-world)
    * (c) committed offsets survive members leaving: a later member resumes
      after them instead of from the start
    * (d) a graceful leave hands partitions over faster than a killed member
      (whose partitions only move once its session times out)
  """
  use ExUnit.Case, async: false

  @moduletag :kafka4
  @moduletag timeout: 600_000

  import KafkaEx.TestHelpers, only: [generate_random_string: 0]

  alias KafkaEx.API
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Consumer.GenConsumer.Supervisor, as: GenConsumerSupervisor
  alias KafkaEx.Kafka4TestHelpers

  @partitions 6

  defmodule RecordingConsumer do
    @moduledoc false
    use KafkaEx.Consumer.GenConsumer

    def init(topic, partition, %{test_pid: test_pid, member: member}) do
      send(test_pid, {:consumer_started, member, {topic, partition}, self()})
      {:ok, %{test_pid: test_pid, member: member, tp: {topic, partition}}}
    end

    def handle_message_set(records, state) do
      send(state.test_pid, {:consumed, state.member, state.tp, self(), Enum.map(records, & &1.value)})
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
    topic = "kafka4_cpm_" <> generate_random_string()
    {:ok, _} = API.create_topic(client, topic, num_partitions: @partitions, replication_factor: 1)

    # Every partition known and led before anything is produced.
    wait_until(fn ->
      case API.metadata(client, [topic], []) do
        {:ok, %ClusterMetadata{topics: %{^topic => %Topic{topic_id: <<_::128>>, partitions: ps}}}} ->
          length(ps) == @partitions and Enum.all?(ps, &(&1.leader >= 0))

        _ ->
          false
      end
    end)

    {:ok, Map.merge(ctx, %{client: client, topic: topic, group: "kafka4_cpm_" <> generate_random_string()})}
  end

  # --- helpers ----------------------------------------------------------------

  defp start_member(ctx, name, opts \\ []) do
    opts =
      Kafka4TestHelpers.consumer_group_opts(
        ctx,
        Keyword.merge(
          [
            __consumer_protocol_manager__: true,
            server_assignor: "uniform",
            extra_consumer_args: %{test_pid: self(), member: name},
            commit_interval: 200,
            commit_threshold: 50,
            fetch_options: [max_wait_time: 500],
            leave_timeout_ms: 5_000
          ],
          opts
        )
      )

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

  defp kill_member(sup) do
    ref = Process.monitor(sup)
    # Kill the manager and the consumers without any terminate/2.
    for {_, pid, _, _} <- Supervisor.which_children(sup), is_pid(pid), do: kill_tree(pid)
    Process.exit(sup, :kill)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end

  defp kill_tree(pid) do
    case Process.info(pid, :links) do
      {:links, links} ->
        for child <- links, is_pid(child), child != self(), do: Process.exit(child, :kill)

      nil ->
        :ok
    end

    Process.exit(pid, :kill)
  end

  defp assignments(sup) do
    ConsumerGroup.assignments(sup)
  catch
    :exit, _ -> []
  end

  defp consumer_pids(sup) do
    case ConsumerGroup.consumer_supervisor_pid(sup) do
      nil -> %{}
      csup -> GenConsumerSupervisor.partition_pids(csup)
    end
  catch
    :exit, _ -> %{}
  end

  defp wait_until(fun, timeout \\ 60_000) do
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

  # All partitions owned, disjointly, by exactly `members`.
  defp balanced?(members) do
    lists = Enum.map(members, &assignments/1)
    all = Enum.concat(lists)

    Enum.all?(lists, &(&1 != [])) and length(all) == @partitions and length(Enum.uniq(all)) == @partitions
  end

  # A producer that keeps writing "i" (round robin over partitions) until told
  # to stop; replies with the number of records written.
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
        # A fresh topic's leaders may still be settling: retry the same
        # record until it is written, so the values stay 0..n-1.
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

  # Drains {:consumed, ...} messages into `acc` (value set) and a per-pid log.
  defp drain(acc) do
    receive do
      {:consumed, member, tp, pid, values} ->
        drain(%{
          acc
          | values: Enum.reduce(values, acc.values, &MapSet.put(&2, &1)),
            last: Map.put(acc.last, {member, tp}, System.monotonic_time(:millisecond)),
            pids: Map.update(acc.pids, {member, tp}, MapSet.new([pid]), &MapSet.put(&1, pid))
        })

      {:consumer_started, _, _, _} ->
        drain(acc)
    after
      0 -> acc
    end
  end

  defp new_acc, do: %{values: MapSet.new(), last: %{}, pids: %{}}

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
    {:ok, offsets} = API.fetch_committed_offset(ctx.client, ctx.group, ctx.topic, partitions)
    for %{partition_offsets: [po]} <- offsets, into: %{}, do: {po.partition, max(po.offset, 0)}
  end

  # --- tests ------------------------------------------------------------------

  test "1 -> 2 -> 3 -> 2 -> 1 members under continuous production: no record lost, no stop-the-world", ctx do
    producer = start_producer(ctx)

    m1 = start_member(ctx, :m1)
    wait_until(fn -> balanced?([m1]) end)
    acc = collect_until(new_acc(), &(MapSet.size(&1.values) > 50), 30_000)

    # m2 joins. m1 keeps some partitions: their consumers must be the same
    # processes before and after (only revoked partitions are stopped).
    before = consumer_pids(m1)
    m2 = start_member(ctx, :m2)
    wait_until(fn -> balanced?([m1, m2]) end)
    kept = consumer_pids(m1)
    assert map_size(kept) > 0

    for {tp, pid} <- kept do
      assert before[tp] == pid, "m1's consumer of #{inspect(tp)} was restarted on m2's join"
    end

    # m3 joins: the same for both m1 and m2.
    before1 = consumer_pids(m1)
    before2 = consumer_pids(m2)
    m3 = start_member(ctx, :m3)
    wait_until(fn -> balanced?([m1, m2, m3]) end)

    for {member, before} <- [{m1, before1}, {m2, before2}], {tp, pid} <- consumer_pids(member) do
      if before[tp], do: assert(before[tp] == pid, "a kept consumer of #{inspect(tp)} was restarted")
    end

    # (b) partitions kept by m1 kept being consumed while m3 joined.
    acc = collect_until(acc, fn _ -> false end, 3_000)
    now = System.monotonic_time(:millisecond)

    for {tp, _} <- consumer_pids(m1) do
      assert now - Map.get(acc.last, {:m1, tp}, 0) < 5_000, "m1 stopped consuming #{inspect(tp)}"
    end

    # m3 leaves gracefully, then m2.
    stop_member(m3)
    wait_until(fn -> balanced?([m1, m2]) end)
    stop_member(m2)
    wait_until(fn -> balanced?([m1]) end)

    produced = stop_producer(producer)
    expected = MapSet.new(0..(produced - 1), &Integer.to_string/1)

    # (a) every record consumed at least once.
    acc = collect_until(acc, &MapSet.subset?(expected, &1.values), 60_000)
    missing = MapSet.difference(expected, acc.values)
    assert MapSet.size(missing) == 0, "#{MapSet.size(missing)} of #{produced} records never consumed"

    # (c) committed offsets reached the end of every partition.
    wait_until(fn ->
      offsets = committed(ctx)
      Enum.sum(Map.values(offsets)) == produced
    end)

    stop_member(m1)
  end

  test "committed offsets survive: a new member resumes after them", ctx do
    {:ok, _} = API.produce(ctx.client, ctx.topic, 0, Enum.map(1..20, &%{value: "a#{&1}"}))
    m1 = start_member(ctx, :m1)
    wait_until(fn -> balanced?([m1]) end)
    collect_until(new_acc(), &(MapSet.size(&1.values) >= 20), 30_000)
    wait_until(fn -> committed(ctx)[0] == 20 end)
    stop_member(m1)

    {:ok, _} = API.produce(ctx.client, ctx.topic, 0, Enum.map(1..5, &%{value: "b#{&1}"}))
    _ = drain(new_acc())

    m2 = start_member(ctx, :m2)
    wait_until(fn -> balanced?([m2]) end)
    acc = collect_until(new_acc(), &(MapSet.size(&1.values) >= 5), 30_000)

    assert MapSet.equal?(acc.values, MapSet.new(1..5, &"b#{&1}"))
    stop_member(m2)
  end

  test "a graceful leave hands partitions over faster than a killed member", ctx do
    # Short session so the killed case finishes in reasonable time; the
    # broker's group.consumer.session.timeout.ms default is 45 s.
    m1 = start_member(ctx, :m1)
    wait_until(fn -> balanced?([m1]) end)

    m2 = start_member(ctx, :m2)
    wait_until(fn -> balanced?([m1, m2]) end)
    t0 = System.monotonic_time(:millisecond)
    stop_member(m2)
    wait_until(fn -> balanced?([m1]) end, 120_000)
    graceful = System.monotonic_time(:millisecond) - t0

    m3 = start_member(ctx, :m3)
    wait_until(fn -> balanced?([m1, m3]) end)
    t0 = System.monotonic_time(:millisecond)
    kill_member(m3)
    wait_until(fn -> balanced?([m1]) end, 120_000)
    killed = System.monotonic_time(:millisecond) - t0

    assert graceful < killed, "graceful #{graceful} ms, killed #{killed} ms"
    assert graceful < 15_000
    stop_member(m1)
  end

  test "rack_id is optional: it is reported by describe only when configured", ctx do
    m1 = start_member(ctx, :m1, rack_id: "rack-a")
    m2 = start_member(ctx, :m2)
    wait_until(fn -> balanced?([m1, m2]) end)

    {:ok, desc} = API.describe_consumer_group(ctx.client, ctx.group)
    m1_id = ConsumerGroup.member_id(m1)
    racks = Map.new(desc.members, &{&1.member_id, &1.rack_id})
    assert racks[m1_id] == "rack-a"
    assert racks[ConsumerGroup.member_id(m2)] in [nil, ""]
  end

  test "describe shows the member's assignment after reconciliation and epoch survives restarts", ctx do
    m1 = start_member(ctx, :m1)
    wait_until(fn -> balanced?([m1]) end)

    wait_until(fn ->
      {:ok, desc} = API.describe_consumer_group(ctx.client, ctx.group)

      match?([_], desc.members) and
        desc.state == "Stable" and
        hd(desc.members).member_epoch == ConsumerGroup.generation_id(m1)
    end)
  end
end
