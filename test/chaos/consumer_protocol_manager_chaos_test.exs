defmodule KafkaEx.Chaos.ConsumerProtocolManagerTest do
  @moduledoc """
  Network faults against `KafkaEx.Consumer.ConsumerGroup.ConsumerProtocolManager`
  (KIP-848), on an `apache/kafka:4.4.0` broker behind Toxiproxy
  (`KafkaEx.ChaosTestHelpers.start_kafka4_infrastructure/1`; the default chaos
  broker is Kafka 3.4, which has no consumer group protocol).

  The broker is a single node, so "coordinator loss" and "broker loss" are the
  same fault here: the proxy is taken down and every connection drops.
  """
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag timeout: 600_000

  import KafkaEx.TestHelpers, only: [generate_random_string: 0]

  alias KafkaEx.API
  alias KafkaEx.ChaosTestHelpers
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Consumer.GenConsumer.Supervisor, as: GenConsumerSupervisor
  alias KafkaEx.ToxiproxyHelpers

  @partitions 4

  defmodule RecordingConsumer do
    @moduledoc false
    use KafkaEx.Consumer.GenConsumer

    def init(topic, partition, %{test_pid: test_pid, member: member}) do
      {:ok, %{test_pid: test_pid, member: member, tp: {topic, partition}}}
    end

    def handle_message_set(records, state) do
      send(state.test_pid, {:consumed, state.member, state.tp, Enum.map(records, & &1.value)})
      {:async_commit, state}
    end
  end

  setup_all do
    {:ok, ctx} = ChaosTestHelpers.start_kafka4_infrastructure()
    on_exit(fn -> ChaosTestHelpers.stop_infrastructure(ctx) end)
    {:ok, ctx}
  end

  setup ctx do
    ChaosTestHelpers.reset_all(ctx.toxiproxy_container)
    ToxiproxyHelpers.enable_proxy(ctx.toxiproxy_container, ctx.proxy_name)
    {:ok, client} = KafkaEx.Client.start_link(client_opts(ctx), :no_name)
    Process.unlink(client)

    on_exit(fn ->
      ChaosTestHelpers.reset_all(ctx.toxiproxy_container)
      ToxiproxyHelpers.enable_proxy(ctx.toxiproxy_container, ctx.proxy_name)
      KafkaEx.TestSupport.ProcessHelpers.stop_safely(client)
    end)

    topic = "chaos_cpm_" <> generate_random_string()
    {:ok, _} = API.create_topic(client, topic, num_partitions: @partitions, replication_factor: 1)

    wait_until(fn ->
      match?(
        {:ok, %ClusterMetadata{topics: %{^topic => %Topic{topic_id: <<_::128>>, partitions: ps}}}}
        when length(ps) == @partitions,
        API.metadata(client, [topic], [])
      )
    end)

    {:ok, Map.merge(ctx, %{client: client, topic: topic, group: "chaos_cpm_" <> generate_random_string()})}
  end

  defp client_opts(%{broker_via_proxy: broker}),
    do: [uris: [broker], consumer_group: :no_consumer_group, use_ssl: false, ssl_options: []]

  defp start_member(ctx, name, opts \\ []) do
    {host, port} = ctx.broker_via_proxy

    opts =
      Keyword.merge(
        [
          __consumer_protocol_manager__: true,
          uris: [{host, port}],
          use_ssl: false,
          ssl_options: [],
          server_assignor: "uniform",
          auto_offset_reset: :earliest,
          extra_consumer_args: %{test_pid: self(), member: name},
          commit_interval: 200,
          commit_threshold: 20,
          fetch_options: [max_wait_time: 500],
          leave_timeout_ms: 3_000
        ],
        opts
      )

    {:ok, sup} = ConsumerGroup.start_link(RecordingConsumer, ctx.group, [ctx.topic], opts)
    Process.unlink(sup)
    on_exit(fn -> stop_member(sup) end)
    sup
  end

  defp stop_member(sup) do
    if Process.alive?(sup) do
      try do
        Supervisor.stop(sup, :shutdown, 120_000)
      catch
        :exit, _ -> :ok
      end
    end
  end

  defp assignments(sup) do
    ConsumerGroup.assignments(sup, 5_000)
  catch
    :exit, _ -> []
  end

  defp balanced?(members) do
    lists = Enum.map(members, &assignments/1)
    all = Enum.concat(lists)
    Enum.all?(lists, &(&1 != [])) and length(all) == @partitions and length(Enum.uniq(all)) == @partitions
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
        Process.sleep(250)
        do_wait_until(fun, deadline)
    end
  end

  defp produce!(ctx, values) do
    values
    |> Enum.with_index()
    |> Enum.group_by(fn {_v, i} -> rem(i, @partitions) end, fn {v, _} -> %{value: v} end)
    |> Enum.each(fn {partition, messages} -> {:ok, _} = API.produce(ctx.client, ctx.topic, partition, messages) end)
  end

  defp collect(acc, expected, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_collect(acc, expected, deadline)
  end

  defp do_collect(acc, expected, deadline) do
    if MapSet.subset?(expected, acc) do
      acc
    else
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:consumed, _member, _tp, values} -> do_collect(Enum.into(values, acc), expected, deadline)
      after
        remaining -> acc
      end
    end
  end

  defp values(range), do: MapSet.new(range, &"v#{&1}")

  test "broker loss while a revoke is in flight: the members recover and no record is lost", ctx do
    produce!(ctx, Enum.map(1..200, &"v#{&1}"))
    m1 = start_member(ctx, :m1)
    wait_until(fn -> balanced?([m1]) end)
    acc = collect(MapSet.new(), values(1..200), 60_000)

    # m2 joins: m1 starts revoking half of its partitions; take the broker
    # away while that revoke (each consumer's final commit) is under way.
    m2 = start_member(ctx, :m2)
    wait_until(fn -> length(assignments(m1)) < @partitions end, 30_000)
    :ok = ToxiproxyHelpers.disable_proxy(ctx.toxiproxy_container, ctx.proxy_name)
    Process.sleep(8_000)
    assert Process.alive?(m1), "m1 died while the broker was unreachable"
    assert Process.alive?(m2), "m2 died while the broker was unreachable"
    :ok = ToxiproxyHelpers.enable_proxy(ctx.toxiproxy_container, ctx.proxy_name)

    wait_until(fn -> balanced?([m1, m2]) end, 120_000)
    produce!(ctx, Enum.map(201..400, &"v#{&1}"))
    acc = collect(acc, values(1..400), 120_000)
    missing = MapSet.difference(values(1..400), acc)
    assert MapSet.size(missing) == 0, "#{MapSet.size(missing)} records never consumed"
  end

  test "coordinator loss in steady state: heartbeats back off and the member keeps its partitions", ctx do
    m1 = start_member(ctx, :m1)
    wait_until(fn -> balanced?([m1]) end)
    pids = m1 |> ConsumerGroup.consumer_supervisor_pid() |> GenConsumerSupervisor.partition_pids()

    # Shorter than the broker's 45 s session timeout: the member is not
    # expired, so it must keep its consumers.
    :ok = ToxiproxyHelpers.disable_proxy(ctx.toxiproxy_container, ctx.proxy_name)
    Process.sleep(15_000)
    :ok = ToxiproxyHelpers.enable_proxy(ctx.toxiproxy_container, ctx.proxy_name)

    produce!(ctx, Enum.map(1..100, &"v#{&1}"))
    acc = collect(MapSet.new(), values(1..100), 120_000)
    assert MapSet.subset?(values(1..100), acc)

    after_pids = m1 |> ConsumerGroup.consumer_supervisor_pid() |> GenConsumerSupervisor.partition_pids()
    assert after_pids == pids, "consumers were restarted although the member was never fenced"
  end

  test "a shutdown while the broker is unreachable finishes within the shutdown budget", ctx do
    m1 = start_member(ctx, :m1)
    wait_until(fn -> balanced?([m1]) end)
    :ok = ToxiproxyHelpers.disable_proxy(ctx.toxiproxy_container, ctx.proxy_name)

    started = System.monotonic_time(:millisecond)
    stop_member(m1)
    elapsed = System.monotonic_time(:millisecond) - started
    refute Process.alive?(m1)

    budget =
      KafkaEx.Consumer.ConsumerGroup.ConsumerProtocolManager.shutdown_budget(
        fetch_options: [max_wait_time: 500],
        leave_timeout_ms: 3_000
      )

    assert elapsed < budget, "shutdown took #{elapsed} ms, budget #{budget} ms"
  end
end
