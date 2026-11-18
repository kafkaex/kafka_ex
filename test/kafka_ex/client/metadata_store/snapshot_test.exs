defmodule KafkaEx.Client.MetadataStore.SnapshotTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.MetadataStore.Snapshot
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.PartitionInfo
  alias KafkaEx.Cluster.Topic

  @fresh %{topics: %{}, brokers: 0}
  @topic_id <<7::128>>

  describe "select/3" do
    test "answers every selector exactly as ClusterMetadata.select_node/2 and the broker lookup do" do
      base = routing_metadata()

      cases = [
        {"present partition", base, NodeSelector.topic_partition("t", 0), %{}, %{}},
        {"missing topic", base, NodeSelector.topic_partition("missing", 0), %{}, %{}},
        {"missing partition", base, NodeSelector.topic_partition("t", 7), %{}, %{}},
        {"leader -1", base, NodeSelector.topic_partition("t", 1), %{}, %{}},
        {"leader on an unknown node", base, NodeSelector.topic_partition("t", 2), %{}, %{}},
        {"coordinator present", base, NodeSelector.consumer_group("g"), %{}, %{}},
        {"coordinator missing", base, NodeSelector.consumer_group("none"), %{}, %{}},
        {"coordinator on an unknown node", base, NodeSelector.consumer_group("lost"), %{}, %{}},
        {"controller known", base, NodeSelector.controller(), %{}, %{}},
        {"controller nil", %{base | controller_id: nil}, NodeSelector.controller(), %{}, %{}},
        {"controller on an unknown node", %{base | controller_id: 9}, NodeSelector.controller(), %{}, %{}},
        {"node_id known", base, NodeSelector.node_id(2), %{}, %{}},
        {"node_id unknown", base, NodeSelector.node_id(9), %{}, %{}},
        {"random", single_broker(base), NodeSelector.random(), %{}, %{}},
        {"random with no brokers", %ClusterMetadata{}, NodeSelector.random(), %{}, %{}},
        {"replica without a hint", base, NodeSelector.topic_partition_replica("t", 0), %{}, %{}},
        {"replica hint valid", base, NodeSelector.topic_partition_replica("t", 0), %{{"t", 0} => 2},
         %{{"t", 0} => {2, :current}}},
        {"replica hint stale", base, NodeSelector.topic_partition_replica("t", 0), %{}, %{{"t", 0} => {2, :stale}}},
        {"replica hint naming an unknown node", base, NodeSelector.topic_partition_replica("t", 0), %{{"t", 0} => 9},
         %{{"t", 0} => {9, :current}}},
        {"replica hint on a leaderless partition", base, NodeSelector.topic_partition_replica("t", 1), %{{"t", 1} => 2},
         %{{"t", 1} => {2, :current}}},
        {"replica hint valid while the leader's broker is unknown", base, NodeSelector.topic_partition_replica("t", 2),
         %{{"t", 2} => 2}, %{{"t", 2} => {2, :current}}}
      ]

      for {name, metadata, selector, preferred, hints} <- cases do
        tid = applied(metadata)
        {:ok, counter} = Snapshot.counter(tid)

        stamped =
          Map.new(hints, fn
            {key, {node, :current}} -> {key, {node, {tid, counter}}}
            {key, {node, :stale}} -> {key, {node, {tid, counter - 1}}}
          end)

        expected =
          outcome(fn ->
            State.select_broker_entry(
              %State{cluster_metadata: %{metadata | preferred_replicas: preferred}},
              selector
            )
          end)

        assert outcome(fn -> Snapshot.select(tid, selector, stamped) end) == expected, name
      end
    end
  end

  describe "atomic swaps" do
    test "a routing read never sees a snapshot mid-replacement" do
      test = self()
      host = "goal2"
      a = snapshot(host, [{1, 9092}, {2, 9093}], 1)
      b = snapshot(host, [{1, 9093}, {2, 9092}], 2)

      writer =
        spawn_link(fn ->
          tid = Snapshot.new()
          send(test, {:table, tid})
          alternate(tid, a, b, 1, %{topics: %{}, brokers: 0})
        end)

      assert_receive {:table, tid}, 1_000
      deadline = System.monotonic_time(:millisecond) + 5_000
      seen = read_routes(tid, MapSet.new(), 0, deadline)
      send(writer, :stop)

      assert MapSet.subset?(seen, MapSet.new([{1, 1, 9092}, {2, 2, 9092}]))
      assert MapSet.size(seen) == 2
    end
  end

  describe "freshness" do
    test "an older seq never overwrites a newer topic" do
      tid = Snapshot.new()
      newer = snapshot("h", [{1, 9092}, {2, 9093}], 2)
      older = %{snapshot("h", [{1, 9094}, {2, 9095}], 1) | controller_id: 2}

      {:applied, freshness} = Snapshot.apply(tid, newer, %{seq: 2, scope: :tracked}, @fresh)
      {:applied, freshness} = Snapshot.apply(tid, older, %{seq: 2, scope: :tracked}, freshness)
      {:applied, _} = Snapshot.apply(tid, older, %{seq: 1, scope: :tracked}, freshness)

      assert {:ok, 2, %Broker{node_id: 2, port: 9093}} = Snapshot.select(tid, NodeSelector.topic_partition("t", 0), %{})
      assert {:ok, 1, %Broker{port: 9092}} = Snapshot.select(tid, NodeSelector.controller(), %{})
    end

    test "a rewritten topic loses the partitions and the id it no longer has" do
      tid = Snapshot.new()
      wide = with_topic(snapshot("h", [{1, 9092}], 1), topic("t", 1, @topic_id))
      narrow = with_topic(wide, topic("t", 1, <<8::128>>, 0..3))

      {:applied, freshness} = Snapshot.apply(tid, wide, %{seq: 1, scope: :tracked}, @fresh)
      {:applied, _} = Snapshot.apply(tid, narrow, %{seq: 2, scope: :tracked}, freshness)

      assert Snapshot.select(tid, NodeSelector.topic_partition("t", 5), %{}) == {:error, :no_such_partition}
      assert Snapshot.topic_name_by_id(tid, @topic_id) == {:error, :no_such_topic}
      assert Snapshot.topic_name_by_id(tid, <<8::128>>) == {:ok, "t"}
    end

    test "a tracked apply drops topics the response omits, a {:topics, _} apply merges" do
      tid = Snapshot.new()
      both = with_topic(snapshot("h", [{1, 9092}], 1), topic("x", 1, @topic_id))
      only_t = snapshot("h", [{1, 9092}], 1)
      x_route = NodeSelector.topic_partition("x", 0)

      {:applied, freshness} = Snapshot.apply(tid, both, %{seq: 1, scope: :tracked}, @fresh)
      {:applied, freshness} = Snapshot.apply(tid, only_t, %{seq: 3, scope: {:topics, ["t"]}}, freshness)

      assert {:ok, [%Topic{name: "x"}]} = Snapshot.topics(tid, ["x"])
      assert {:ok, 1, %Broker{}} = Snapshot.select(tid, x_route, %{})

      {:applied, freshness} = Snapshot.apply(tid, only_t, %{seq: 4, scope: :tracked}, freshness)

      assert Snapshot.topics(tid, ["x"]) == {:ok, []}
      assert Snapshot.select(tid, x_route, %{}) == {:error, :no_such_topic}
      assert Snapshot.topic_name_by_id(tid, @topic_id) == {:error, :no_such_topic}

      {:applied, _} = Snapshot.apply(tid, both, %{seq: 2, scope: :tracked}, freshness)

      assert Snapshot.topics(tid, ["x"]) == {:ok, []}
      assert Snapshot.select(tid, x_route, %{}) == {:error, :no_such_topic}
    end

    test "an address change reroutes every partition the node leads, in topics the response omits too" do
      tid = Snapshot.new()

      base = snapshot("h", [{1, 9092}, {2, 9093}], 1)
      first = base |> with_topic(topic("by_two", 2)) |> with_topic(topic("by_three", 3))

      moved = %ClusterMetadata{
        brokers: brokers("h", [{1, 9094}, {3, 9095}]),
        controller_id: 1,
        topics: %{"other" => topic("other", 1)}
      }

      {:applied, freshness} = Snapshot.apply(tid, first, %{seq: 1, scope: :tracked}, @fresh)
      assert Snapshot.select(tid, NodeSelector.topic_partition("by_three", 0), %{}) == {:error, :broker_not_found}

      {:applied, _} = Snapshot.apply(tid, moved, %{seq: 2, scope: {:topics, ["other"]}}, freshness)

      for p <- 0..7 do
        assert {:ok, 1, %Broker{port: 9094}} = Snapshot.select(tid, NodeSelector.topic_partition("t", p), %{})
      end

      assert Snapshot.select(tid, NodeSelector.topic_partition("by_two", 0), %{}) == {:error, :broker_not_found}
      assert Snapshot.select(tid, NodeSelector.node_id(2), %{}) == {:error, :no_such_node}
      assert Enum.sort(Enum.map(Snapshot.brokers(tid), & &1.node_id)) == [1, 3]

      assert {:ok, 3, %Broker{port: 9095}} =
               Snapshot.select(tid, NodeSelector.topic_partition("by_three", 0), %{})
    end
  end

  describe "counter" do
    test "every applied response bumps the counter once; put/drop_coordinator do not" do
      tid = Snapshot.new()
      metadata = snapshot("h", [{1, 9092}], 1)

      assert Snapshot.counter(tid) == {:ok, 0}
      {:applied, freshness} = Snapshot.apply(tid, metadata, %{seq: 1, scope: :tracked}, @fresh)
      assert Snapshot.counter(tid) == {:ok, 1}
      {:applied, freshness} = Snapshot.apply(tid, metadata, %{seq: 2, scope: :tracked}, freshness)
      assert Snapshot.counter(tid) == {:ok, 2}

      Snapshot.put_coordinator(tid, "g", 1)
      Snapshot.drop_coordinator(tid, "g")
      :ignored = Snapshot.apply(tid, %{metadata | brokers: %{}}, %{seq: 3, scope: :tracked}, freshness)

      assert Snapshot.counter(tid) == {:ok, 2}
    end
  end

  describe "guarded reads" do
    test "reads on a deleted table are misses, never raises" do
      tid = applied(routing_metadata())
      :ets.delete(tid)

      assert Snapshot.select(tid, NodeSelector.topic_partition("t", 0), %{}) == {:error, :unavailable}
      assert Snapshot.select(tid, NodeSelector.consumer_group("g"), %{}) == {:error, :unavailable}
      assert Snapshot.select(tid, NodeSelector.controller(), %{}) == {:error, :unavailable}
      assert Snapshot.select(tid, NodeSelector.random(), %{}) == {:error, :unavailable}
      assert Snapshot.topics(tid, ["t"]) == {:error, :unavailable}
      assert Snapshot.counter(tid) == {:error, :unavailable}
      assert Snapshot.topic_name_by_id(tid, @topic_id) == {:error, :no_such_topic}
      assert Snapshot.brokers(tid) == []
      assert Snapshot.to_cluster_metadata(tid) == %ClusterMetadata{}
      assert Snapshot.topic_view(tid, ["t"]) == %ClusterMetadata{}
    end
  end

  describe "views" do
    test "to_cluster_metadata/1 round-trips brokers, controller, topics and coordinators" do
      metadata = routing_metadata()
      tid = applied(metadata)

      assert Snapshot.to_cluster_metadata(tid) == metadata
      assert Enum.sort_by(Snapshot.brokers(tid), & &1.node_id) == ClusterMetadata.brokers(metadata)
      assert Snapshot.topics(tid, ["u", "t", "missing"]) == {:ok, ClusterMetadata.topics_metadata(metadata, ["u", "t"])}
      assert Snapshot.topic_name_by_id(tid, @topic_id) == {:ok, "u"}
      assert Snapshot.topic_view(tid, ["u"]) == %ClusterMetadata{topics: Map.take(metadata.topics, ["u"])}
    end
  end

  defp alternate(tid, a, b, seq, freshness) do
    receive do
      :stop -> :ok
    after
      0 ->
        {:applied, freshness} =
          Snapshot.apply(tid, if(rem(seq, 2) == 0, do: a, else: b), %{seq: seq, scope: :tracked}, freshness)

        alternate(tid, a, b, seq + 1, freshness)
    end
  end

  @reads 20_000

  defp read_routes(tid, seen, reads, deadline) do
    seen =
      Enum.reduce(0..7, seen, fn p, acc ->
        case Snapshot.select(tid, NodeSelector.topic_partition("t", p), %{}) do
          {:ok, leader, %Broker{node_id: node, port: port}} -> MapSet.put(acc, {leader, node, port})
          {:error, _} -> acc
        end
      end)

    done? = reads >= @reads and MapSet.size(seen) >= 2

    if done? or System.monotonic_time(:millisecond) > deadline do
      seen
    else
      read_routes(tid, seen, reads + 1, deadline)
    end
  end

  defp outcome(fun) do
    fun.()
  rescue
    e -> {:raised, e.__struct__}
  end

  defp applied(%ClusterMetadata{brokers: brokers}) when map_size(brokers) == 0, do: Snapshot.new()

  defp applied(%ClusterMetadata{} = metadata) do
    tid = Snapshot.new()
    {:applied, _} = Snapshot.apply(tid, metadata, %{seq: 1, scope: :all}, @fresh)
    Enum.each(metadata.consumer_group_coordinators, fn {group, node} -> Snapshot.put_coordinator(tid, group, node) end)
    tid
  end

  defp routing_metadata do
    t = %Topic{
      name: "t",
      partition_leaders: %{0 => 1, 1 => -1, 2 => 9},
      partitions: [
        PartitionInfo.build(partition_id: 0, leader: 1, replicas: [1, 2], isr: [1, 2]),
        PartitionInfo.build(partition_id: 1, leader: -1, replicas: [2], isr: []),
        PartitionInfo.build(partition_id: 2, leader: 9, replicas: [9], isr: [9])
      ]
    }

    %ClusterMetadata{
      brokers: brokers("h", [{1, 9092}, {2, 9093}]),
      controller_id: 1,
      topics: %{"t" => t, "u" => topic("u", 2, @topic_id)},
      consumer_group_coordinators: %{"g" => 2, "lost" => 9}
    }
  end

  defp single_broker(%ClusterMetadata{brokers: brokers} = metadata) do
    %{metadata | brokers: Map.take(brokers, [1])}
  end

  defp snapshot(host, brokers, leader) do
    %ClusterMetadata{brokers: brokers(host, brokers), controller_id: 1, topics: %{"t" => topic("t", leader)}}
  end

  defp with_topic(%ClusterMetadata{topics: topics} = metadata, %Topic{name: name} = topic) do
    %{metadata | topics: Map.put(topics, name, topic)}
  end

  defp brokers(host, brokers) do
    Map.new(brokers, fn {node, port} -> {node, %Broker{node_id: node, host: host, port: port}} end)
  end

  defp topic(name, leader, topic_id \\ nil, partitions \\ 0..7) do
    %Topic{
      name: name,
      topic_id: topic_id,
      partition_leaders: Map.new(partitions, &{&1, leader}),
      partitions:
        Enum.map(partitions, &PartitionInfo.build(partition_id: &1, leader: leader, replicas: [leader], isr: [leader]))
    }
  end
end
