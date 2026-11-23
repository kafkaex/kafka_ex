defmodule KafkaEx.Client.MetadataStore.Snapshot do
  @moduledoc """
  The per-client ETS table holding the cluster metadata the metadata store fetched: the broker map with the
  controller id, topics, one row per partition with its leader and that leader's broker, consumer-group
  coordinators, topic ids, and a counter bumped on every applied response.

  The table is protected, so only the store writes it; any process reads it directly, without calling the client
  or the store. `select/3` routes a partition from that one partition row. On a deleted table, routing, counter
  and topic reads return `{:error, :unavailable}`; the broker list and the struct views come back empty.
  """

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic

  @type t :: :ets.tid()
  @type stamp :: {t(), non_neg_integer()}
  @type hints :: %{{binary(), integer()} => {integer(), stamp()}}
  @type freshness :: %{topics: %{binary() => non_neg_integer()}, brokers: non_neg_integer()}
  @type scope :: :all | :tracked | {:topics, [binary()]}

  @spec new() :: t()
  def new do
    tid = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
    :ets.insert(tid, [{:counter, 0}, {:brokers, %{}, nil}])
    tid
  end

  @spec counter(t()) :: {:ok, non_neg_integer()} | {:error, :unavailable}
  def counter(tid) do
    with {:ok, [{:counter, counter}]} <- lookup(tid, :counter), do: {:ok, counter}
  end

  @spec select(t(), NodeSelector.t(), hints()) ::
          {:ok, integer(), Broker.t()}
          | {:error, ClusterMetadata.node_select_error() | :broker_not_found | :unavailable}
  def select(tid, %NodeSelector{strategy: :topic_partition, topic: topic, partition: partition}, _hints) do
    with {:ok, leader, broker} <- partition_route(tid, topic, partition), do: found(leader, broker)
  end

  def select(tid, %NodeSelector{strategy: :topic_partition_replica, topic: topic, partition: partition}, hints) do
    with {:ok, leader, broker} <- partition_route(tid, topic, partition) do
      pick_preferred_or_leader(tid, Map.get(hints, {topic, partition}), leader, broker)
    end
  end

  def select(tid, %NodeSelector{strategy: :consumer_group, consumer_group_name: group}, _hints) do
    case lookup(tid, {:coordinator, group}) do
      {:ok, [{_, node_id}]} -> broker_route(tid, node_id, :broker_not_found)
      {:ok, []} -> {:error, :no_such_consumer_group}
      error -> error
    end
  end

  def select(tid, %NodeSelector{strategy: :controller}, hints) do
    with {:ok, [{:brokers, _, controller_id}]} <- lookup(tid, :brokers) do
      select(tid, NodeSelector.node_id(controller_id), hints)
    end
  end

  def select(tid, %NodeSelector{strategy: :node_id, node_id: node_id}, _hints) when is_integer(node_id) do
    broker_route(tid, node_id, :no_such_node)
  end

  def select(tid, %NodeSelector{strategy: :random}, _hints) do
    with {:ok, [{:brokers, brokers, _}]} <- lookup(tid, :brokers) do
      [node_id] = Enum.take_random(Map.keys(brokers), 1)
      {:ok, node_id, Map.fetch!(brokers, node_id)}
    end
  end

  @spec topics(t(), [binary()]) :: {:ok, [Topic.t()]} | {:error, :unavailable}
  def topics(tid, names) do
    with {:ok, topics} <- topics_by_name(tid, names), do: {:ok, Map.values(topics)}
  end

  @spec brokers(t()) :: [Broker.t()]
  def brokers(tid) do
    case lookup(tid, :brokers) do
      {:ok, [{:brokers, brokers, _}]} -> Map.values(brokers)
      {:error, :unavailable} -> []
    end
  end

  @spec to_cluster_metadata(t()) :: ClusterMetadata.t()
  def to_cluster_metadata(tid) do
    case match_object(tid, :_) do
      {:ok, rows} -> Enum.reduce(rows, %ClusterMetadata{}, &put_row/2)
      {:error, :unavailable} -> %ClusterMetadata{}
    end
  end

  @spec topic_view(t(), [binary()]) :: ClusterMetadata.t()
  def topic_view(tid, names) do
    case topics_by_name(tid, names) do
      {:ok, topics} -> %ClusterMetadata{topics: topics}
      {:error, :unavailable} -> %ClusterMetadata{}
    end
  end

  @spec topic_name_by_id(t(), binary()) :: {:ok, binary()} | {:error, :no_such_topic}
  def topic_name_by_id(tid, topic_id) do
    case lookup(tid, {:topic_id, topic_id}) do
      {:ok, [{_, name}]} -> {:ok, name}
      _ -> {:error, :no_such_topic}
    end
  end

  @spec apply(t(), ClusterMetadata.t(), %{seq: pos_integer(), scope: scope()}, freshness()) ::
          {:applied, freshness()} | :ignored
  def apply(_tid, %ClusterMetadata{brokers: brokers}, _request, _freshness) when map_size(brokers) == 0, do: :ignored

  def apply(tid, %ClusterMetadata{} = metadata, %{seq: seq, scope: scope}, freshness) do
    current = %{
      counter: :ets.lookup_element(tid, :counter, 2),
      brokers: :ets.lookup_element(tid, :brokers, 2),
      topics: Map.new(:ets.match_object(tid, {{:topic, :_}, :_}), fn {{:topic, name}, topic} -> {name, topic} end)
    }

    {rows, deletes, freshness} = plan_rows(current, metadata, seq, scope, freshness)
    :ets.insert(tid, rows)
    Enum.each(deletes, &:ets.delete(tid, &1))
    {:applied, freshness}
  end

  @spec put_coordinator(t(), binary(), integer()) :: true
  def put_coordinator(tid, group, node_id), do: :ets.insert(tid, {{:coordinator, group}, node_id})

  @spec drop_coordinator(t(), binary()) :: true
  def drop_coordinator(tid, group), do: :ets.delete(tid, {:coordinator, group})

  defp partition_route(tid, topic, partition) do
    case lookup(tid, {:partition, topic, partition}) do
      {:ok, [{_, leader, _}]} when leader < 0 -> {:error, :leader_not_available}
      {:ok, [{_, leader, broker}]} -> {:ok, leader, broker}
      {:ok, []} -> missing_partition(tid, topic)
      error -> error
    end
  end

  defp missing_partition(tid, topic) do
    case lookup(tid, {:topic, topic}) do
      {:ok, []} -> {:error, :no_such_topic}
      {:ok, [_]} -> {:error, :no_such_partition}
      error -> error
    end
  end

  defp pick_preferred_or_leader(_tid, nil, leader, broker), do: found(leader, broker)

  defp pick_preferred_or_leader(tid, {node_id, stamp}, leader, broker) do
    with {:ok, counter} <- counter(tid),
         true <- stamp == {tid, counter},
         {:ok, _, _} = preferred <- broker_route(tid, node_id, :no_such_node) do
      preferred
    else
      {:error, :unavailable} = error -> error
      _ -> found(leader, broker)
    end
  end

  defp broker_route(tid, node_id, missing) do
    with {:ok, [{:brokers, brokers, _}]} <- lookup(tid, :brokers) do
      case Map.fetch(brokers, node_id) do
        {:ok, broker} -> {:ok, node_id, broker}
        :error -> {:error, missing}
      end
    end
  end

  defp found(_node_id, nil), do: {:error, :broker_not_found}
  defp found(node_id, %Broker{} = broker), do: {:ok, node_id, broker}

  defp topics_by_name(tid, names) do
    Enum.reduce_while(names, {:ok, %{}}, fn name, {:ok, acc} ->
      case lookup(tid, {:topic, name}) do
        {:ok, [{_, topic}]} -> {:cont, {:ok, Map.put(acc, name, topic)}}
        {:ok, []} -> {:cont, {:ok, acc}}
        error -> {:halt, error}
      end
    end)
  end

  defp put_row({:brokers, brokers, controller_id}, metadata) do
    %{metadata | brokers: brokers, controller_id: controller_id}
  end

  defp put_row({{:topic, name}, topic}, metadata), do: %{metadata | topics: Map.put(metadata.topics, name, topic)}

  defp put_row({{:coordinator, group}, node_id}, metadata) do
    %{metadata | consumer_group_coordinators: Map.put(metadata.consumer_group_coordinators, group, node_id)}
  end

  defp put_row(_row, metadata), do: metadata

  defp lookup(tid, key) do
    {:ok, :ets.lookup(tid, key)}
  rescue
    ArgumentError -> {:error, :unavailable}
  end

  defp match_object(tid, pattern) do
    {:ok, :ets.match_object(tid, pattern)}
  rescue
    ArgumentError -> {:error, :unavailable}
  end

  defp plan_rows(current, %ClusterMetadata{} = metadata, seq, scope, freshness) do
    write_brokers? = seq > freshness.brokers
    brokers = if write_brokers?, do: without_sockets(metadata.brokers), else: current.brokers
    written = Map.filter(metadata.topics, fn {name, _} -> newer?(seq, freshness, name) end)
    dropped = dropped_topics(current.topics, metadata.topics, seq, scope, freshness)
    kept = current.topics |> Map.drop(Map.keys(written)) |> Map.drop(dropped)

    rows =
      broker_rows(write_brokers?, brokers, metadata.controller_id) ++
        Enum.flat_map(written, fn {_, topic} -> topic_rows(topic, brokers) end) ++
        rerouted_rows(kept, changed_nodes(write_brokers?, current.brokers, brokers), brokers) ++
        [{:counter, current.counter + 1}]

    deletes =
      Enum.flat_map(dropped, &topic_keys(Map.fetch!(current.topics, &1))) ++
        Enum.flat_map(written, fn {name, topic} -> stale_keys(Map.get(current.topics, name), topic) end)

    touched = Map.new(Map.keys(written) ++ dropped, &{&1, seq})

    freshness = %{
      topics: Map.merge(freshness.topics, touched),
      brokers: if(write_brokers?, do: seq, else: freshness.brokers)
    }

    {rows, deletes, freshness}
  end

  defp newer?(seq, freshness, name), do: seq > Map.get(freshness.topics, name, 0)

  defp without_sockets(brokers), do: Map.new(brokers, fn {id, broker} -> {id, %{broker | socket: nil}} end)

  defp dropped_topics(_current, _response, _seq, {:topics, _}, _freshness), do: []

  defp dropped_topics(current, response, seq, scope, freshness) when scope in [:all, :tracked] do
    for {name, _} <- current, not Map.has_key?(response, name), newer?(seq, freshness, name), do: name
  end

  defp broker_rows(false, _brokers, _controller_id), do: []

  defp broker_rows(true, brokers, controller_id), do: [{:brokers, brokers, controller_id}]

  defp topic_rows(%Topic{name: name, partition_leaders: leaders} = topic, brokers) do
    [{{:topic, name}, topic} | topic_id_rows(topic)] ++
      Enum.map(leaders, fn {partition, leader} -> partition_row(name, partition, leader, brokers) end)
  end

  defp topic_id_rows(%Topic{name: name, topic_id: <<_::128>> = id}), do: [{{:topic_id, id}, name}]
  defp topic_id_rows(%Topic{}), do: []

  defp partition_row(name, partition, leader, brokers) do
    {{:partition, name, partition}, leader, Map.get(brokers, leader)}
  end

  defp changed_nodes(false, _old, _new), do: MapSet.new()

  defp changed_nodes(true, old, new) do
    for id <- Map.keys(old) ++ Map.keys(new), Map.get(old, id) != Map.get(new, id), into: MapSet.new(), do: id
  end

  defp rerouted_rows(topics, changed, brokers) do
    for {name, %Topic{partition_leaders: leaders}} <- topics,
        {partition, leader} <- leaders,
        MapSet.member?(changed, leader) do
      partition_row(name, partition, leader, brokers)
    end
  end

  defp topic_keys(%Topic{name: name, partition_leaders: leaders} = topic) do
    [{:topic, name} | Enum.map(topic_id_rows(topic), &elem(&1, 0))] ++
      Enum.map(Map.keys(leaders), &{:partition, name, &1})
  end

  defp stale_keys(nil, _topic), do: []

  defp stale_keys(%Topic{name: name} = old, %Topic{} = new) do
    lost_partitions = Map.keys(old.partition_leaders) -- Map.keys(new.partition_leaders)
    lost_ids = Enum.map(topic_id_rows(old) -- topic_id_rows(new), &elem(&1, 0))
    lost_ids ++ Enum.map(lost_partitions, &{:partition, name, &1})
  end
end
