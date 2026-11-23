defmodule KafkaEx.Client.MetadataStore.MetadataTable do
  @moduledoc """
  The per-client ETS table holding the cluster metadata the metadata store fetched, as `KafkaEx.Cluster` structs:
  one row per broker, the controller id, topics, an index row each of broker ids and topic names, one `PartitionInfo` row per partition, consumer-group
  coordinators, topic ids, and a counter bumped on every applied response.

  The table is protected, so only the store writes it; any process reads it directly, without calling the client
  or the store. `select/3` routes a partition with two narrow lookups, the partition row and its leader's broker
  row. On a deleted table, routing, counter and topic reads return `{:error, :unavailable}`; the broker list and
  the struct views come back empty.
  """

  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.PartitionInfo
  alias KafkaEx.Cluster.Topic

  @type t :: :ets.tid()
  @type stamp :: {t(), non_neg_integer()}
  @type hints :: %{{binary(), integer()} => {integer(), stamp()}}
  @type freshness :: %{topics: %{binary() => non_neg_integer()}, brokers: non_neg_integer()}
  @type scope :: :all | :tracked | {:topics, [binary()]}

  @spec new() :: t()
  def new do
    tid = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
    :ets.insert(tid, [{:counter, 0}, {:controller_id, nil}, {:broker_ids, []}, {:topic_names, []}])
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
    with {:ok, leader} <- partition_route(tid, topic, partition), do: broker_route(tid, leader, :broker_not_found)
  end

  def select(tid, %NodeSelector{strategy: :topic_partition_replica, topic: topic, partition: partition}, hints) do
    with {:ok, leader} <- partition_route(tid, topic, partition) do
      pick_preferred_or_leader(tid, Map.get(hints, {topic, partition}), leader)
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
    with {:ok, [{:controller_id, controller_id}]} <- lookup(tid, :controller_id) do
      select(tid, NodeSelector.node_id(controller_id), hints)
    end
  end

  def select(tid, %NodeSelector{strategy: :node_id, node_id: node_id}, _hints) when is_integer(node_id) do
    broker_route(tid, node_id, :no_such_node)
  end

  def select(tid, %NodeSelector{strategy: :random}, _hints) do
    with {:ok, entries} <- broker_entries(tid) do
      [{node_id, broker}] = Enum.take_random(entries, 1)
      {:ok, node_id, broker}
    end
  end

  @spec topics(t(), [binary()]) :: {:ok, [Topic.t()]} | {:error, :unavailable}
  def topics(tid, names) do
    with {:ok, topics} <- topics_by_name(tid, names), do: {:ok, Map.values(topics)}
  end

  @spec brokers(t()) :: [Broker.t()]
  def brokers(tid) do
    case broker_entries(tid) do
      {:ok, entries} -> entries |> Enum.map(&elem(&1, 1)) |> Enum.sort_by(& &1.node_id)
      {:error, :unavailable} -> []
    end
  end

  @spec to_cluster_metadata(t()) :: ClusterMetadata.t()
  def to_cluster_metadata(tid) do
    with {:ok, brokers} <- broker_entries(tid),
         {:ok, [{_, names}]} <- lookup(tid, :topic_names),
         {:ok, topics} <- topics_by_name(tid, names),
         {:ok, [{_, controller_id}]} <- lookup(tid, :controller_id),
         {:ok, coordinators} <- match_object(tid, {{:coordinator, :_}, :_}) do
      %ClusterMetadata{
        brokers: Map.new(brokers),
        topics: topics,
        controller_id: controller_id,
        consumer_group_coordinators: Map.new(coordinators, fn {{:coordinator, group}, node_id} -> {group, node_id} end)
      }
    else
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
    topic_names = :ets.lookup_element(tid, :topic_names, 2)
    known = MapSet.new(topic_names)

    current = %{
      counter: :ets.lookup_element(tid, :counter, 2),
      broker_ids: :ets.lookup_element(tid, :broker_ids, 2),
      topic_names: topic_names,
      topic: fn name -> MapSet.member?(known, name) && :ets.lookup_element(tid, {:topic, name}, 2) end
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
      {:ok, [{_, %PartitionInfo{leader: leader}}]} when leader < 0 -> {:error, :leader_not_available}
      {:ok, [{_, %PartitionInfo{leader: leader}}]} -> {:ok, leader}
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

  defp pick_preferred_or_leader(tid, nil, leader), do: broker_route(tid, leader, :broker_not_found)

  defp pick_preferred_or_leader(tid, {node_id, stamp}, leader) do
    with {:ok, counter} <- counter(tid),
         true <- stamp == {tid, counter},
         {:ok, _, _} = preferred <- broker_route(tid, node_id, :no_such_node) do
      preferred
    else
      {:error, :unavailable} = error -> error
      _ -> broker_route(tid, leader, :broker_not_found)
    end
  end

  defp broker_route(tid, node_id, missing) do
    case lookup(tid, {:broker, node_id}) do
      {:ok, [{_, broker}]} -> {:ok, node_id, broker}
      {:ok, []} -> {:error, missing}
      error -> error
    end
  end

  defp broker_entries(tid) do
    with {:ok, [{_, ids}]} <- lookup(tid, :broker_ids) do
      Enum.reduce_while(ids, {:ok, []}, fn id, {:ok, acc} ->
        case lookup(tid, {:broker, id}) do
          {:ok, [{_, broker}]} -> {:cont, {:ok, [{id, broker} | acc]}}
          {:ok, []} -> {:cont, {:ok, acc}}
          error -> {:halt, error}
        end
      end)
    end
  end

  defp topics_by_name(tid, names) do
    Enum.reduce_while(names, {:ok, %{}}, fn name, {:ok, acc} ->
      case lookup(tid, {:topic, name}) do
        {:ok, [{_, topic}]} -> {:cont, {:ok, Map.put(acc, name, topic)}}
        {:ok, []} -> {:cont, {:ok, acc}}
        error -> {:halt, error}
      end
    end)
  end

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
    written = Map.filter(metadata.topics, fn {name, _} -> newer?(seq, freshness, name) end)
    dropped = dropped_topics(current.topic_names, metadata.topics, seq, scope, freshness)

    rows =
      cluster_rows(write_brokers?, metadata) ++
        Enum.flat_map(written, fn {_, topic} -> topic_rows(topic) end) ++
        [{:counter, current.counter + 1}, {:topic_names, topic_names(current.topic_names, written, dropped)}]

    deletes =
      lost_broker_keys(write_brokers?, current.broker_ids, metadata.brokers) ++
        Enum.flat_map(dropped, &topic_keys(current.topic.(&1))) ++
        Enum.flat_map(written, fn {name, topic} -> stale_keys(current.topic.(name), topic) end)

    touched = Map.new(Map.keys(written) ++ dropped, &{&1, seq})

    freshness = %{
      topics: Map.merge(freshness.topics, touched),
      brokers: if(write_brokers?, do: seq, else: freshness.brokers)
    }

    {rows, deletes, freshness}
  end

  defp topic_names(current, written, dropped), do: Enum.uniq(current ++ Map.keys(written)) -- dropped

  defp newer?(seq, freshness, name), do: seq > Map.get(freshness.topics, name, 0)

  defp dropped_topics(_current, _response, _seq, {:topics, _}, _freshness), do: []

  defp dropped_topics(current, response, seq, scope, freshness) when scope in [:all, :tracked] do
    for name <- current, not Map.has_key?(response, name), newer?(seq, freshness, name), do: name
  end

  defp cluster_rows(false, _metadata), do: []

  defp cluster_rows(true, %ClusterMetadata{brokers: brokers, controller_id: controller_id}) do
    [
      {:controller_id, controller_id},
      {:broker_ids, Map.keys(brokers)}
      | Enum.map(brokers, fn {id, broker} -> {{:broker, id}, %{broker | socket: nil}} end)
    ]
  end

  defp lost_broker_keys(false, _current_ids, _brokers), do: []
  defp lost_broker_keys(true, current_ids, brokers), do: Enum.map(current_ids -- Map.keys(brokers), &{:broker, &1})

  defp topic_rows(%Topic{name: name, partitions: partitions} = topic) do
    [{{:topic, name}, topic} | topic_id_rows(topic)] ++
      Enum.map(partitions, &{{:partition, name, &1.partition_id}, &1})
  end

  defp topic_id_rows(%Topic{name: name, topic_id: <<_::128>> = id}), do: [{{:topic_id, id}, name}]
  defp topic_id_rows(%Topic{}), do: []

  defp topic_keys(%Topic{name: name} = topic) do
    [{:topic, name} | Enum.map(topic_id_rows(topic), &elem(&1, 0))] ++
      Enum.map(partition_ids(topic), &{:partition, name, &1})
  end

  defp partition_ids(%Topic{partitions: partitions}), do: Enum.map(partitions, & &1.partition_id)

  defp stale_keys(false, _topic), do: []

  defp stale_keys(%Topic{name: name} = old, %Topic{} = new) do
    lost_partitions = partition_ids(old) -- partition_ids(new)
    lost_ids = Enum.map(topic_id_rows(old) -- topic_id_rows(new), &elem(&1, 0))
    lost_ids ++ Enum.map(lost_partitions, &{:partition, name, &1})
  end
end
