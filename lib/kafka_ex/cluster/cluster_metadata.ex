defmodule KafkaEx.Cluster.ClusterMetadata do
  @moduledoc """
  Encapsulates what we know about the state of a Kafka broker cluster

  This module is mainly used internally in New.Client, but some of its
  functions may be useful for extracting metadata information
  """

  alias KafkaEx.API, as: KafkaExAPI
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.Topic

  defstruct brokers: %{}, controller_id: nil, topics: %{}, consumer_group_coordinators: %{}

  @type t :: %__MODULE__{
          brokers: %{KafkaExAPI.node_id() => Broker.t()},
          controller_id: KafkaExAPI.node_id() | nil,
          topics: %{KafkaExAPI.topic_name() => Topic.t()},
          consumer_group_coordinators: %{KafkaExAPI.consumer_group_name() => KafkaExAPI.node_id()}
        }

  @typedoc """
  Possible errors given by `select_node/2`
  """
  @type node_select_error :: :no_such_node | :no_such_topic | :no_such_partition | :no_such_consumer_group

  @doc """
  List names of topics known by the cluster metadata

  Tthis is a subset of the topics in the cluster - it will only contain topics for which we have fetched metadata
  """
  @spec known_topics(t) :: [KafkaExAPI.topic_name()]
  def known_topics(%__MODULE__{topics: topics}), do: Map.keys(topics)

  @doc """
  Return the metadata for the given topics
  """
  @spec topics_metadata(t, [KafkaExAPI.topic_name()]) :: [Topic.t()]
  def topics_metadata(%__MODULE__{topics: topics}, wanted_topics) do
    topics
    |> Map.take(wanted_topics)
    |> Map.values()
  end

  @doc """
  Return a list of the known brokers
  """
  @spec brokers(t) :: [Broker.t()]
  def brokers(%__MODULE__{brokers: brokers}), do: Map.values(brokers)

  @doc """
  Find the node id for a given selector

  Note this will not update the metadata, only select a node given the current metadata.

  See `t:KafkaEx.Client.NodeSelector.t/0`
  """
  @spec select_node(t, NodeSelector.t()) :: {:ok, KafkaExAPI.node_id()} | {:error, node_select_error}
  def select_node(%__MODULE__{controller_id: controller_id} = cluster_metadata, %NodeSelector{strategy: :controller}) do
    select_node(cluster_metadata, NodeSelector.node_id(controller_id))
  end

  def select_node(%__MODULE__{brokers: brokers}, %NodeSelector{strategy: :random}) do
    [node_id] = Enum.take_random(Map.keys(brokers), 1)
    {:ok, node_id}
  end

  def select_node(%__MODULE__{} = cluster_metadata, %NodeSelector{
        strategy: :topic_partition,
        topic: topic,
        partition: partition
      }) do
    case Map.fetch(cluster_metadata.topics, topic) do
      :error ->
        {:error, :no_such_topic}

      {:ok, %Topic{partition_leaders: partition_leaders}} ->
        case Map.fetch(partition_leaders, partition) do
          :error -> {:error, :no_such_partition}
          {:ok, node_id} -> {:ok, node_id}
        end
    end
  end

  def select_node(%__MODULE__{} = cluster_metadata, %NodeSelector{
        strategy: :consumer_group,
        consumer_group_name: consumer_group
      }) do
    case Map.fetch(cluster_metadata.consumer_group_coordinators, consumer_group) do
      :error -> {:error, :no_such_consumer_group}
      {:ok, coordinator_node_id} -> {:ok, coordinator_node_id}
    end
  end

  def select_node(%__MODULE__{} = cluster_metadata, %NodeSelector{strategy: :node_id, node_id: node_id})
      when is_integer(node_id) do
    case Map.fetch(cluster_metadata.brokers, node_id) do
      {:ok, _broker} -> {:ok, node_id}
      :error -> {:error, :no_such_node}
    end
  end

  @doc """
  Constructs a `t:t/0` from a `Kayrock.Metadata.V1.Response` struct.

  The `V1` here is a minimum - this should work with higher versions of the
  metadata response struct.
  """
  @spec from_metadata_v1_response(map) :: t
  def from_metadata_v1_response(metadata) do
    brokers =
      Enum.into(metadata.brokers, %{}, fn broker_metadata ->
        %{host: host, port: port, node_id: node_id, rack: rack} = broker_metadata
        {node_id, %Broker{host: host, port: port, node_id: node_id, rack: rack}}
      end)

    topics =
      metadata.topics
      |> Enum.filter(fn topic_entry -> topic_entry.error_code == 0 end)
      |> Enum.into(%{}, fn topic_entry ->
        case topic_entry do
          %{name: topic_name, error_code: 0} -> {topic_name, Topic.from_topic_metadata(topic_entry)}
          _ -> nil
        end
      end)

    %__MODULE__{brokers: brokers, controller_id: metadata.controller_id, topics: topics}
  end

  @doc """
  Merge two sets of cluster metadata with their brokers.
  Returns the merged metadata and a list of brokers that should have their connections closed
  because they are not present in the new metadata
  """
  @spec merge_brokers(t, t) :: {t, [Broker.t()]}
  def merge_brokers(%__MODULE__{} = old_cluster_metadata, %__MODULE__{} = new_cluster_metadata) do
    old_brokers = Map.values(old_cluster_metadata.brokers)

    new_brokers =
      Enum.into(new_cluster_metadata.brokers, %{}, fn {node_id, new_broker} ->
        case Enum.find(old_brokers, &(&1.host == new_broker.host && &1.port == new_broker.port)) do
          %Broker{socket: socket} when not is_nil(socket) -> {node_id, %{new_broker | socket: socket}}
          _ -> {node_id, new_broker}
        end
      end)

    brokers_to_close =
      old_brokers
      |> Enum.filter(fn b ->
        !Enum.any?(new_brokers, fn {_, nb} ->
          nb.host == b.host && nb.port == b.port
        end)
      end)

    {%{new_cluster_metadata | brokers: new_brokers}, brokers_to_close}
  end

  @doc """
  Returns a `t:Broker.t/0` for the given `t:KafkaExAPI.node_id/0` or `nil` if
  there is no known broker with that node id
  """
  @spec broker_by_node_id(t, KafkaExAPI.node_id()) :: Broker.t() | nil
  def broker_by_node_id(%__MODULE__{brokers: brokers}, node_id) do
    Map.get(brokers, node_id)
  end

  @doc """
  Execute an update callback on each broker
  """
  @spec update_brokers(t, (Broker.t() -> Broker.t())) :: t
  def update_brokers(%__MODULE__{brokers: brokers} = cluster_metadata, cb) when is_function(cb, 1) do
    updated_brokers = Enum.into(brokers, %{}, fn {node_id, broker} -> {node_id, cb.(broker)} end)
    %{cluster_metadata | brokers: updated_brokers}
  end

  @doc """
  update a consumer group coordinator node id
  """
  @spec put_consumer_group_coordinator(t, KafkaExAPI.consumer_group_name(), KafkaExAPI.node_id()) :: t
  def put_consumer_group_coordinator(
        %__MODULE__{consumer_group_coordinators: consumer_group_coordinators} = cluster_metadata,
        consumer_group,
        coordinator_node_id
      ) do
    %{
      cluster_metadata
      | consumer_group_coordinators:
          Map.put(
            consumer_group_coordinators,
            consumer_group,
            coordinator_node_id
          )
    }
  end

  @doc """
  remove the given topics (e.g., when they are deleted)
  """
  @spec remove_topics(t, [KafkaExAPI.topic_name()]) :: t
  def remove_topics(%__MODULE__{topics: topics} = cluster_metadata, topics_to_remove) do
    %{cluster_metadata | topics: Map.drop(topics, topics_to_remove)}
  end
end
