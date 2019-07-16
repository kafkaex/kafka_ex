defmodule KafkaEx.New.ClusterMetadata do
  @moduledoc """
  Encapsulates what we know about the state of a Kafka broker cluster
  """

  defstruct brokers: %{},
            controller_id: nil,
            topics: %{},
            consumer_group_coordinators: %{}

  @type t :: %__MODULE__{}

  alias KafkaEx.New.Broker
  alias KafkaEx.New.Topic

  @type node_selector ::
          :controller
          | :random
          | {:topic_partition, binary, integer}
          | {:consumer_group, binary}
  @type node_select_error :: :no_such_node | :no_such_topic | :no_such_partition

  def known_topics(%__MODULE__{topics: topics}), do: Map.keys(topics)

  @spec select_node(t, node_selector) ::
          {:ok, Kayrock.node_id()} | {:error, node_select_error}
  def select_node(
        %__MODULE__{controller_id: controller_id} = cluster_metadata,
        :controller
      ) do
    select_node(cluster_metadata, controller_id)
  end

  def select_node(%__MODULE__{brokers: brokers}, :random) do
    [node_id] = Enum.take_random(Map.keys(brokers), 1)
    {:ok, node_id}
  end

  def select_node(
        %__MODULE__{} = cluster_metadata,
        {:topic_partition, topic, partition}
      ) do
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

  def select_node(
        %__MODULE__{} = cluster_metadata,
        {:consumer_group, consumer_group}
      ) do
    case Map.fetch(cluster_metadata.consumer_group_coordinators, consumer_group) do
      :error ->
        {:error, :no_such_consumer_group}

      {:ok, coordinator_node_id} ->
        {:ok, coordinator_node_id}
    end
  end

  def select_node(%__MODULE__{} = cluster_metadata, node_id)
      when is_integer(node_id) do
    case Map.fetch(cluster_metadata.brokers, node_id) do
      {:ok, _broker} -> {:ok, node_id}
      :error -> {:error, :no_such_node}
    end
  end

  def from_metadata_v1_response(metadata) do
    brokers =
      metadata.brokers
      |> Enum.into(%{}, fn broker_metadata ->
        %{host: host, port: port, node_id: node_id, rack: rack} =
          broker_metadata

        {node_id, %Broker{host: host, port: port, node_id: node_id, rack: rack}}
      end)

    topics =
      metadata.topic_metadata
      |> Enum.filter(fn topic_metadata -> topic_metadata.error_code == 0 end)
      |> Enum.into(%{}, fn topic_metadata ->
        case topic_metadata do
          %{topic: topic_name, error_code: 0} ->
            {topic_name, Topic.from_topic_metadata(topic_metadata)}

          _ ->
            nil
        end
      end)

    %__MODULE__{
      brokers: brokers,
      controller_id: metadata.controller_id,
      topics: topics
    }
  end

  require Logger

  def merge_brokers(
        %__MODULE__{} = old_cluster_metadata,
        %__MODULE__{} = new_cluster_metadata
      ) do
    old_brokers = Map.values(old_cluster_metadata.brokers)

    Logger.debug(
      "MERGE #{inspect(old_brokers)} #{inspect(new_cluster_metadata)}"
    )

    new_brokers =
      Enum.into(new_cluster_metadata.brokers, %{}, fn {node_id, new_broker} ->
        case Enum.find(old_brokers, fn b ->
               b.host == new_broker.host && b.port == new_broker.port
             end) do
          %Broker{socket: socket} when not is_nil(socket) ->
            {node_id, %{new_broker | socket: socket}}

          _ ->
            {node_id, new_broker}
        end
      end)

    brokers_to_close =
      old_brokers
      |> Enum.filter(fn b ->
        !Enum.any?(new_brokers, fn {_, nb} ->
          nb.host == b.host && nb.port == b.port
        end)
      end)

    Logger.debug(
      "MERGED BROKERS #{inspect(new_brokers)}, " <>
        "TO CLOSE: #{inspect(brokers_to_close)}"
    )

    {%{new_cluster_metadata | brokers: new_brokers}, brokers_to_close}
  end

  def broker_by_node_id(%__MODULE__{brokers: brokers}, node_id) do
    Map.get(brokers, node_id)
  end

  def get_and_update_broker(
        %__MODULE__{brokers: brokers} = cluster_metadata,
        node_id,
        cb
      )
      when is_function(cb, 1) do
    broker = broker_by_node_id(cluster_metadata, node_id)
    {val, updated_broker} = cb.(broker)

    {val,
     %{cluster_metadata | brokers: Map.put(brokers, node_id, updated_broker)}}
  end

  def update_brokers(%__MODULE__{brokers: brokers} = cluster_metadata, cb)
      when is_function(cb, 1) do
    updated_brokers =
      Enum.into(brokers, %{}, fn {node_id, broker} ->
        {node_id, cb.(broker)}
      end)

    %{cluster_metadata | brokers: updated_brokers}
  end

  def put_consumer_group_coordinator(
        %__MODULE__{consumer_group_coordinators: consumer_group_coordinators} =
          cluster_metadata,
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

  def remove_topics(
        %__MODULE__{topics: topics} = cluster_metadata,
        topics_to_remove
      ) do
    %{cluster_metadata | topics: Map.drop(topics, topics_to_remove)}
  end
end
