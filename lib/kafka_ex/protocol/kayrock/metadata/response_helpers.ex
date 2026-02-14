defmodule KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers do
  @moduledoc """
  Helper functions for parsing Metadata responses across different protocol versions.
  """

  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.PartitionInfo
  alias KafkaEx.Cluster.Topic
  alias Kayrock.ErrorCode

  @doc """
  Converts a Kayrock Metadata response to ClusterMetadata.
  """
  @spec to_cluster_metadata(map(), Keyword.t()) :: {:ok, ClusterMetadata.t()} | {:error, term}
  def to_cluster_metadata(response, _opts \\ []) do
    brokers = parse_brokers(response.brokers)
    topics = parse_topics(response.topics)
    controller_id = Map.get(response, :controller_id)

    cluster_metadata = %ClusterMetadata{
      brokers: brokers,
      controller_id: controller_id,
      topics: topics,
      consumer_group_coordinators: %{}
    }

    {:ok, cluster_metadata}
  rescue
    e -> {:error, {:parse_error, e}}
  end

  @doc """
  Parses broker list from Kayrock response.
  """
  @spec parse_brokers([map()]) :: %{non_neg_integer() => Broker.t()}
  def parse_brokers(kayrock_brokers) when is_list(kayrock_brokers) do
    Enum.into(kayrock_brokers, %{}, fn broker_map ->
      node_id = broker_map.node_id
      rack = Map.get(broker_map, :rack)

      broker = %Broker{
        node_id: node_id,
        host: broker_map.host,
        port: broker_map.port,
        socket: nil,
        rack: rack
      }

      {node_id, broker}
    end)
  end

  @doc """
  Parses topic metadata list from Kayrock response.
  """
  @spec parse_topics([map()]) :: %{String.t() => Topic.t()}
  def parse_topics(kayrock_topics) when is_list(kayrock_topics) do
    kayrock_topics
    |> Enum.filter(&(ErrorCode.code_to_atom(&1.error_code) == :no_error))
    |> Enum.into(%{}, fn topic_map ->
      topic_name = topic_map.name
      is_internal = Map.get(topic_map, :is_internal, false)
      partitions = parse_partitions(topic_map.partitions)

      partition_leaders =
        Enum.into(partitions, %{}, fn partition ->
          {partition.partition_id, partition.leader}
        end)

      topic = %Topic{
        name: topic_name,
        partition_leaders: partition_leaders,
        is_internal: is_internal,
        partitions: partitions
      }

      {topic_name, topic}
    end)
  end

  @doc """
  Parses partition metadata list from Kayrock response.
  """
  @spec parse_partitions([map()]) :: [PartitionInfo.t()]
  def parse_partitions(kayrock_partitions) when is_list(kayrock_partitions) do
    kayrock_partitions
    |> Enum.filter(&(ErrorCode.code_to_atom(&1.error_code) == :no_error))
    |> Enum.map(fn partition_map ->
      %PartitionInfo{
        partition_id: partition_map.partition_index,
        leader: partition_map.leader_id,
        replicas: partition_map.replica_nodes || [],
        isr: partition_map.isr_nodes || []
      }
    end)
  end

  @doc """
  Checks if the metadata response contains any errors.
  """
  @spec check_for_errors(map()) :: :ok | {:error, term}
  def check_for_errors(response) do
    response.topics
    |> Enum.reject(&(ErrorCode.code_to_atom(&1.error_code) == :no_error))
    |> Enum.map(&{&1.name, ErrorCode.code_to_atom(&1.error_code)})
    |> case do
      [] -> :ok
      errors -> {:error, {:topic_errors, errors}}
    end
  end
end
