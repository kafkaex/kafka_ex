defmodule KafkaEx.New.Protocols.Kayrock.Metadata.ResponseHelpers do
  @moduledoc """
  Helper functions for parsing Metadata responses across different protocol versions.
  """

  alias KafkaEx.New.Kafka.Broker
  alias KafkaEx.New.Kafka.ClusterMetadata
  alias KafkaEx.New.Kafka.PartitionInfo
  alias KafkaEx.New.Kafka.Topic
  alias Kayrock.ErrorCode

  @doc """
  Converts a Kayrock Metadata response to ClusterMetadata.
  """
  @spec to_cluster_metadata(map(), Keyword.t()) :: {:ok, ClusterMetadata.t()} | {:error, term}
  def to_cluster_metadata(response, _opts \\ []) do
    brokers = parse_brokers(response.brokers)
    topics = parse_topics(response.topic_metadata)
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
    |> Enum.filter(fn topic_map -> topic_map.error_code == 0 end)
    |> Enum.into(%{}, fn topic_map ->
      topic_name = topic_map.topic
      is_internal = Map.get(topic_map, :is_internal, false)
      partitions = parse_partitions(topic_map.partition_metadata)

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
    |> Enum.filter(fn partition_map -> partition_map.error_code == 0 end)
    |> Enum.map(fn partition_map ->
      %PartitionInfo{
        partition_id: partition_map.partition,
        leader: partition_map.leader,
        replicas: partition_map.replicas || [],
        isr: partition_map.isr || []
      }
    end)
  end

  @doc """
  Checks if the metadata response contains any errors.
  """
  @spec check_for_errors(map()) :: :ok | {:error, term}
  def check_for_errors(response) do
    # Check for topic-level errors
    topic_errors =
      response.topic_metadata
      |> Enum.reject(fn topic -> topic.error_code == 0 end)
      |> Enum.map(fn topic ->
        {topic.topic, ErrorCode.code_to_atom(topic.error_code)}
      end)

    case topic_errors do
      [] -> :ok
      errors -> {:error, {:topic_errors, errors}}
    end
  end
end
