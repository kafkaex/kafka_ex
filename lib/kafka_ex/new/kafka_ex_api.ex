defmodule KafkaEx.New.KafkaExAPI do
  @moduledoc """
  This module interfaces Kafka through the ServerKayrock implementation

  This is intended to become the future KafkaEx API
  """

  alias KafkaEx.ServerKayrock
  alias KafkaEx.New.ClusterMetadata
  alias KafkaEx.New.Topic

  @type node_id :: non_neg_integer
  @type topic_name :: binary
  @type partition_id :: non_neg_integer
  @type consumer_group_name :: binary
  @type offset :: non_neg_integer
  @type error_atom :: atom
  @type client :: GenServer.server()
  @type correlation_id :: non_neg_integer

  @doc """
  Fetch the latest offset for a given partition
  """
  @spec latest_offset(client, topic_name, partition_id) ::
          {:error, error_atom} | {:ok, offset}
  def latest_offset(client, topic, partition) do
    request = %Kayrock.ListOffsets.V1.Request{
      replica_id: -1,
      topics: [
        %{topic: topic, partitions: [%{partition: partition, timestamp: -1}]}
      ]
    }

    {:ok, resp} =
      ServerKayrock.kayrock_call(
        client,
        request,
        {:topic_partition, topic, partition}
      )

    [topic_resp] = resp.responses
    [%{error_code: error_code, offset: offset}] = topic_resp.partition_responses

    case error_code do
      0 -> {:ok, offset}
      _ -> {:error, Kayrock.ErrorCode.code_to_atom(error_code)}
    end
  end

  @doc """
  Get topic metadata for the given topics

  Fetches metadata from the server if necessary.  Set allow_topic_creation to
  true to allow the topics to be created if they don't exist
  """
  @spec topics_metadata(client, [topic_name], boolean) :: {:ok, [Topic.t()]}
  def topics_metadata(client, topics, allow_topic_creation \\ false) do
    GenServer.call(client, {:topic_metadata, topics, allow_topic_creation})
  end

  @doc """
  Returns the cluster metadata from the given client
  """
  @spec cluster_metadata(client) :: {:ok, ClusterMetadata.t()}
  def(cluster_metadata(client)) do
    GenServer.call(client, :cluster_metadata)
  end

  @doc """
  Returns the current correlation id for the given client
  """
  @spec correlation_id(client) :: {:ok, correlation_id}
  def correlation_id(client) do
    GenServer.call(client, :correlation_id)
  end

  @doc """
  Set the consumer group name that will be used by the given client for autocommit
  """
  @spec set_consumer_group_for_auto_commit(client, consumer_group_name) ::
          :ok | {:error, :invalid_consumer_group}
  def set_consumer_group_for_auto_commit(client, consumer_group) do
    GenServer.call(
      client,
      {:set_consumer_group_for_auto_commit, consumer_group}
    )
  end
end
