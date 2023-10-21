defmodule KafkaEx.New.KafkaExAPI do
  @moduledoc """
  This module interfaces Kafka through the New.Client implementation

  This is intended to become the future KafkaEx API

  Most functions here take a client pid as the first argument.

  ```
  {:ok, client} = KafkaEx.New.Client.start_link()

  KafkaEx.New.KafkaExAPI.latest_offset(client, "some_topic", 0)
  ```
  """

  alias KafkaEx.New.Client
  alias KafkaEx.New.Structs.ClusterMetadata
  alias KafkaEx.New.Structs.ConsumerGroup
  alias KafkaEx.New.Structs.Topic
  alias KafkaEx.New.Structs.NodeSelector

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
      Client.send_request(
        client,
        request,
        NodeSelector.topic_partition(topic, partition)
      )

    [topic_resp] = resp.responses
    [%{error_code: error_code, offset: offset}] = topic_resp.partition_responses

    case error_code do
      0 -> {:ok, offset}
      _ -> {:error, Kayrock.ErrorCode.code_to_atom(error_code)}
    end
  end

  @doc """
  Sends a request to describe a group identified by its name.
  We support only one consumer group per request for now, as we don't
  group requests by group coordinator.
  """
  @spec describe_group(client, Keyword.t()) ::
          {:ok, ConsumerGroup.t()} | {:error, any}
  def describe_group(client, consumer_group_name) do
    case GenServer.call(client, {:describe_groups, [consumer_group_name]}) do
      {:ok, [group]} -> {:ok, group}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Get topic metadata for the given topics

  Always calls out to the broker to get the most up-to-date metadata (and
  subsequently updates the client's state with the updated metadata). Set
  allow_topic_creation to true to allow the topics to be created if they
  don't exist
  """
  @spec topics_metadata(client, [topic_name], boolean) :: {:ok, [Topic.t()]}
  def topics_metadata(client, topics, allow_topic_creation \\ false) do
    GenServer.call(client, {:topic_metadata, topics, allow_topic_creation})
  end

  @doc """
  Returns the cluster metadata from the given client
  """
  @spec cluster_metadata(client) :: {:ok, ClusterMetadata.t()}
  def cluster_metadata(client) do
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
  Set the consumer group name that will be used by the given client for
  autocommit

  NOTE this function will not be supported after the legacy API is removed
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
