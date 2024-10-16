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

  alias KafkaEx.New.Structs.ClusterMetadata
  alias KafkaEx.New.Structs.ConsumerGroup
  alias KafkaEx.New.Structs.Topic
  alias KafkaEx.New.Structs.Offset

  @type node_id :: non_neg_integer

  @type topic_name :: KafkaEx.Types.topic()
  @type partition_id :: KafkaEx.Types.partition()
  @type consumer_group_name :: KafkaEx.Types.consumer_group_name()
  @type offset_val :: KafkaEx.Types.offset()
  @type timestamp_request :: KafkaEx.Types.timestamp_request()

  @type error_atom :: atom
  @type client :: GenServer.server()
  @type correlation_id :: non_neg_integer
  @type opts :: Keyword.t()
  @type partition_offset_request :: %{partition_num: partition_id, timestamp: timestamp_request}

  @doc """
  Fetch the latest offset for a given partition
  """
  @spec latest_offset(client, topic_name, partition_id) :: {:error, error_atom} | {:ok, offset_val}
  @spec latest_offset(client, topic_name, partition_id, opts) :: {:error, error_atom} | {:ok, offset_val}
  def latest_offset(client, topic, partition_id, opts \\ []) do
    opts = Keyword.merge([api_version: 1], opts)
    partition = %{partition_num: partition_id, timestamp: :latest}

    case list_offsets(client, [{topic, [partition]}], opts) do
      {:ok, [%{partition_offsets: [offset]}]} -> {:ok, offset.offset}
      result -> result
    end
  end

  @doc """
  Fetch the latest offset for a given partition
  """
  @spec earliest_offset(client, topic_name, partition_id) :: {:error, error_atom} | {:ok, offset_val}
  @spec earliest_offset(client, topic_name, partition_id, opts) :: {:error, error_atom} | {:ok, offset_val}
  def earliest_offset(client, topic, partition_id, opts \\ []) do
    opts = Keyword.merge([api_version: 1], opts)
    partition = %{partition_num: partition_id, timestamp: :earliest}

    case list_offsets(client, [{topic, [partition]}], opts) do
      {:ok, [%{partition_offsets: [offset]}]} -> {:ok, offset.offset}
      result -> result
    end
  end

  @doc """
  Sends a request to describe a group identified by its name.
  We support only one consumer group per request for now, as we don't
  group requests by group coordinator.
  """
  @spec describe_group(client, consumer_group_name) :: {:ok, ConsumerGroup.t()} | {:error, any}
  @spec describe_group(client, consumer_group_name, opts) :: {:ok, ConsumerGroup.t()} | {:error, any}
  def describe_group(client, consumer_group_name, opts \\ []) do
    case GenServer.call(client, {:describe_groups, [consumer_group_name], opts}) do
      {:ok, [group]} -> {:ok, group}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Returns list of Offsets per topic per partition.
  We support only one topic partition pair for now, as we don't request by leader.
  """
  @spec list_offsets(client, [{topic_name, [partition_offset_request]}]) :: {:ok, list(Offset.t())} | {:error, any}
  @spec list_offsets(client, [{topic_name, [partition_offset_request]}], opts) ::
          {:ok, list(Offset.t())} | {:error, any}
  def list_offsets(client, [{topic, [partition_request]}], opts \\ []) do
    case GenServer.call(client, {:list_offsets, [{topic, [partition_request]}], opts}) do
      {:ok, offsets} -> {:ok, offsets}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
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
