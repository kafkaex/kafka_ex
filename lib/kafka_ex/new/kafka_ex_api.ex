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

  alias KafkaEx.New.Kafka.ApiVersions
  alias KafkaEx.New.Kafka.ClusterMetadata
  alias KafkaEx.New.Kafka.ConsumerGroupDescription
  alias KafkaEx.New.Kafka.Heartbeat
  alias KafkaEx.New.Kafka.JoinGroup
  alias KafkaEx.New.Kafka.LeaveGroup
  alias KafkaEx.New.Kafka.Offset
  alias KafkaEx.New.Kafka.RecordMetadata
  alias KafkaEx.New.Kafka.SyncGroup
  alias KafkaEx.New.Kafka.Topic

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
  @type partition_id_request :: %{partition_num: partition_id}
  @type partition_offset_commit_request :: %{partition_num: partition_id, offset: offset_val}
  @type member_id :: binary
  @type generation_id :: non_neg_integer

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
  @spec describe_group(client, consumer_group_name) :: {:ok, ConsumerGroupDescription.t()} | {:error, any}
  @spec describe_group(client, consumer_group_name, opts) :: {:ok, ConsumerGroupDescription.t()} | {:error, any}
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

  This returns the cluster metadata currently cached in the client's state.
  It does not make a network request to Kafka. If you need fresh metadata,
  use `metadata/2` or `metadata/3` instead.
  """
  @spec cluster_metadata(client) :: {:ok, ClusterMetadata.t()}
  def cluster_metadata(client) do
    GenServer.call(client, :cluster_metadata)
  end

  @doc """
  Fetch metadata from Kafka brokers

  Makes a network request to fetch fresh metadata from Kafka. This updates
  the client's internal cluster metadata state and returns the updated metadata.

  By default, fetches metadata for all topics in the cluster. To fetch metadata
  for specific topics only, use `metadata/3`.

  ## API Version Differences

    - **V0**: Basic metadata (topics, partitions, brokers)
    - **V1**: Adds `controller_id`, `is_internal` flag, broker `rack`
    - **V2**: Adds `cluster_id` field
  """
  @spec metadata(client) :: {:ok, ClusterMetadata.t()} | {:error, error_atom}
  @spec metadata(client, opts) :: {:ok, ClusterMetadata.t()} | {:error, error_atom}
  def metadata(client, opts \\ []) do
    api_version = Keyword.get(opts, :api_version, 1)

    case GenServer.call(client, {:metadata, nil, opts, api_version}) do
      {:ok, cluster_metadata} -> {:ok, cluster_metadata}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Fetch metadata for specific topics

  Makes a network request to fetch fresh metadata for the specified topics only.
  This is more efficient than fetching all topics if you only need information
  about a subset of topics.
  """
  @spec metadata(client, [topic_name] | nil, opts) :: {:ok, ClusterMetadata.t()} | {:error, error_atom}
  def metadata(client, topics, opts) when is_list(topics) or is_nil(topics) do
    api_version = Keyword.get(opts, :api_version, 1)
    request_opts = Keyword.put(opts, :topics, topics)

    case GenServer.call(client, {:metadata, topics, request_opts, api_version}) do
      {:ok, cluster_metadata} -> {:ok, cluster_metadata}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Returns the current correlation id for the given client
  """
  @spec correlation_id(client) :: {:ok, correlation_id}
  def correlation_id(client) do
    GenServer.call(client, :correlation_id)
  end

  @doc """
  Fetch API versions supported by the Kafka broker

  Queries the broker to discover which Kafka API versions it supports.
  This enables the client to negotiate compatible API versions for all operations.

  The response includes the minimum and maximum supported versions for each Kafka API,
  identified by their API key.

  ## API Version Differences

    - **V0**: Basic API version discovery
    - **V1**: Adds `throttle_time_ms` to response for rate limiting visibility
  """
  @spec api_versions(client) :: {:ok, ApiVersions.t()} | {:error, error_atom}
  @spec api_versions(client, opts) :: {:ok, ApiVersions.t()} | {:error, error_atom}
  def api_versions(client, opts \\ []) do
    case GenServer.call(client, {:api_versions, opts}) do
      {:ok, versions} -> {:ok, versions}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Set the consumer group name that will be used by the given client for
  autocommit

  NOTE this function will not be supported after the legacy API is removed
  """
  @spec set_consumer_group_for_auto_commit(client, consumer_group_name) :: :ok | {:error, :invalid_consumer_group}
  def set_consumer_group_for_auto_commit(client, consumer_group) do
    GenServer.call(client, {:set_consumer_group_for_auto_commit, consumer_group})
  end

  @doc """
  Fetch committed offsets for a consumer group

  Retrieves the last committed offsets for the specified topic/partition(s)
  for a consumer group. This is useful for tracking consumer group progress
  and implementing manual offset management.
  """
  @spec fetch_committed_offset(client, consumer_group_name, topic_name, [partition_id_request]) ::
          {:ok, [Offset.t()]} | {:error, error_atom}
  @spec fetch_committed_offset(client, consumer_group_name, topic_name, [partition_id_request], opts) ::
          {:ok, [Offset.t()]} | {:error, error_atom}
  def fetch_committed_offset(client, consumer_group, topic, partitions, opts \\ []) do
    case GenServer.call(client, {:offset_fetch, consumer_group, [{topic, partitions}], opts}) do
      {:ok, offsets} -> {:ok, offsets}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Commit offsets for a consumer group

  Stores the committed offsets for the specified topic/partition(s) for a
  consumer group. This is used for manual offset management and consumer
  group coordination.
  """
  @spec commit_offset(client, consumer_group_name, topic_name, [partition_offset_commit_request]) ::
          {:ok, [Offset.t()]} | {:error, error_atom}
  @spec commit_offset(client, consumer_group_name, topic_name, [partition_offset_commit_request], opts) ::
          {:ok, [Offset.t()]} | {:error, error_atom}
  def commit_offset(client, consumer_group, topic, partitions, opts \\ []) do
    case GenServer.call(client, {:offset_commit, consumer_group, [{topic, partitions}], opts}) do
      {:ok, offsets} -> {:ok, offsets}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Send a heartbeat to a consumer group coordinator

  Sends a periodic heartbeat to the group coordinator to indicate that the
  consumer is still active and participating in the consumer group. This is
  required to maintain group membership and prevent rebalancing.
  """
  @spec heartbeat(client, consumer_group_name, member_id, generation_id) ::
          {:ok, :no_error | Heartbeat.t()} | {:error, error_atom}
  @spec heartbeat(client, consumer_group_name, member_id, generation_id, opts) ::
          {:ok, :no_error | Heartbeat.t()} | {:error, error_atom}
  def heartbeat(client, consumer_group, member_id, generation_id, opts \\ []) do
    case GenServer.call(client, {:heartbeat, consumer_group, member_id, generation_id, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Join a consumer group

  Initiates the consumer group join protocol. This is the first step in the
  consumer group coordination protocol. The consumer sends its metadata (topics
  it's interested in) and receives back group membership information including
  the generation ID and member ID.

  If this member is elected as the group leader, it will receive the metadata
  of all members and is responsible for computing partition assignments (via
  `sync_group/5`).

  ## API Version Differences
    - **V0**: Basic JoinGroup with session_timeout
    - **V1**: Adds separate rebalance_timeout field
    - **V2**: Adds throttle_time_ms to response
  """
  @spec join_group(client, consumer_group_name, member_id) :: {:ok, JoinGroup.t()} | {:error, error_atom}
  @spec join_group(client, consumer_group_name, member_id, opts) :: {:ok, JoinGroup.t()} | {:error, error_atom}
  def join_group(client, consumer_group, member_id, opts \\ []) do
    case GenServer.call(client, {:join_group, consumer_group, member_id, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Leave a consumer group

  Notifies the group coordinator that a consumer is voluntarily leaving the
  consumer group. This allows the coordinator to immediately trigger a rebalance
  without waiting for a session timeout, improving rebalance latency.
  """
  @spec leave_group(client, consumer_group_name, member_id) :: {:ok, :no_error | LeaveGroup.t()} | {:error, error_atom}
  @spec leave_group(client, consumer_group_name, member_id, opts) :: {:ok, :no_error | LeaveGroup.t()} | {:error, error_atom}
  def leave_group(client, consumer_group, member_id, opts \\ []) do
    case GenServer.call(client, {:leave_group, consumer_group, member_id, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Synchronize consumer group state

  Completes the consumer group rebalance protocol by synchronizing state
  between the group leader and followers. The leader provides partition
  assignments which are distributed to all members. Followers receive their
  assigned partitions from this call.
  """
  @spec sync_group(client, consumer_group_name, generation_id, member_id) :: {:ok, SyncGroup.t()} | {:error, error_atom}
  @spec sync_group(client, consumer_group_name, generation_id, member_id, opts) :: {:ok, SyncGroup.t()} | {:error, error_atom}
  def sync_group(client, consumer_group, generation_id, member_id, opts \\ []) do
    case GenServer.call(client, {:sync_group, consumer_group, generation_id, member_id, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Produce messages to a Kafka topic partition

  Sends one or more messages to the specified topic and partition. Returns the
  base offset assigned to the first message in the batch.

  ## API Version Differences

  | Version | Features |
  |---------|----------|
  | V0-V1 | Basic produce using MessageSet format |
  | V2 | Adds timestamp support, still MessageSet format |
  | V3+ | RecordBatch format with headers, transactional support |
  | V5 | Adds log_start_offset to response |
  """
  @spec produce(client, topic_name, partition_id, [map()]) :: {:ok, RecordMetadata.t()} | {:error, error_atom}
  @spec produce(client, topic_name, partition_id, [map()], opts) :: {:ok, RecordMetadata.t()} | {:error, error_atom}
  def produce(client, topic, partition, messages, opts \\ []) do
    case GenServer.call(client, {:produce, topic, partition, messages, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Produce a single message to a Kafka topic partition
  Convenience function for producing a single message. Wraps the message in a list and calls `produce/5`.
  """
  @spec produce_one(client, topic_name, partition_id, binary) :: {:ok, RecordMetadata.t()} | {:error, error_atom}
  @spec produce_one(client, topic_name, partition_id, binary, opts) :: {:ok, RecordMetadata.t()} | {:error, error_atom}
  def produce_one(client, topic, partition, value, opts \\ []) do
    {message_opts, produce_opts} = Keyword.split(opts, [:key, :timestamp, :headers])

    message =
      %{value: value}
      |> maybe_put(:key, Keyword.get(message_opts, :key))
      |> maybe_put(:timestamp, Keyword.get(message_opts, :timestamp))
      |> maybe_put(:headers, Keyword.get(message_opts, :headers))

    produce(client, topic, partition, [message], produce_opts)
  end

  # Helper to conditionally add keys to a map only if value is not nil
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
