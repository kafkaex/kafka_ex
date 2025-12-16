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
  alias KafkaEx.New.Kafka.CreateTopics
  alias KafkaEx.New.Kafka.DeleteTopics
  alias KafkaEx.New.Kafka.Fetch
  alias KafkaEx.New.Kafka.FindCoordinator
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
  @spec leave_group(client, consumer_group_name, member_id, opts) ::
          {:ok, :no_error | LeaveGroup.t()} | {:error, error_atom}
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
  @spec sync_group(client, consumer_group_name, generation_id, member_id, opts) ::
          {:ok, SyncGroup.t()} | {:error, error_atom}
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

  @doc """
  Fetch records from a Kafka topic partition

  Retrieves records from the specified topic and partition starting from the given offset.
  Returns the fetched records along with metadata about the fetch (high watermark, etc).

  ## Options

    * `:max_bytes` - Maximum bytes to fetch per partition (default: 1,000,000)
    * `:max_wait_time` - Maximum time to wait for records in ms (default: 10,000)
    * `:min_bytes` - Minimum bytes to accumulate before returning (default: 1)
    * `:isolation_level` - 0 for READ_UNCOMMITTED, 1 for READ_COMMITTED (V4+, default: 0)
    * `:api_version` - API version to use (default: 3)

  ## API Version Differences

  | Version | Features |
  |---------|----------|
  | V0 | Basic fetch using MessageSet format |
  | V1 | Adds throttle_time_ms to response |
  | V3 | Adds max_bytes at request level |
  | V4 | Adds isolation_level, last_stable_offset, aborted_transactions |
  | V5+ | Adds log_start_offset |
  | V7 | Adds incremental fetch (session_id, epoch) |
  """
  @spec fetch(client, topic_name, partition_id, offset_val) :: {:ok, Fetch.t()} | {:error, error_atom}
  @spec fetch(client, topic_name, partition_id, offset_val, opts) :: {:ok, Fetch.t()} | {:error, error_atom}
  def fetch(client, topic, partition, offset, opts \\ []) do
    case GenServer.call(client, {:fetch, topic, partition, offset, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Fetch all available records from a topic partition

  Convenience function that fetches records from the earliest offset up to the high watermark.
  Useful for reading all data in a partition (within the configured max_bytes limit).
  """
  @spec fetch_all(client, topic_name, partition_id) :: {:ok, Fetch.t()} | {:error, error_atom}
  @spec fetch_all(client, topic_name, partition_id, opts) :: {:ok, Fetch.t()} | {:error, error_atom}
  def fetch_all(client, topic, partition, opts \\ []) do
    case earliest_offset(client, topic, partition) do
      {:ok, offset} -> fetch(client, topic, partition, offset, opts)
      {:error, _} = error -> error
    end
  end

  @doc """
  Find the coordinator broker for a consumer group or transaction

  Discovers which broker is the coordinator for the specified consumer group
  (or transactional producer). This is used internally for consumer group
  operations but can also be called directly.

  ## Options

    * `:coordinator_type` - 0 for group (default), 1 for transaction
    * `:api_version` - API version to use (default: 1)

  ## API Version Differences

  | Version | Features |
  |---------|----------|
  | V0 | Basic group coordinator discovery (uses group_id) |
  | V1 | Adds coordinator_type (group/transaction), throttle_time_ms, error_message |

  ## Examples

      # Find group coordinator
      {:ok, coordinator} = KafkaExAPI.find_coordinator(client, "my-consumer-group")

      # Find transaction coordinator
      {:ok, coordinator} = KafkaExAPI.find_coordinator(client, "my-transactional-id",
        coordinator_type: :transaction)
  """
  @spec find_coordinator(client, consumer_group_name) ::
          {:ok, FindCoordinator.t()} | {:error, error_atom}
  @spec find_coordinator(client, consumer_group_name, opts) ::
          {:ok, FindCoordinator.t()} | {:error, error_atom}
  def find_coordinator(client, group_id, opts \\ []) do
    case GenServer.call(client, {:find_coordinator, group_id, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Creates one or more topics in the Kafka cluster.

  ## Parameters

    * `client` - The client pid
    * `topics` - List of topic configurations, each being a keyword list or map with:
      * `:topic` - Topic name (required)
      * `:num_partitions` - Number of partitions (default: -1 for broker default)
      * `:replication_factor` - Replication factor (default: -1 for broker default)
      * `:replica_assignment` - Manual replica assignment (default: [])
      * `:config_entries` - Topic configuration entries as `{name, value}` tuples (default: [])
    * `timeout` - Request timeout in milliseconds (required)
    * `opts` - Optional keyword list with:
      * `:validate_only` - If true, only validate without creating topics (V1+, default: false)
      * `:api_version` - API version to use (default: 1)

  ## API Version Differences

  | Version | Features |
  |---------|----------|
  | V0 | Basic topic creation |
  | V1 | Adds validate_only flag, error_message in response |
  | V2 | Adds throttle_time_ms in response |

  ## Examples

      # Create a topic with broker defaults
      {:ok, result} = KafkaExAPI.create_topics(client, [
        [topic: "my-topic"]
      ], 10_000)

      # Create a topic with custom configuration
      {:ok, result} = KafkaExAPI.create_topics(client, [
        [
          topic: "my-topic",
          num_partitions: 3,
          replication_factor: 2,
          config_entries: [
            {"cleanup.policy", "compact"},
            {"retention.ms", "86400000"}
          ]
        ]
      ], 10_000)

      # Validate only (V1+)
      {:ok, result} = KafkaExAPI.create_topics(client, [
        [topic: "my-topic", num_partitions: 3]
      ], 10_000, validate_only: true)

      # Check results
      if CreateTopics.success?(result) do
        IO.puts("All topics created successfully")
      else
        for failed <- CreateTopics.failed_topics(result) do
          IO.puts("Failed to create \#{failed.topic}: \#{failed.error}")
        end
      end
  """
  @spec create_topics(client, list(), non_neg_integer()) ::
          {:ok, CreateTopics.t()} | {:error, error_atom}
  @spec create_topics(client, list(), non_neg_integer(), opts) ::
          {:ok, CreateTopics.t()} | {:error, error_atom}
  def create_topics(client, topics, timeout, opts \\ []) do
    case GenServer.call(client, {:create_topics, topics, timeout, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Creates a single topic with the given configuration.

  This is a convenience function that wraps `create_topics/4` for creating a single topic.

  ## Parameters

    * `client` - The client pid
    * `topic_name` - The name of the topic to create
    * `opts` - Keyword list with:
      * `:num_partitions` - Number of partitions (default: -1 for broker default)
      * `:replication_factor` - Replication factor (default: -1 for broker default)
      * `:config_entries` - Topic configuration entries (default: [])
      * `:timeout` - Request timeout in milliseconds (default: 10_000)
      * `:validate_only` - If true, only validate (V1+, default: false)
      * `:api_version` - API version to use (default: 1)

  ## Examples

      # Create with defaults
      {:ok, result} = KafkaExAPI.create_topic(client, "my-topic")

      # Create with specific configuration
      {:ok, result} = KafkaExAPI.create_topic(client, "my-topic",
        num_partitions: 6,
        replication_factor: 3,
        timeout: 30_000
      )
  """
  @spec create_topic(client, topic_name) :: {:ok, CreateTopics.t()} | {:error, error_atom}
  @spec create_topic(client, topic_name, opts) :: {:ok, CreateTopics.t()} | {:error, error_atom}
  def create_topic(client, topic_name, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, 10_000)
    {api_opts, topic_opts} = Keyword.split(opts, [:validate_only, :api_version])

    topic_config = Keyword.merge([topic: topic_name], topic_opts)
    create_topics(client, [topic_config], timeout, api_opts)
  end

  # ---------------------------------------------------------------------------
  # DeleteTopics API
  # ---------------------------------------------------------------------------

  @doc """
  Deletes topics from the Kafka cluster.

  ## Parameters

    * `client` - The client pid
    * `topics` - List of topic names to delete
    * `timeout` - Request timeout in milliseconds
    * `opts` - Keyword list with:
      * `:api_version` - API version to use (default: 1)

  ## Returns

    * `{:ok, DeleteTopics.t()}` - Result with status for each topic
    * `{:error, error_atom}` - Error if the request failed

  ## Examples

      # Delete a single topic
      {:ok, result} = KafkaExAPI.delete_topics(client, ["my-topic"], 30_000)

      # Delete multiple topics
      {:ok, result} = KafkaExAPI.delete_topics(client, ["topic1", "topic2", "topic3"], 30_000)

      # Check results
      if DeleteTopics.success?(result) do
        IO.puts("All topics deleted successfully")
      else
        failed = DeleteTopics.failed_topics(result)
        IO.inspect(failed, label: "Failed to delete")
      end

  ## API Version Differences

  | Version | Features |
  |---------|----------|
  | V0      | Basic topic deletion |
  | V1      | Adds throttle_time_ms in response |
  """
  @spec delete_topics(client, [topic_name], timeout :: non_neg_integer) ::
          {:ok, DeleteTopics.t()} | {:error, error_atom}
  @spec delete_topics(client, [topic_name], timeout :: non_neg_integer, opts) ::
          {:ok, DeleteTopics.t()} | {:error, error_atom}
  def delete_topics(client, topics, timeout, opts \\ []) do
    case GenServer.call(client, {:delete_topics, topics, timeout, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Deletes a single topic from the Kafka cluster.

  This is a convenience function that wraps `delete_topics/4` for deleting a single topic.

  ## Parameters

    * `client` - The client pid
    * `topic_name` - The name of the topic to delete
    * `opts` - Keyword list with:
      * `:timeout` - Request timeout in milliseconds (default: 30_000)
      * `:api_version` - API version to use (default: 1)

  ## Examples

      # Delete with defaults
      {:ok, result} = KafkaExAPI.delete_topic(client, "my-topic")

      # Delete with custom timeout
      {:ok, result} = KafkaExAPI.delete_topic(client, "my-topic", timeout: 60_000)
  """
  @spec delete_topic(client, topic_name) :: {:ok, DeleteTopics.t()} | {:error, error_atom}
  @spec delete_topic(client, topic_name, opts) :: {:ok, DeleteTopics.t()} | {:error, error_atom}
  def delete_topic(client, topic_name, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, 30_000)
    delete_topics(client, [topic_name], timeout, opts)
  end

  # Helper to conditionally add keys to a map only if value is not nil
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
