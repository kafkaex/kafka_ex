defmodule KafkaEx.API do
  @moduledoc """
  The primary Kafka client API for KafkaEx v1.0+

  This module provides two ways to interact with Kafka:

  ## Direct API Usage

  All functions take a client pid as the first argument:

      {:ok, client} = KafkaEx.API.start_client()

      # Produce a message
      {:ok, metadata} = KafkaEx.API.produce(client, "my-topic", 0, [%{value: "hello"}])

      # Fetch messages
      {:ok, fetch_result} = KafkaEx.API.fetch(client, "my-topic", 0, 0)

  ## Using via `use KafkaEx.API`

  You can also `use` this module in your own module to get API functions
  that automatically use a configured client:

      defmodule MyApp.Kafka do
        use KafkaEx.API

        # Override client/0 to return your client pid
        def client do
          MyApp.KafkaClient  # a named client
        end
      end

      # Now call without passing client:
      {:ok, metadata} = MyApp.Kafka.produce("my-topic", 0, [%{value: "hello"}])

  ### Options for `use`

    * `:client` - A module, atom, or function that returns the client pid.
      If not provided, you must implement `client/0` in your module.

  Example with static client name:

      defmodule MyApp.Kafka do
        use KafkaEx.API, client: MyApp.KafkaClient
      end

  Example with dynamic client lookup:

      defmodule MyApp.Kafka do
        use KafkaEx.API

        def client do
          Process.whereis(:my_kafka_client) || raise "Client not started"
        end
      end
  """

  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.DeleteTopics
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Messages.Heartbeat
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Messages.LeaveGroup
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.Messages.SyncGroup
  alias KafkaEx.Cluster.Topic

  # Types
  @type node_id :: non_neg_integer
  @type topic_name :: KafkaEx.Support.Types.topic()
  @type partition_id :: KafkaEx.Support.Types.partition()
  @type consumer_group_name :: KafkaEx.Support.Types.consumer_group_name()
  @type offset_val :: KafkaEx.Support.Types.offset()
  @type timestamp_request :: KafkaEx.Support.Types.timestamp_request()
  @type error_atom :: atom
  @type client :: GenServer.server()
  @type correlation_id :: non_neg_integer
  @type opts :: Keyword.t()
  @type partition_offset_request :: %{partition_num: partition_id, timestamp: timestamp_request}
  @type partition_id_request :: %{partition_num: partition_id}
  @type partition_offset_commit_request :: %{partition_num: partition_id, offset: offset_val}
  @type member_id :: binary
  @type generation_id :: non_neg_integer

  # ---------------------------------------------------------------------------
  # __using__ macro for mixin pattern
  # ---------------------------------------------------------------------------

  @doc false
  defmacro __using__(opts) do
    client_config = Keyword.get(opts, :client)

    quote do
      @behaviour KafkaEx.API.Behaviour

      # Client accessor - can be overridden
      if unquote(client_config) do
        def client, do: unquote(client_config)
      else
        def client do
          raise "You must implement client/0 or pass :client option to use KafkaEx.API"
        end
      end

      defoverridable client: 0

      # Offset functions
      def latest_offset(topic, partition, opts \\ []) do
        KafkaEx.API.latest_offset(client(), topic, partition, opts)
      end

      def earliest_offset(topic, partition, opts \\ []) do
        KafkaEx.API.earliest_offset(client(), topic, partition, opts)
      end

      def list_offsets(topic_partitions, opts \\ []) do
        KafkaEx.API.list_offsets(client(), topic_partitions, opts)
      end

      # Metadata functions
      def metadata(opts \\ []) do
        KafkaEx.API.metadata(client(), opts)
      end

      def cluster_metadata do
        KafkaEx.API.cluster_metadata(client())
      end

      def topics_metadata(topics, allow_creation \\ false) do
        KafkaEx.API.topics_metadata(client(), topics, allow_creation)
      end

      def api_versions(opts \\ []) do
        KafkaEx.API.api_versions(client(), opts)
      end

      # Produce functions
      def produce(topic, partition, messages, opts \\ []) do
        KafkaEx.API.produce(client(), topic, partition, messages, opts)
      end

      def produce_one(topic, partition, value, opts \\ []) do
        KafkaEx.API.produce_one(client(), topic, partition, value, opts)
      end

      # Fetch functions
      def fetch(topic, partition, offset, opts \\ []) do
        KafkaEx.API.fetch(client(), topic, partition, offset, opts)
      end

      def fetch_all(topic, partition, opts \\ []) do
        KafkaEx.API.fetch_all(client(), topic, partition, opts)
      end

      # Consumer group functions
      def describe_group(group, opts \\ []) do
        KafkaEx.API.describe_group(client(), group, opts)
      end

      def join_group(group, member_id, opts \\ []) do
        KafkaEx.API.join_group(client(), group, member_id, opts)
      end

      def sync_group(group, generation_id, member_id, opts \\ []) do
        KafkaEx.API.sync_group(client(), group, generation_id, member_id, opts)
      end

      def leave_group(group, member_id, opts \\ []) do
        KafkaEx.API.leave_group(client(), group, member_id, opts)
      end

      def heartbeat(group, member_id, generation_id, opts \\ []) do
        KafkaEx.API.heartbeat(client(), group, member_id, generation_id, opts)
      end

      def find_coordinator(group_id, opts \\ []) do
        KafkaEx.API.find_coordinator(client(), group_id, opts)
      end

      # Offset management functions
      def fetch_committed_offset(group, topic, partitions, opts \\ []) do
        KafkaEx.API.fetch_committed_offset(client(), group, topic, partitions, opts)
      end

      def commit_offset(group, topic, partitions, opts \\ []) do
        KafkaEx.API.commit_offset(client(), group, topic, partitions, opts)
      end

      # Topic management functions
      def create_topics(topics, timeout, opts \\ []) do
        KafkaEx.API.create_topics(client(), topics, timeout, opts)
      end

      def create_topic(topic_name, opts \\ []) do
        KafkaEx.API.create_topic(client(), topic_name, opts)
      end

      def delete_topics(topics, timeout, opts \\ []) do
        KafkaEx.API.delete_topics(client(), topics, timeout, opts)
      end

      def delete_topic(topic_name, opts \\ []) do
        KafkaEx.API.delete_topic(client(), topic_name, opts)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Client Lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Start a new Kafka client.

  ## Options

    * `:brokers` - List of broker tuples, e.g., `[{"localhost", 9092}]`
    * `:client_id` - Client identifier string
    * All options supported by `KafkaEx.Client.start_link/1`

  ## Examples

      # Start with default configuration
      {:ok, client} = KafkaEx.API.start_client()

      # Start with custom brokers
      {:ok, client} = KafkaEx.API.start_client(brokers: [{"kafka1", 9092}, {"kafka2", 9092}])

      # Start a named client
      {:ok, client} = KafkaEx.API.start_client(name: MyApp.KafkaClient)
  """
  @spec start_client(opts) :: {:ok, pid} | {:error, term}
  def start_client(opts \\ []) do
    KafkaEx.Client.start_link(opts)
  end

  @doc """
  Returns a child specification for starting a client under a supervisor.

  ## Examples

      children = [
        {KafkaEx.API, name: MyApp.KafkaClient, brokers: [{"localhost", 9092}]}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
  """
  @spec child_spec(opts) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {KafkaEx.Client, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  # ---------------------------------------------------------------------------
  # Offset Functions
  # ---------------------------------------------------------------------------

  @doc """
  Fetch the latest offset for a given partition.

  ## Examples

      {:ok, offset} = KafkaEx.API.latest_offset(client, "my-topic", 0)
  """
  @spec latest_offset(client, topic_name, partition_id) :: {:ok, offset_val} | {:error, error_atom}
  @spec latest_offset(client, topic_name, partition_id, opts) :: {:ok, offset_val} | {:error, error_atom}
  def latest_offset(client, topic, partition_id, opts \\ []) do
    opts = Keyword.merge([api_version: 1], opts)
    partition = %{partition_num: partition_id, timestamp: :latest}

    case list_offsets(client, [{topic, [partition]}], opts) do
      {:ok, [%{partition_offsets: [offset]}]} -> {:ok, offset.offset}
      result -> result
    end
  end

  @doc """
  Fetch the earliest offset for a given partition.

  ## Examples

      {:ok, offset} = KafkaEx.API.earliest_offset(client, "my-topic", 0)
  """
  @spec earliest_offset(client, topic_name, partition_id) :: {:ok, offset_val} | {:error, error_atom}
  @spec earliest_offset(client, topic_name, partition_id, opts) :: {:ok, offset_val} | {:error, error_atom}
  def earliest_offset(client, topic, partition_id, opts \\ []) do
    opts = Keyword.merge([api_version: 1], opts)
    partition = %{partition_num: partition_id, timestamp: :earliest}

    case list_offsets(client, [{topic, [partition]}], opts) do
      {:ok, [%{partition_offsets: [offset]}]} -> {:ok, offset.offset}
      result -> result
    end
  end

  @doc """
  Returns list of Offsets per topic per partition.

  We support only one topic partition pair for now, as we don't request by leader.

  ## Examples

      partitions = [%{partition_num: 0, timestamp: :latest}]
      {:ok, offsets} = KafkaEx.API.list_offsets(client, [{"my-topic", partitions}])
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

  # ---------------------------------------------------------------------------
  # Metadata Functions
  # ---------------------------------------------------------------------------

  @doc """
  Get topic metadata for the given topics.

  Always calls out to the broker to get the most up-to-date metadata (and
  subsequently updates the client's state with the updated metadata). Set
  allow_topic_creation to true to allow the topics to be created if they
  don't exist.

  ## Examples

      {:ok, topics} = KafkaEx.API.topics_metadata(client, ["my-topic"])
  """
  @spec topics_metadata(client, [topic_name], boolean) :: {:ok, [Topic.t()]}
  def topics_metadata(client, topics, allow_topic_creation \\ false) do
    GenServer.call(client, {:topic_metadata, topics, allow_topic_creation})
  end

  @doc """
  Returns the cluster metadata from the given client.

  This returns the cluster metadata currently cached in the client's state.
  It does not make a network request to Kafka. If you need fresh metadata,
  use `metadata/2` or `metadata/3` instead.

  ## Examples

      {:ok, cluster_metadata} = KafkaEx.API.cluster_metadata(client)
  """
  @spec cluster_metadata(client) :: {:ok, ClusterMetadata.t()}
  def cluster_metadata(client) do
    GenServer.call(client, :cluster_metadata)
  end

  @doc """
  Fetch metadata from Kafka brokers.

  Makes a network request to fetch fresh metadata from Kafka. This updates
  the client's internal cluster metadata state and returns the updated metadata.

  By default, fetches metadata for all topics in the cluster. To fetch metadata
  for specific topics only, use `metadata/3`.

  ## Examples

      {:ok, metadata} = KafkaEx.API.metadata(client)
      {:ok, metadata} = KafkaEx.API.metadata(client, api_version: 2)
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
  Fetch metadata for specific topics.

  Makes a network request to fetch fresh metadata for the specified topics only.
  This is more efficient than fetching all topics if you only need information
  about a subset of topics.

  ## Examples

      {:ok, metadata} = KafkaEx.API.metadata(client, ["topic1", "topic2"], [])
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
  Returns the current correlation id for the given client.
  """
  @spec correlation_id(client) :: {:ok, correlation_id}
  def correlation_id(client) do
    GenServer.call(client, :correlation_id)
  end

  @doc """
  Fetch API versions supported by the Kafka broker.

  Queries the broker to discover which Kafka API versions it supports.
  This enables the client to negotiate compatible API versions for all operations.

  The response includes the minimum and maximum supported versions for each Kafka API,
  identified by their API key.

  ## Examples

      {:ok, versions} = KafkaEx.API.api_versions(client)
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
  autocommit.

  NOTE this function will not be supported after the legacy API is removed.
  """
  @spec set_consumer_group_for_auto_commit(client, consumer_group_name) :: :ok | {:error, :invalid_consumer_group}
  def set_consumer_group_for_auto_commit(client, consumer_group) do
    GenServer.call(client, {:set_consumer_group_for_auto_commit, consumer_group})
  end

  # ---------------------------------------------------------------------------
  # Consumer Group Functions
  # ---------------------------------------------------------------------------

  @doc """
  Sends a request to describe a group identified by its name.

  We support only one consumer group per request for now, as we don't
  group requests by group coordinator.

  ## Examples

      {:ok, description} = KafkaEx.API.describe_group(client, "my-group")
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
  Join a consumer group.

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

  ## Examples

      {:ok, result} = KafkaEx.API.join_group(client, "my-group", "")
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
  Synchronize consumer group state.

  Completes the consumer group rebalance protocol by synchronizing state
  between the group leader and followers. The leader provides partition
  assignments which are distributed to all members. Followers receive their
  assigned partitions from this call.

  ## Examples

      {:ok, result} = KafkaEx.API.sync_group(client, "my-group", gen_id, member_id, assignments: assignments)
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
  Leave a consumer group.

  Notifies the group coordinator that a consumer is voluntarily leaving the
  consumer group. This allows the coordinator to immediately trigger a rebalance
  without waiting for a session timeout, improving rebalance latency.

  ## Examples

      {:ok, result} = KafkaEx.API.leave_group(client, "my-group", member_id)
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
  Send a heartbeat to a consumer group coordinator.

  Sends a periodic heartbeat to the group coordinator to indicate that the
  consumer is still active and participating in the consumer group. This is
  required to maintain group membership and prevent rebalancing.

  ## Examples

      {:ok, result} = KafkaEx.API.heartbeat(client, "my-group", member_id, generation_id)
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
  Find the coordinator broker for a consumer group or transaction.

  Discovers which broker is the coordinator for the specified consumer group
  (or transactional producer). This is used internally for consumer group
  operations but can also be called directly.

  ## Options

    * `:coordinator_type` - 0 for group (default), 1 for transaction
    * `:api_version` - API version to use (default: 1)

  ## Examples

      # Find group coordinator
      {:ok, coordinator} = KafkaEx.API.find_coordinator(client, "my-consumer-group")

      # Find transaction coordinator
      {:ok, coordinator} = KafkaEx.API.find_coordinator(client, "my-transactional-id",
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

  # ---------------------------------------------------------------------------
  # Offset Management Functions
  # ---------------------------------------------------------------------------

  @doc """
  Fetch committed offsets for a consumer group.

  Retrieves the last committed offsets for the specified topic/partition(s)
  for a consumer group. This is useful for tracking consumer group progress
  and implementing manual offset management.

  ## Examples

      partitions = [%{partition_num: 0}]
      {:ok, offsets} = KafkaEx.API.fetch_committed_offset(client, "my-group", "my-topic", partitions)
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
  Commit offsets for a consumer group.

  Stores the committed offsets for the specified topic/partition(s) for a
  consumer group. This is used for manual offset management and consumer
  group coordination.

  ## Examples

      partitions = [%{partition_num: 0, offset: 100}]
      {:ok, result} = KafkaEx.API.commit_offset(client, "my-group", "my-topic", partitions)
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

  # ---------------------------------------------------------------------------
  # Produce Functions
  # ---------------------------------------------------------------------------

  @doc """
  Produce messages to a Kafka topic partition.

  Sends one or more messages to the specified topic and partition. Returns the
  base offset assigned to the first message in the batch.

  ## Parameters

    * `client` - The client pid
    * `topic` - Topic name
    * `partition` - Partition number, or `nil` to use partitioner
    * `messages` - List of message maps with `:value`, optional `:key`, `:timestamp`, `:headers`
    * `opts` - Options including:
      * `:api_version` - API version to use
      * `:required_acks` - Number of acks required (default: 1)
      * `:timeout` - Request timeout
      * `:partitioner` - Custom partitioner module (default: configured or `KafkaEx.Producer.Partitioner.Default`)

  ## Partitioning

  When `partition` is `nil`, the partitioner is used to determine the partition:

    * If message has a `:key`, the default partitioner uses murmur2 hash for consistent partitioning
    * If message has no `:key`, a random partition is selected

  You can configure the default partitioner globally:

      config :kafka_ex, partitioner: MyApp.CustomPartitioner

  Or per-request:

      KafkaEx.API.produce(client, "topic", nil, messages, partitioner: MyApp.CustomPartitioner)

  ## Examples

      # Explicit partition
      messages = [%{value: "hello"}, %{key: "k1", value: "world"}]
      {:ok, metadata} = KafkaEx.API.produce(client, "my-topic", 0, messages)

      # Use partitioner (partition determined by key)
      {:ok, metadata} = KafkaEx.API.produce(client, "my-topic", nil, [%{key: "user-123", value: "data"}])

      # Use custom partitioner
      {:ok, metadata} = KafkaEx.API.produce(client, "my-topic", nil, messages,
        partitioner: MyApp.RoundRobinPartitioner)
  """
  @spec produce(client, topic_name, partition_id | nil, [map()]) :: {:ok, RecordMetadata.t()} | {:error, error_atom}
  @spec produce(client, topic_name, partition_id | nil, [map()], opts) ::
          {:ok, RecordMetadata.t()} | {:error, error_atom}
  def produce(client, topic, partition, messages, opts \\ [])

  def produce(client, topic, nil, messages, opts) do
    with {:ok, resolved_partition} <- resolve_partition(client, topic, messages, opts) do
      produce(client, topic, resolved_partition, messages, opts)
    end
  end

  def produce(client, topic, partition, messages, opts) when is_integer(partition) do
    case GenServer.call(client, {:produce, topic, partition, messages, opts}) do
      {:ok, result} -> {:ok, result}
      {:error, %{error: error_atom}} -> {:error, error_atom}
      {:error, error_atom} -> {:error, error_atom}
    end
  end

  @doc """
  Produce a single message to a Kafka topic partition.

  Convenience function for producing a single message. Wraps the message in a list and calls `produce/5`.

  ## Examples

      # Explicit partition
      {:ok, metadata} = KafkaEx.API.produce_one(client, "my-topic", 0, "hello")
      {:ok, metadata} = KafkaEx.API.produce_one(client, "my-topic", 0, "hello", key: "my-key")

      # Use partitioner (key-based partitioning)
      {:ok, metadata} = KafkaEx.API.produce_one(client, "my-topic", nil, "hello", key: "user-123")
  """
  @spec produce_one(client, topic_name, partition_id | nil, binary) :: {:ok, RecordMetadata.t()} | {:error, error_atom}
  @spec produce_one(client, topic_name, partition_id | nil, binary, opts) ::
          {:ok, RecordMetadata.t()} | {:error, error_atom}
  def produce_one(client, topic, partition, value, opts \\ []) do
    {message_opts, produce_opts} = Keyword.split(opts, [:key, :timestamp, :headers])

    message =
      %{value: value}
      |> maybe_put(:key, Keyword.get(message_opts, :key))
      |> maybe_put(:timestamp, Keyword.get(message_opts, :timestamp))
      |> maybe_put(:headers, Keyword.get(message_opts, :headers))

    produce(client, topic, partition, [message], produce_opts)
  end

  # Resolves partition using the configured partitioner
  defp resolve_partition(client, topic, messages, opts) do
    partitioner = Keyword.get(opts, :partitioner, KafkaEx.Producer.Partitioner.get_partitioner())

    # Get the first message's key and value for partitioning
    # (assuming all messages in a batch should go to the same partition)
    first_message = List.first(messages) || %{}
    key = Map.get(first_message, :key)
    value = Map.get(first_message, :value, "")

    # Get partition count from metadata
    case topics_metadata(client, [topic], true) do
      {:ok, [topic_info]} ->
        partition_count = map_size(topic_info.partition_leaders)

        if partition_count > 0 do
          partition = partitioner.assign_partition(topic, key, value, partition_count)
          {:ok, partition}
        else
          {:error, :no_partitions_available}
        end

      {:ok, []} ->
        {:error, :topic_not_found}

      {:error, _} = error ->
        error
    end
  end

  # ---------------------------------------------------------------------------
  # Fetch Functions
  # ---------------------------------------------------------------------------

  @doc """
  Fetch records from a Kafka topic partition.

  Retrieves records from the specified topic and partition starting from the given offset.
  Returns the fetched records along with metadata about the fetch (high watermark, etc).

  ## Options

    * `:max_bytes` - Maximum bytes to fetch per partition (default: 1,000,000)
    * `:max_wait_time` - Maximum time to wait for records in ms (default: 10,000)
    * `:min_bytes` - Minimum bytes to accumulate before returning (default: 1)
    * `:isolation_level` - 0 for READ_UNCOMMITTED, 1 for READ_COMMITTED (V4+, default: 0)
    * `:api_version` - API version to use (default: 3)

  ## Examples

      {:ok, result} = KafkaEx.API.fetch(client, "my-topic", 0, 0)
      messages = result.records
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
  Fetch all available records from a topic partition.

  Convenience function that fetches records from the earliest offset up to the high watermark.
  Useful for reading all data in a partition (within the configured max_bytes limit).

  ## Examples

      {:ok, result} = KafkaEx.API.fetch_all(client, "my-topic", 0)
  """
  @spec fetch_all(client, topic_name, partition_id) :: {:ok, Fetch.t()} | {:error, error_atom}
  @spec fetch_all(client, topic_name, partition_id, opts) :: {:ok, Fetch.t()} | {:error, error_atom}
  def fetch_all(client, topic, partition, opts \\ []) do
    case earliest_offset(client, topic, partition) do
      {:ok, offset} -> fetch(client, topic, partition, offset, opts)
      {:error, _} = error -> error
    end
  end

  # ---------------------------------------------------------------------------
  # Topic Management Functions
  # ---------------------------------------------------------------------------

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

      ## Examples

      # Create a topic with broker defaults
    {:ok, result} = KafkaEx.API.create_topics(client, [
        [topic: "my-topic"]
      ], 10_000)

      # Create a topic with custom configuration
      {:ok, result} = KafkaEx.API.create_topics(client, [
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
      {:ok, result} = KafkaEx.API.create_topics(client, [
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
  @spec create_topics(client, list(), non_neg_integer()) :: {:ok, CreateTopics.t()} | {:error, error_atom}
  @spec create_topics(client, list(), non_neg_integer(), opts) :: {:ok, CreateTopics.t()} | {:error, error_atom}
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
      {:ok, result} = KafkaEx.API.create_topic(client, "my-topic")

      # Create with specific configuration
      {:ok, result} = KafkaEx.API.create_topic(client, "my-topic",
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
      {:ok, result} = KafkaEx.API.delete_topics(client, ["my-topic"], 30_000)

      # Delete multiple topics
      {:ok, result} = KafkaEx.API.delete_topics(client, ["topic1", "topic2", "topic3"], 30_000)

      # Check results
      if DeleteTopics.success?(result) do
        IO.puts("All topics deleted successfully")
      else
        failed = DeleteTopics.failed_topics(result)
        IO.inspect(failed, label: "Failed to delete")
      end
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
      {:ok, result} = KafkaEx.API.delete_topic(client, "my-topic")

      # Delete with custom timeout
      {:ok, result} = KafkaEx.API.delete_topic(client, "my-topic", timeout: 60_000)
  """
  @spec delete_topic(client, topic_name) :: {:ok, DeleteTopics.t()} | {:error, error_atom}
  @spec delete_topic(client, topic_name, opts) :: {:ok, DeleteTopics.t()} | {:error, error_atom}
  def delete_topic(client, topic_name, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, 30_000)
    delete_topics(client, [topic_name], timeout, opts)
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Helper to conditionally add keys to a map only if value is not nil
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
