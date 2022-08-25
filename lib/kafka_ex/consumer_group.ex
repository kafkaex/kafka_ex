defmodule KafkaEx.ConsumerGroup do
  @moduledoc """
  A process that manages membership in a Kafka consumer group.

  Consumers in a consumer group coordinate with each other through a Kafka
  broker to distribute the work of consuming one or several topics without any
  overlap. This is facilitated by the
  [Kafka client-side assignment protocol](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal).

  Any time group membership changes (a member joins or leaves the group), a
  Kafka broker initiates group synchronization by asking one of the group
  members (the leader elected by the broker) to provide partition assignments
  for the whole group.  KafkaEx uses a round robin partition assignment
  algorithm by default.  This can be overridden by passing a callback function
  in the `:partition_assignment_callback` option.  See
  `KafkaEx.ConsumerGroup.PartitionAssignment` for details on partition
  assignment functions.

  A `KafkaEx.ConsumerGroup` process is responsible for:

  1. Maintaining membership in a Kafka consumer group.
  2. Determining partition assignments if elected as the group leader.
  3. Launching and terminating `KafkaEx.GenConsumer` processes based on its
    assigned partitions.

  To use a `KafkaEx.ConsumerGroup`, a developer must define a module that
  implements the `KafkaEx.GenConsumer` behaviour and start a
  `KafkaEx.ConsumerGroup` configured to use that module.

  The api versions of some of the underlying messages can be specified in the
  `:api_versions` option.  Note that these will be ignored (api version 0 used)
  unless you have `kafka_version: "kayrock"` set in the KafkaEx application
  config.  The following versions can be specified:

  * `:fetch` - Fetch requests - use v2+ for newer versions of Kafka
  * `:offset_fetch` - Offset fetch requests - use v1+ for offsets stored in
    Kafka (as opposed to zookeeper)
  * `:offset_commit` - Offset commit requests - use v1+ to store offsets in
    Kafka (as opposed to zookeeper)

  ## Example

  Suppose we want to consume from a topic called `"example_topic"` with a
  consumer group named `"example_group"` and we have a `KafkaEx.GenConsumer`
  implementation called `ExampleGenConsumer` (see the `KafkaEx.GenConsumer`
  documentation).  We could start a consumer group in our application's
  supervision tree as follows:

  ```
  defmodule MyApp do
    use Application

    def start(_type, _args) do
      consumer_group_opts = [
        # setting for the ConsumerGroup
        heartbeat_interval: 1_000,
        # this setting will be forwarded to the GenConsumer
        commit_interval: 1_000
      ]

      gen_consumer_impl = ExampleGenConsumer
      consumer_group_name = "example_group"
      topic_names = ["example_topic"]

      children = [
        # ... other children
        %{
          id: KafkaEx.ConsumerGroup,
          start: {
            KafkaEx.ConsumerGroup,
            :start_link,
            [gen_consumer_impl, consumer_group_name, topic_names, consumer_group_opts]
          }
        }
        # ... other children
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
    end
  end
  ```

  **Note** It is not necessary for the Elixir nodes in a consumer group to be
  connected (i.e., using distributed Erlang methods).  The coordination of
  group consumers is mediated by the broker.

  See `start_link/4` for configuration details.
  """

  use Supervisor

  alias KafkaEx.ConsumerGroup.PartitionAssignment
  alias KafkaEx.GenConsumer

  @typedoc """
  Option values used when starting a consumer group

  * `:heartbeat_interval` - How frequently, in milliseconds, to send heartbeats
     to the broker.  This impacts how quickly we will process partition
     changes as consumers start/stop.  Default: 5000 (5 seconds).
  * `:session_timeout` - Consumer group session timeout in milliseconds.
     Default: 30000 (30 seconds).  See below.
  * `:session_timeout_padding` - Timeout padding for consumer group options.
     Default: 10000 (10 seconds).  See below.
  * Any of `t:KafkaEx.GenConsumer.option/0`,
     which will be passed on to consumers
  * `:gen_server_opts` - `t:GenServer.options/0` passed on to the manager
     GenServer
  * `:name` - Name for the consumer group supervisor
  * `:max_restarts`, `:max_seconds` - Supervisor restart policy parameters
  * `:partition_assignment_callback` - See
     `t:KafkaEx.ConsumerGroup.PartitionAssignment.callback/0`
  * `:uris` - See `KafkaEx.create_worker/2`

  Note `:session_timeout` is registered with the broker and determines how long
  before the broker will de-register a consumer from which it has not heard a
  heartbeat.  This value must between the broker cluster's configured values
  for `group.min.session.timeout.ms` and `group.max.session.timeout.ms` (6000
  and 30000 by default).  See
  [https://kafka.apache.org/documentation/#configuration](https://kafka.apache.org/documentation/#configuration).
  You may need to adjust `session_timeout_padding` on high-latency clusters to
  avoid timing out when joining/syncing consumer groups.
  """
  @type option ::
          KafkaEx.GenConsumer.option()
          | {:heartbeat_interval, pos_integer}
          | {:session_timeout, pos_integer}
          | {:session_timeout_padding, pos_integer}
          | {:partition_assignment_callback, PartitionAssignment.callback()}
          | {:gen_server_opts, GenServer.options()}
          | {:name, Supervisor.name()}
          | {:max_restarts, non_neg_integer}
          | {:max_seconds, non_neg_integer}
          | {:uris, KafkaEx.uri()}

  @type options :: [option]

  @doc """
  Starts a consumer group process tree process linked to the current process.

  This can be used to start a `KafkaEx.ConsumerGroup` as part of a supervision
  tree.

  `consumer_module` is
   - a module that implements the `KafkaEx.GenConsumer`
  behaviour.
   - a tuple of `{gen_consumer_module, consumer_module}` can substitute another
     `GenServer` implementation for `KafkaEx.GenConsumer`. When a single module
     is passed it is transformed to `{KafkaEx.GenConsumer, consumer_module}`.

  `group_name` is the name of the consumer group.

  `topics` is a list of topics that the consumer group should consume from.

  `opts` can be composed of options for the supervisor as well as for the
  `KafkEx.GenConsumer` processes that will be spawned by the supervisor.  See
  `t:option/0` for details.

  *Note* When starting a consumer group with multiple topics, you should
  propagate this configuration change to your consumers.  If you add a topic to
  an existing consumer group from a single consumer, it may take a long time
  to propagate depending on the leader election process.

  ### Return Values

  This function has the same return values as `Supervisor.start_link/3`.
  """
  @spec start_link(module | {module, module}, binary, [binary], options) ::
          Supervisor.on_start()
  def start_link(consumer_module, group_name, topics, opts \\ [])

  def start_link(consumer_module, group_name, topics, opts)
      when is_atom(consumer_module) do
    start_link({KafkaEx.GenConsumer, consumer_module}, group_name, topics, opts)
  end

  def start_link(
        {gen_consumer_module, consumer_module},
        group_name,
        topics,
        opts
      ) do
    {supervisor_opts, module_opts} =
      Keyword.split(opts, [:name, :strategy, :max_restarts, :max_seconds])

    Supervisor.start_link(
      __MODULE__,
      {{gen_consumer_module, consumer_module}, group_name, topics, module_opts},
      supervisor_opts
    )
  end

  @doc """
  Returns the generation id of the consumer group.

  The generation id is provided by the broker on sync.  Returns `nil` if
  queried before the initial sync has completed.
  """
  @spec generation_id(Supervisor.supervisor(), timeout) :: integer | nil
  def generation_id(supervisor_pid, timeout \\ 5000) do
    call_manager(supervisor_pid, :generation_id, timeout)
  end

  @doc """
  Returns the consumer group member id

  The id is assigned by the broker.  Returns `nil` if queried before the
  initial sync has completed.
  """
  @spec member_id(Supervisor.supervisor(), timeout) :: binary | nil
  def member_id(supervisor_pid, timeout \\ 5000) do
    call_manager(supervisor_pid, :member_id, timeout)
  end

  @doc """
  Returns the member id of the consumer group's leader

  This is provided by the broker on sync.  Returns `nil` if queried before the
  initial sync has completed
  """
  @spec leader_id(Supervisor.supervisor(), timeout) :: binary | nil
  def leader_id(supervisor_pid, timeout \\ 5000) do
    call_manager(supervisor_pid, :leader_id, timeout)
  end

  @doc """
  Returns true if this consumer is the leader of the consumer group

  Leaders are elected by the broker and are responsible for assigning
  partitions.  Returns false if queried before the initial sync has completed.
  """
  @spec leader?(Supervisor.supervisor(), timeout) :: boolean
  def leader?(supervisor_pid, timeout \\ 5000) do
    call_manager(supervisor_pid, :am_leader, timeout)
  end

  @doc """
  Returns a list of topic and partition assignments for which this consumer is
  responsible.

  These are assigned by the leader and communicated by the broker on sync.
  """
  @spec assignments(Supervisor.supervisor(), timeout) :: [
          {topic :: binary, partition_id :: non_neg_integer}
        ]
  def assignments(supervisor_pid, timeout \\ 5000) do
    call_manager(supervisor_pid, :assignments, timeout)
  end

  @doc """
  Returns the pid of the `KafkaEx.GenConsumer.Supervisor` that supervises this
  member's consumers.

  Returns `nil` if called before the initial sync.
  """
  @spec consumer_supervisor_pid(Supervisor.supervisor(), timeout) :: nil | pid
  def consumer_supervisor_pid(supervisor_pid, timeout \\ 5000) do
    call_manager(supervisor_pid, :consumer_supervisor_pid, timeout)
  end

  @doc """
  Returns the pids of consumer processes
  """
  @spec consumer_pids(Supervisor.supervisor()) :: [pid]
  def consumer_pids(supervisor_pid) do
    supervisor_pid
    |> consumer_supervisor_pid
    |> GenConsumer.Supervisor.child_pids()
  end

  @doc """
  Returns the name of the consumer group
  """
  @spec group_name(Supervisor.supervisor(), timeout) :: binary
  def group_name(supervisor_pid, timeout \\ 5000) do
    call_manager(supervisor_pid, :group_name, timeout)
  end

  @doc """
  Returns a map from `{topic, partition_id}` to consumer pid
  """
  @spec partition_consumer_map(Supervisor.supervisor()) :: %{
          {topic :: binary, partition_id :: non_neg_integer} => pid
        }
  def partition_consumer_map(supervisor_pid) do
    supervisor_pid
    |> consumer_pids
    |> Enum.into(%{}, fn pid ->
      {GenConsumer.partition(pid), pid}
    end)
  end

  @doc """
  Returns true if at least one child consumer process is alive
  """
  @spec active?(Supervisor.supervisor(), timeout) :: boolean
  def active?(supervisor_pid, timeout \\ 5000) do
    consumer_supervisor = consumer_supervisor_pid(supervisor_pid, timeout)

    consumer_supervisor &&
      Process.alive?(consumer_supervisor) &&
      GenConsumer.Supervisor.active?(consumer_supervisor)
  end

  @doc """
  Returns the pid of the `KafkaEx.ConsumerGroup.Manager` process for the
  given consumer group supervisor.

  Intended for introspection usage only.
  """
  @spec get_manager_pid(Supervisor.supervisor()) :: pid
  def get_manager_pid(supervisor_pid) do
    {_, pid, _, _} =
      Enum.find(
        Supervisor.which_children(supervisor_pid),
        fn
          {KafkaEx.ConsumerGroup.Manager, _, _, _} -> true
          {_, _, _, _} -> false
        end
      )

    pid
  end

  # used by ConsumerGroup.Manager to set partition assignments
  @doc false
  def start_consumer(
        pid,
        {gen_consumer_module, consumer_module},
        group_name,
        assignments,
        opts
      ) do
    child =
      Supervisor.child_spec(
        {
          KafkaEx.GenConsumer.Supervisor,
          %{
            gen_consumer_module: gen_consumer_module,
            consumer_module: consumer_module,
            group_name: group_name,
            assignments: assignments,
            opts: opts
          }
        },
        id: :consumer
      )

    case Supervisor.start_child(pid, child) do
      {:ok, consumer_pid} -> {:ok, consumer_pid}
      {:ok, consumer_pid, _info} -> {:ok, consumer_pid}
    end
  end

  # used by ConsumerGroup to pause consumption during rebalance
  @doc false
  def stop_consumer(pid) do
    case Supervisor.terminate_child(pid, :consumer) do
      :ok ->
        Supervisor.delete_child(pid, :consumer)

      {:error, :not_found} ->
        :ok
    end
  end

  @doc false
  def init({{gen_consumer_module, consumer_module}, group_name, topics, opts}) do
    opts = Keyword.put(opts, :supervisor_pid, self())

    children = [
      {KafkaEx.ConsumerGroup.Manager,
       {{gen_consumer_module, consumer_module}, group_name, topics, opts}}
    ]

    Supervisor.init(children,
      strategy: :one_for_all,
      max_restarts: 0,
      max_seconds: 1
    )
  end

  defp call_manager(supervisor_pid, call, timeout) do
    supervisor_pid
    |> get_manager_pid
    |> GenServer.call(call, timeout)
  end
end
