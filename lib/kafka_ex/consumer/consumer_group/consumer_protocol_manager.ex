defmodule KafkaEx.Consumer.ConsumerGroup.ConsumerProtocolManager do
  @moduledoc """
  Membership in a consumer group that uses the KIP-848 "consumer" group
  protocol (`group.protocol=consumer`, Kafka 4.0+): the member state machine
  that drives `KafkaEx.API.consumer_group_heartbeat/3` and one
  `KafkaEx.Consumer.GenConsumer` per assigned partition.

  This is the KIP-848 counterpart of `KafkaEx.Consumer.ConsumerGroup.Manager`
  (classic JoinGroup/SyncGroup). There is no group-wide stop-the-world
  rebalance: the broker computes each member's target assignment, and a member
  only stops the consumers of partitions it loses and only starts the
  consumers of partitions it gains. Partitions it keeps are consumed without
  interruption.

  It is modelled on the Java 4.4 client's `ConsumerMembershipManager`,
  `AbstractMembershipManager` and `ConsumerHeartbeatRequestManager`; where it
  differs, the code says so.

  ## Lifecycle

    1. **Join.** The member id is generated once, at start, with
       `KafkaEx.API.generate_member_id/0` (KIP-1082) and kept for the
       member's lifetime, including every retry of the join and every rejoin
       after fencing (Java: `AbstractMembershipManager.memberId` is final). The
       join is a heartbeat with `member_epoch: 0`, the subscription, the
       rebalance timeout and `topic_partitions: []`.
    2. **Heartbeat** every `heartbeat_interval_ms` *as dictated by the broker
       in each response* (there is no client setting). Only fields that
       changed are sent (KIP-848 "nil = unchanged"); the join and the first
       heartbeat after an error send them all. `topic_partitions` is the set
       of partitions the member reports as owned, sent when it changed.
    3. **Reconcile** a new target assignment (one at a time; a newer target
       that arrives meanwhile is picked up when the current step ends):
       * revoke first: stop the consumers of `owned - target` concurrently
         (each commits in `terminate/2`), then report the reduced owned set
         with an immediate heartbeat;
       * assign: start a consumer for each partition of `target - owned`
         with the current member epoch (one at a time: each consumer's init
         starts its client, as for a classic group; a newer target waits
         until they are all started). Topics arrive as topic ids; names come
         from the response, else from a Metadata request by id (ids it
         fails for, with any error, are skipped and the rest asked again). A
         partition whose topic name is unknown is never assigned: it stays
         pending and the lookup is retried with a backoff;
       * partitions in both sets keep running and are moved to the new
         member epoch with `KafkaEx.Consumer.GenConsumer.update_membership/3`;
       * then acknowledge: report the new owned set with an immediate
         heartbeat.
    4. **Leave** on shutdown: stop the consumers (they commit), then a
       heartbeat with `member_epoch: -1` (or `-2` for a static member, which
       keeps its assignment until its session expires). A member in the
       group keeps heartbeating (current epoch, nothing else) while its
       consumers stop, as Java does in `PREPARE_LEAVING`, so a slow stop does
       not outlast the session timeout and get its partitions handed to
       another member while they are still consumed here. The leave is also
       sent while the member is still joining (or rejoining) once a join has
       been sent: the broker may already have registered the member under
       its member id and given it partitions, and without a leave they would
       not be consumed until the session expires. As in Java
       (`AbstractMembershipManager.leaveGroup` skips only `FENCED` / `FATAL`
       members), no leave is sent while fenced or after a terminal error.

  ## Errors

    * `:fenced_member_epoch` / `:unknown_member_id` on a heartbeat: the member
      is fenced (Java `transitionToFenced`). Its partitions are *lost*: their
      consumers are stopped **without** a final commit (the epoch is no longer
      valid, the broker would reject it; Java `onPartitionsLost`), then the
      member rejoins with epoch 0 and the **same** member id.
    * Transient errors (connection, coordinator moved or loading; see
      `KafkaEx.Support.Retry.consumer_group_heartbeat_retryable?/1`) are
      retried by the client first. It re-discovers the coordinator on
      `:not_coordinator` / `:coordinator_not_available` and also after a
      connection failure (`:timeout`, `:closed`, `:no_broker`, ...; a
      `:timeout` is not resent, the next heartbeat re-discovers it), as Java
      does, so a member whose coordinator broker died finds the new one
      without waiting for a metadata refresh. If they
      persist the heartbeat is retried with an exponential backoff
      (`:heartbeat_retry_backoff_ms` up to `:heartbeat_retry_max_backoff_ms`,
      Java `retry.backoff.ms` / `retry.backoff.max.ms`).
    * `:topic_authorization_failed` is logged as an error and retried like a
      transient error; the member stays in the group (Java 4.4 raises an
      error event to the application and does not fail the member, so one
      unauthorized topic, e.g. matched by a regex, does not stop the group).
    * Anything else (`:group_authorization_failed`, `:unsupported_assignor`,
      `:unreleased_instance_id`, `:fenced_instance_id`,
      `:group_max_size_reached`, `:invalid_regular_expression`,
      `:unsupported_version`, `:api_not_supported_by_broker`, ...) is terminal,
      as in Java (`handleFatalFailure` / `transitionToFatal`): the member stops
      with `{:shutdown, {:terminal, reason}}`, its consumers are stopped
      without a commit, no leave is sent, and
      `[:kafka_ex, :consumer, :member_terminated]` is emitted. Under the
      `one_for_all` / `max_restarts: 0` consumer group supervisor this stops
      the whole group. `:unsupported_version` and
      `:api_not_supported_by_broker` are logged with a hint to use
      `group_protocol: :classic`: the cluster does not offer the protocol.
    * A broker that does not advertise ConsumerGroupHeartbeat at all
      (Kafka < 4.0) is detected before the join: `init/1` stops with
      `:consumer_group_protocol_not_supported`, so the consumer group fails
      to start. There is no fallback to the classic protocol (as in Java).

  ## Consumer ownership

  Consumers are started with
  `KafkaEx.Consumer.GenConsumer.Supervisor.start_partition/3` (they are
  `restart: :temporary`) and monitored. This process is their only owner:
  every start, stop, membership update and restart is decided here, so an
  update or a revoke can never race a background restart. Starts (assigns
  and restarts alike) run in background processes, so a slow consumer start
  (its own client connecting to the brokers) never stops this process from
  heartbeating; each consumer is monitored as soon as its own start returns,
  not once a whole batch has started. A revoke or a rejoin waits for the
  restart in flight of a partition it releases. When a consumer exits on its
  own:

    * abnormally (a crash): it is started again with the *current* member
      epoch after a per-partition backoff (`:partition_restart_backoff_ms`
      doubling up to `:partition_restart_max_backoff_ms`, reset after a
      minute of uptime; a start that fails is not uptime, so a consumer that
      cannot start keeps backing off). The partition stays owned meanwhile;
      if it leaves the target and comes back before its revoke runs, the
      restart still happens.
    * `{:shutdown, {:rejoin_required, reason}}` (its commit or offset fetch
      was rejected as `:unknown_member_id` / `:fenced_member_epoch`): an
      immediate heartbeat asks the broker for the member's real state (Java's
      `CommitRequestManager` does not fence the member on a commit error
      either: the heartbeat does), and the consumer is restarted with a
      backoff. If the member really was fenced, the heartbeat fences it.
    * `{:shutdown, {:terminal_error, reason}}` (e.g. a terminal commit
      error): the member stops with `{:shutdown, {:terminal, reason}}`.
      Unlike the classic group (where the partition silently stops being
      consumed), the failure is loud.
    * `:normal`, `:shutdown` or another `{:shutdown, _}` (the consumer chose
      to stop, or the consumer supervisor is shutting down): it is not
      restarted, as under the classic `restart: :transient`. The partition
      stays owned until the broker revokes it, which in this protocol may not
      happen for a long time. Every reason but exactly `:shutdown` (what the
      consumer supervisor uses on a group stop) is logged as an error and
      emits `[:kafka_ex, :consumer, :partition_stopped]`.

  ## Options

  Besides the `KafkaEx.Consumer.GenConsumer` options (passed on to every
  consumer) and the connection options (`:uris`, `:use_ssl`, `:ssl_options`,
  `:auth`):

    * `:server_assignor` - server-side assignor (`"uniform"` or `"range"` on
      Apache Kafka); default `nil`, the broker's default
    * `:rebalance_timeout_ms` - how long the broker waits for this member to
      revoke partitions before fencing it; default `300_000` (Java
      `max.poll.interval.ms`)
    * `:subscribed_topic_regex` - subscribe by regular expression (RE2/J,
      ConsumerGroupHeartbeat v1+) instead of, or besides, the topic names;
      default `nil`
    * `:group_instance_id` - static membership instance id; default `nil`
    * `:rack_id` - **optional rack awareness**, off by default. With `nil`
      (the default) no rack id is ever sent and the member behaves exactly
      like one without rack awareness. When set, it is passed through on the
      join so the broker's assignor can prefer partitions with a replica in
      the same rack; there is no client-side rack logic.
    * `:heartbeat_retry_backoff_ms` (100) / `:heartbeat_retry_max_backoff_ms`
      (1_000) - backoff between heartbeats that failed with a transient error
    * `:partition_restart_backoff_ms` (500) /
      `:partition_restart_max_backoff_ms` (30_000) - backoff before a crashed
      consumer is started again
    * `:topic_resolution_backoff_ms` (500) /
      `:topic_resolution_max_backoff_ms` (10_000) - backoff between Metadata
      lookups of topic ids whose name is unknown
    * `:leave_timeout_ms` - how long the leave heartbeat may take on
      shutdown; default `5_000`
    * `:membership_update_timeout_ms` - timeout of each
      `GenConsumer.update_membership/3` (it queues behind a fetch
      long-poll); default `60_000`. A consumer that misses it is retried after
      `:partition_restart_backoff_ms` and does not hold back the
      acknowledgement of the assignment.

  ## Supervision

  It is started like `KafkaEx.Consumer.ConsumerGroup.Manager`, as a child of
  a `KafkaEx.Consumer.ConsumerGroup` supervisor (`:supervisor_pid`), under
  which it adds its `KafkaEx.Consumer.GenConsumer.Supervisor` (child id
  `:consumer`). `child_spec/1` sets a `:shutdown` that covers stopping the
  consumers (their own `:shutdown`) plus the leave.

  ## Telemetry

    * `[:kafka_ex, :consumer, :member_joined]` - the member (re)joined;
      measurements `%{count: 1}`, metadata `%{group_id, member_id,
      member_epoch}`
    * `[:kafka_ex, :consumer, :reconcile]` - an assignment change was
      reconciled and acknowledged; measurements `%{revoked:, assigned:,
      duration:}` (native time units), metadata `%{group_id, member_id,
      member_epoch}`
    * `[:kafka_ex, :consumer, :member_fenced]` - the member was fenced and
      its partitions lost; measurements `%{count: 1, lost: n}`, metadata
      `%{group_id, member_id, member_epoch, reason}`
    * `[:kafka_ex, :consumer, :member_left]` - the leave heartbeat was sent
      on shutdown; measurements `%{count: 1}`, metadata `%{group_id,
      member_id, member_epoch, result}` (`member_epoch` is `-1` or `-2`,
      `result` is `:ok` or `{:error, reason}`)
    * `[:kafka_ex, :consumer, :partition_stopped]` - a consumer stopped on
      its own and is not restarted; measurements `%{count: 1}`, metadata
      `%{group_id, member_id, member_epoch, topic, partition, reason}`
    * `[:kafka_ex, :consumer, :member_terminated]` - see `KafkaEx.Telemetry`
    * every heartbeat is also a `[:kafka_ex, :consumer,
      :consumer_group_heartbeat, ...]` span, emitted by the client
  """
  use GenServer

  alias KafkaEx.API, as: KafkaExAPI
  alias KafkaEx.Client
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Config
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Consumer.GenConsumer.Supervisor, as: GenConsumerSupervisor
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Messages.ConsumerGroupHeartbeat
  alias KafkaEx.Messages.TopicPartitions
  alias KafkaEx.Support.Retry
  alias KafkaEx.Telemetry

  require Logger

  defmodule State do
    @moduledoc false

    # owned: %{{topic_id, partition} => entry}, where entry is
    #   %{topic: name, pid: pid | nil, ref: reference | nil,
    #     epoch: the member epoch the consumer holds (nil when not running),
    #     status: :running | :restarting | :starting | :stopped,
    #     crashes: consecutive abnormal exits, started_at: monotonic ms,
    #     token: restart timer token | :due (timer fired while out of the
    #       target, restart when back) | nil, push_after: monotonic ms | nil}
    defstruct [
      :supervisor_pid,
      :client,
      :consumer_supervisor,
      :gen_consumer_module,
      :consumer_module,
      :consumer_opts,
      :group_name,
      :topics,
      :topic_regex,
      :server_assignor,
      :rebalance_timeout_ms,
      :instance_id,
      :rack_id,
      :member_id,
      :config,
      :heartbeat_timer,
      :heartbeat_interval_ms,
      :target,
      # The revoke / assign / lose in flight (one at a time).
      :op,
      # The epoch push in flight. Separate from :op: it queues behind fetch
      # long-polls and must not hold back a revoke.
      :push,
      :reconcile,
      :resolve_timer,
      # The Metadata-by-id lookup of unnamed topic ids in flight:
      # %{pid, ref, ids}. Async like the heartbeat: the lookup can take the
      # client's whole call budget (longer than a session timeout), and the
      # manager must keep heartbeating meanwhile.
      :lookup,
      # The heartbeat in flight: %{pid, ref, partitions} (one at a time, like
      # Java's HeartbeatRequestState). Sent from a monitored process so the
      # manager keeps handling consumer exits, revokes and shutdown while a
      # heartbeat waits on a slow or unreachable coordinator.
      :inflight,
      # A heartbeat was requested while one was in flight: send it right
      # after the reply.
      heartbeat_pending: false,
      # Consumer restarts in flight: %{monitor_ref => %{pid, key, epoch}}.
      # Async like the assign: a consumer start can take long (its own client
      # connects to every bootstrap broker), and the manager must keep
      # heartbeating meanwhile.
      starts: %{},
      # Revoked partitions without a running consumer, reported with the
      # revoke in flight.
      revoking: [],
      owns_client: false,
      # A heartbeat failed with a terminal error (Java transitionToFatal):
      # the partitions are lost and no leave is sent.
      fatal: false,
      member_epoch: 0,
      # A join (epoch 0) was sent: the broker may know the member id, so a
      # shutdown sends a leave even while still joining.
      join_sent: false,
      # :joining (sending epoch 0) | :stable | :fenced (releasing its
      # partitions before it rejoins)
      phase: :joining,
      heartbeat_failures: 0,
      # KIP-848: every field is sent on the join and after an error.
      send_full: true,
      # The topic_partitions last sent (nil: unknown, send them).
      sent_partitions: nil,
      # The owned set reported to the broker.
      reported: MapSet.new(),
      topic_names: %{},
      owned: %{},
      resolve_failures: 0
    ]

    @type t :: %__MODULE__{}
  end

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)
  @heartbeat_api_key @protocol.api_key(:consumer_group_heartbeat)
  # Java AbstractHeartbeatRequestManager.CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG,
  # in KafkaEx terms.
  @protocol_not_supported_msg "the cluster does not support the KIP-848 consumer group protocol " <>
                                "(Kafka 4.0+ with \"consumer\" in group.coordinator.rebalance.protocols); " <>
                                "start the group with group_protocol: :classic until the cluster is upgraded"

  # The terminal-heartbeat variant: UNSUPPORTED_VERSION also covers a
  # cluster that has the protocol but not the version a feature needs.
  @protocol_maybe_not_supported_msg "the cluster may not support the KIP-848 consumer group protocol " <>
                                      "(Kafka 4.0+ with \"consumer\" in group.coordinator.rebalance.protocols); " <>
                                      "if so, start the group with group_protocol: :classic until the cluster is upgraded"

  @default_rebalance_timeout_ms 300_000
  @default_config %{
    heartbeat_retry_backoff_ms: 100,
    heartbeat_retry_max_backoff_ms: 1_000,
    partition_restart_backoff_ms: 500,
    partition_restart_max_backoff_ms: 30_000,
    topic_resolution_backoff_ms: 500,
    topic_resolution_max_backoff_ms: 10_000,
    leave_timeout_ms: 5_000,
    membership_update_timeout_ms: 60_000
  }
  @manager_keys [
    :supervisor_pid,
    :client,
    :server_assignor,
    :rebalance_timeout_ms,
    :subscribed_topic_regex,
    :group_instance_id,
    :rack_id,
    :heartbeat_interval,
    :session_timeout,
    :session_timeout_padding,
    :rebalance_timeout,
    :partition_assignment_callback
    | Map.keys(@default_config)
  ]
  # A consumer that ran this long before crashing starts a fresh backoff.
  @crash_reset_ms 60_000
  @max_int32 2_147_483_647
  # The starts are sent concurrently, but each runs the consumer's init
  # (which starts its own client) inside GenConsumer.Supervisor, one after
  # another, as for a classic ConsumerGroup. A newer target (e.g. a revoke)
  # waits until the assign ends: about partitions x client connect time.
  @start_concurrency 16
  # The fetch :max_wait_time default and the margin GenConsumer.Supervisor
  # gives a consumer to stop (see GenConsumer.Supervisor.start_partition/3).
  @default_max_wait_time 10_000
  @consumer_stop_margin 10_000
  @terminate_margin 5_000
  @async_result {__MODULE__, :async_result}
  @async_started {__MODULE__, :async_started}

  @type topic_id :: <<_::128>>
  @type key :: {topic_id, non_neg_integer}

  # ---------------------------------------------------------------------------
  # Client API

  @doc false
  @spec start_link({{module, module}, binary, [binary], Keyword.t()}) :: GenServer.on_start()
  def start_link({{gen_consumer_module, consumer_module}, group_name, topics, opts}) do
    gen_server_opts = Keyword.get(opts, :gen_server_opts, [])
    args = {{gen_consumer_module, consumer_module}, group_name, topics, Keyword.delete(opts, :gen_server_opts)}
    GenServer.start_link(__MODULE__, args, gen_server_opts)
  end

  @doc """
  Child spec with a `:shutdown` long enough for `terminate/2` to stop the
  consumers (their own `:shutdown`, by default the fetch `:max_wait_time` +
  10 s), after awaiting a heartbeat in flight, and send the leave heartbeat
  (each bounded by `:leave_timeout_ms`).
  """
  @spec child_spec({{module, module}, binary, [binary], Keyword.t()}) :: Supervisor.child_spec()
  def child_spec({_modules, _group_name, _topics, opts} = arg) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [arg]}, shutdown: shutdown_budget(opts)}
  end

  @doc false
  @spec shutdown_budget(Keyword.t()) :: pos_integer()
  def shutdown_budget(opts) do
    leave_timeout = config_value(opts, :leave_timeout_ms)
    # A heartbeat in flight, an assign in flight (bounded by consumer init),
    # then the consumer stops, then the leave.
    2 * (consumer_shutdown(opts) + @terminate_margin) + 2 * leave_timeout + @terminate_margin
  end

  defp consumer_shutdown(opts) do
    case Keyword.get(opts, :shutdown) do
      shutdown when is_integer(shutdown) and shutdown >= 0 ->
        shutdown

      _ ->
        fetch_options = Keyword.get(opts, :fetch_options, [])

        max_wait =
          case Keyword.keyword?(fetch_options) && Keyword.get(fetch_options, :max_wait_time) do
            wait when is_integer(wait) and wait >= 0 -> wait
            _ -> @default_max_wait_time
          end

        max_wait + @consumer_stop_margin
    end
  end

  # ---------------------------------------------------------------------------
  # Init

  @impl true
  def init({{gen_consumer_module, consumer_module}, group_name, topics, opts}) do
    case validate(group_name, topics, opts) do
      {:ok, validated} -> do_init({gen_consumer_module, consumer_module}, group_name, topics, opts, validated)
      {:error, reason} -> {:stop, reason}
    end
  end

  defp validate(group_name, topics, opts) do
    regex = Keyword.get(opts, :subscribed_topic_regex)

    with :ok <- check(is_binary(group_name) and group_name != "", :invalid_consumer_group),
         :ok <- check(is_list(topics) and Enum.all?(topics, &(is_binary(&1) and &1 != "")), :invalid_topics),
         :ok <- check(is_nil(regex) or (is_binary(regex) and regex != ""), :invalid_subscribed_topic_regex),
         :ok <- check(topics != [] or is_binary(regex), :empty_subscription),
         :ok <- check(optional_string?(Keyword.get(opts, :server_assignor)), :invalid_server_assignor),
         :ok <- check(optional_string?(Keyword.get(opts, :rack_id)), :invalid_rack_id),
         :ok <- check(valid_rebalance_timeout?(opts), :invalid_rebalance_timeout_ms),
         :ok <- validate_config(opts),
         {:ok, instance_id} <- resolve_instance_id(opts) do
      {:ok, %{instance_id: instance_id, regex: regex}}
    end
  end

  defp check(true, _error), do: :ok
  defp check(false, error), do: {:error, {:invalid_option, error}}

  defp optional_string?(nil), do: true
  defp optional_string?(value), do: is_binary(value) and value != ""

  defp valid_rebalance_timeout?(opts) do
    case Keyword.get(opts, :rebalance_timeout_ms, @default_rebalance_timeout_ms) do
      ms when is_integer(ms) and ms > 0 and ms <= @max_int32 -> true
      _ -> false
    end
  end

  defp validate_config(opts) do
    Enum.reduce_while(Map.keys(@default_config), :ok, fn key, :ok ->
      case raw_config_value(opts, key) do
        value when is_integer(value) and value >= 0 -> {:cont, :ok}
        _ -> {:halt, {:error, {:invalid_option, :"invalid_#{key}"}}}
      end
    end)
  end

  defp resolve_instance_id(opts) do
    {:ok, opts |> Keyword.get(:group_instance_id) |> Config.resolve_group_instance_id()}
  rescue
    ArgumentError -> {:error, {:invalid_option, :invalid_group_instance_id}}
  end

  defp config_value(opts, key) do
    case Keyword.get(opts, key, Map.fetch!(@default_config, key)) do
      value when is_integer(value) and value >= 0 -> value
      _ -> Map.fetch!(@default_config, key)
    end
  end

  # Validation, unlike config_value/2, sees the raw option.
  defp raw_config_value(opts, key), do: Keyword.get(opts, key, Map.fetch!(@default_config, key))

  @dialyzer {:nowarn_function, do_init: 5}
  defp do_init({gen_consumer_module, consumer_module}, group_name, topics, opts, validated) do
    with {:ok, client, owns_client} <- start_client(opts, group_name, topics),
         :ok <- check_broker_support(client, owns_client, group_name) do
      Process.flag(:trap_exit, true)

      state = %State{
        supervisor_pid: Keyword.fetch!(opts, :supervisor_pid),
        client: client,
        owns_client: owns_client,
        gen_consumer_module: gen_consumer_module,
        consumer_module: consumer_module,
        consumer_opts: Keyword.drop(opts, @manager_keys),
        group_name: group_name,
        topics: topics,
        topic_regex: validated.regex,
        server_assignor: Keyword.get(opts, :server_assignor),
        rebalance_timeout_ms: Keyword.get(opts, :rebalance_timeout_ms, @default_rebalance_timeout_ms),
        instance_id: validated.instance_id,
        rack_id: Keyword.get(opts, :rack_id),
        # KIP-1082: generated once and kept for the member's lifetime,
        # including join retries and rejoins (Java: memberId is final).
        member_id: KafkaExAPI.generate_member_id(),
        config: Map.new(@default_config, fn {key, default} -> {key, Keyword.get(opts, key, default)} end)
      }

      {:ok, state, {:continue, :start}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  # Fail fast, with no silent fall back to the classic protocol, when the
  # broker does not advertise ConsumerGroupHeartbeat (Kafka < 4.0): the
  # group fails to start with :consumer_group_protocol_not_supported. A
  # Kafka 4.x broker with "consumer" missing from
  # group.coordinator.rebalance.protocols still advertises the API (verified
  # on 4.4.0) and answers the join with UNSUPPORTED_VERSION, which is
  # terminal (heartbeat_error/2): the group stops at its first heartbeat, as
  # Java's does. A probe that itself fails (an error reply, or an exit
  # because the client call timed out or the client died) decides nothing;
  # the join's own error then does. The probe goes to any broker (as
  # ApiVersions does), so during a rolling broker upgrade from 3.x it can
  # reach a not-yet-upgraded broker and refuse to start; start the group
  # once every broker runs 4.x.
  defp check_broker_support(client, owns_client, group_name) do
    with {:ok, %ApiVersions{} = versions} <- KafkaExAPI.api_versions(client),
         {:error, :unsupported_api} <- ApiVersions.max_version_for_api(versions, @heartbeat_api_key) do
      Logger.error(
        "Consumer group #{inspect(group_name)}: #{@protocol_not_supported_msg} " <>
          "(the broker does not advertise ConsumerGroupHeartbeat)"
      )

      if owns_client, do: stop_client(%State{owns_client: true, client: client})
      {:error, :consumer_group_protocol_not_supported}
    else
      _ -> :ok
    end
  catch
    :exit, _ -> :ok
  end

  # A pre-supplied `:client` (tests) is used as is and never stopped.
  defp start_client(opts, group_name, topics) do
    case Keyword.get(opts, :client) do
      nil ->
        client_opts = Manager.group_client_opts(opts, group_name, topics)

        case Client.start_link(client_opts, :no_name) do
          {:ok, client} -> {:ok, client, true}
          {:error, reason} -> {:error, reason}
        end

      client when is_pid(client) ->
        {:ok, client, false}
    end
  end

  @impl true
  def handle_continue(:start, %State{} = state) do
    # Not in init/1: the consumer group supervisor is still starting us and
    # would not answer start_child until init/1 returns.
    {:ok, consumer_supervisor} =
      ConsumerGroup.start_consumer(
        state.supervisor_pid,
        {state.gen_consumer_module, state.consumer_module},
        state.group_name,
        [],
        state.consumer_opts
      )

    {:noreply, schedule_heartbeat(%State{state | consumer_supervisor: consumer_supervisor}, 0)}
  end

  # ---------------------------------------------------------------------------
  # Introspection (the calls KafkaEx.Consumer.ConsumerGroup makes)

  @impl true
  def handle_call(:member_id, _from, state), do: {:reply, state.member_id, state}
  def handle_call(:member_epoch, _from, state), do: {:reply, state.member_epoch, state}
  # There is no generation in the consumer protocol; the member epoch is the
  # closest equivalent (it moves whenever the member's assignment does).
  def handle_call(:generation_id, _from, state), do: {:reply, state.member_epoch, state}
  def handle_call(:leader_id, _from, state), do: {:reply, nil, state}
  def handle_call(:am_leader, _from, state), do: {:reply, false, state}
  def handle_call(:group_name, _from, state), do: {:reply, state.group_name, state}
  def handle_call(:consumer_supervisor_pid, _from, state), do: {:reply, state.consumer_supervisor, state}

  def handle_call(:assignments, _from, state) do
    assignments = state.owned |> Enum.map(fn {{_id, partition}, entry} -> {entry.topic, partition} end) |> Enum.sort()
    {:reply, assignments, state}
  end

  # A consumer's commit or offset fetch was rejected as an unknown or fenced
  # member. Only the heartbeat knows the member's real state (the consumer
  # may hold an old epoch), so ask now; it fences the member if it was.
  @impl true
  def handle_cast({:rejoin_required, reason, epoch}, %State{} = state) do
    Logger.info(
      "Consumer of group #{state.group_name} reported #{inspect(reason)} (member_epoch=#{inspect(epoch)}); " <>
        "checking the membership with a heartbeat"
    )

    {:noreply, request_heartbeat(state)}
  end

  def handle_cast(_msg, state), do: {:noreply, state}

  # A heartbeat as soon as possible: now, or right after the one in flight.
  # While fenced heartbeat/1 sends nothing (the rejoin follows the release of
  # the partitions).
  defp request_heartbeat(%State{inflight: nil} = state), do: schedule_heartbeat(state, 0)
  defp request_heartbeat(%State{} = state), do: %State{state | heartbeat_pending: true}

  # ---------------------------------------------------------------------------
  # Messages

  @impl true
  def handle_info({:heartbeat, ref}, %State{heartbeat_timer: {ref, _timer}} = state) do
    heartbeat(%State{state | heartbeat_timer: nil})
  end

  def handle_info({:heartbeat, _stale}, state), do: {:noreply, state}

  # An async worker's result (see run_async/1) completes it; its :DOWN is
  # then flushed. A :DOWN first means the worker died without a result.
  def handle_info({@async_result, ref, result}, %State{} = state) when is_reference(ref) do
    async_finished(state, ref, {:op_result, result})
  end

  # One consumer of the assign in flight has started: own and monitor it now,
  # not once the whole batch has (an early exit would otherwise be seen as
  # :noproc and lose its reason). The worker sends these before its result.
  def handle_info({@async_started, ref, key, result}, %State{op: %{kind: :assign, ref: ref} = op} = state) do
    {:noreply, apply_start(state, key, new_entry(op.names[key]), result, op.epoch)}
  end

  def handle_info({@async_started, _stale, _key, _result}, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = state)
      when (is_map(state.inflight) and state.inflight.ref == ref) or (is_map(state.op) and state.op.ref == ref) or
             (is_map(state.push) and state.push.ref == ref) or (is_map(state.lookup) and state.lookup.ref == ref) or
             is_map_key(state.starts, ref) do
    async_finished(state, ref, reason)
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, %State{} = state) do
    case Enum.find(state.owned, fn {_key, entry} -> entry.ref == ref end) do
      {key, entry} -> consumer_down(state, key, %{entry | pid: pid}, reason)
      nil -> {:noreply, state}
    end
  end

  def handle_info({:restart_partition, key, token}, %State{} = state) do
    case state.owned do
      # Only while the partition is still in the target: one that is being
      # revoked is left to the revoke (it drops entries that are not running).
      # A partition out of the target now may come back before that revoke
      # runs (it waits for an op in flight): the restart is then kept due
      # (token :due) and step/1 starts it once the partition is back.
      %{^key => %{status: :restarting, token: ^token} = entry} when state.phase == :stable ->
        if in_target?(state, key),
          do: {:noreply, start_restart(state, key, %{entry | token: nil})},
          else: {:noreply, %State{state | owned: Map.put(state.owned, key, %{entry | token: :due})}}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:resolve_topics, ref}, %State{resolve_timer: {ref, _}} = state) do
    step(%State{state | resolve_timer: nil})
  end

  def handle_info({:resolve_topics, _stale}, state), do: {:noreply, state}

  def handle_info(:step, state), do: step(state)

  def handle_info({:EXIT, pid, reason}, %State{client: pid} = state) do
    Logger.error("Consumer protocol manager's Kafka client died: #{inspect(reason)}")
    {:stop, {:shutdown, {:client_died, reason}}, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Heartbeat

  defp heartbeat(%State{phase: :fenced} = state), do: {:noreply, state}

  defp heartbeat(%State{inflight: inflight} = state) when inflight != nil,
    do: {:noreply, %State{state | heartbeat_pending: true}}

  defp heartbeat(%State{} = state) do
    {opts, partitions_sent} = heartbeat_opts(state)
    client = state.client
    group = state.group_name

    {pid, ref} = run_async(fn -> send_heartbeat(client, group, opts) end)

    inflight = %{pid: pid, ref: ref, partitions: partitions_sent, epoch: opts[:member_epoch]}
    join_sent = state.join_sent or opts[:member_epoch] == 0
    {:noreply, %State{state | inflight: inflight, heartbeat_pending: false, join_sent: join_sent}}
  end

  # The phase cannot change while a heartbeat is in flight (only one is in
  # flight, and only its reply moves the phase; none is sent while fenced),
  # so a reply always describes the current membership.
  defp heartbeat_done(%State{} = state, inflight, result) do
    case result do
      {:ok, %ConsumerGroupHeartbeat{} = response} -> heartbeat_ok(state, inflight.partitions, response)
      {:error, {:heartbeat_process_died, _} = reason} -> heartbeat_error(state, reason)
      {:error, reason} -> heartbeat_error(state, reason)
    end
  end

  # The client call can exit (call budget exceeded, client gone): a transient
  # failure here, not a crash of the member.
  defp send_heartbeat(client, group, opts) do
    KafkaExAPI.consumer_group_heartbeat(client, group, opts)
  catch
    :exit, {:timeout, _} -> {:error, :timeout}
    :exit, _ -> {:error, :not_connected}
  end

  @doc false
  # The ConsumerGroupHeartbeat options for the next heartbeat, and the
  # topic_partitions set they carry (nil when left out = unchanged). Mirrors
  # Java ConsumerHeartbeatRequestManager.HeartbeatState.buildRequestData:
  # instance id always; subscription, rebalance timeout, assignor and rack id
  # only on a full request (join, after an error); owned partitions when they
  # changed.
  @spec heartbeat_opts(State.t()) :: {Keyword.t(), MapSet.t() | nil}
  def heartbeat_opts(%State{} = state) do
    joining = state.phase == :joining
    full = joining or state.send_full
    partitions = if joining, do: MapSet.new(), else: state.reported
    send_partitions = full or partitions != state.sent_partitions

    opts =
      [member_id: state.member_id, member_epoch: if(joining, do: 0, else: state.member_epoch)]
      |> put_present(:instance_id, state.instance_id)
      |> put_when(full, :rebalance_timeout_ms, state.rebalance_timeout_ms)
      |> put_when(full and (state.topics != [] or is_nil(state.topic_regex)), :subscribed_topic_names, state.topics)
      |> put_when(full, :subscribed_topic_regex, state.topic_regex)
      |> put_when(full, :server_assignor, state.server_assignor)
      |> put_when(full, :rack_id, state.rack_id)
      |> put_when(send_partitions, :topic_partitions, encode_partitions(partitions))

    {opts, if(send_partitions, do: partitions)}
  end

  defp put_present(opts, _key, nil), do: opts
  defp put_present(opts, key, value), do: opts ++ [{key, value}]

  defp put_when(opts, true, key, value), do: put_present(opts, key, value)
  defp put_when(opts, false, _key, _value), do: opts

  defp encode_partitions(keys) do
    keys
    |> Enum.group_by(fn {topic_id, _} -> topic_id end, fn {_, partition} -> partition end)
    |> Enum.sort()
    |> Enum.map(fn {topic_id, partitions} -> %{topic_id: topic_id, partitions: Enum.sort(partitions)} end)
  end

  defp heartbeat_ok(%State{} = state, partitions_sent, %ConsumerGroupHeartbeat{} = result) do
    joined = state.phase == :joining

    state = %State{
      state
      | phase: :stable,
        member_id: result.member_id || state.member_id,
        member_epoch: result.member_epoch,
        heartbeat_interval_ms: result.heartbeat_interval_ms,
        heartbeat_failures: 0,
        send_full: false,
        sent_partitions: partitions_sent || state.sent_partitions
    }

    if joined do
      Logger.info("Joined consumer group #{state.group_name} as #{state.member_id} (epoch #{state.member_epoch})")
      Telemetry.emit_member_joined(state.group_name, state.member_id, state.member_epoch)
    end

    interval = max(result.heartbeat_interval_ms, 0)
    # A heartbeat requested while this one was in flight goes out now.
    delay = if state.heartbeat_pending, do: 0, else: interval

    %State{state | heartbeat_pending: false}
    |> new_target(result.assignment)
    |> schedule_heartbeat(delay)
    |> step()
  end

  defp new_target(state, nil), do: state

  defp new_target(%State{} = state, assignment) do
    names =
      for %TopicPartitions{topic_id: <<_::128>> = id, topic_name: name} <- assignment,
          is_binary(name),
          into: %{},
          do: {id, name}

    target =
      for %TopicPartitions{topic_id: <<_::128>> = id, partitions: partitions} <- assignment,
          partition <- partitions,
          into: MapSet.new(),
          do: {id, partition}

    %State{state | target: target, topic_names: Map.merge(state.topic_names, names), resolve_failures: 0}
  end

  defp heartbeat_error(%State{} = state, reason) do
    cond do
      Retry.consumer_group_heartbeat_rejoin_error?(reason) ->
        fence(state, reason)

      # Java 4.4 AbstractHeartbeatRequestManager.onErrorResponse reports
      # TOPIC_AUTHORIZATION_FAILED to the application (an ErrorEvent) and
      # keeps the member: no handleFatalFailure. One unauthorized topic (e.g.
      # matched by a regex subscription) must not stop the group. Logged as an
      # error and retried like a transient failure.
      reason == :topic_authorization_failed ->
        Logger.error(
          "ConsumerGroupHeartbeat for #{state.group_name} failed with :topic_authorization_failed; " <>
            "the member stays in the group and retries"
        )

        retry_heartbeat(state, reason)

      Retry.consumer_group_heartbeat_retryable?(reason) or match?({:heartbeat_process_died, _}, reason) ->
        retry_heartbeat(state, reason)

      true ->
        Logger.error(
          "ConsumerGroupHeartbeat for #{state.group_name} failed with terminal #{inspect(reason)}" <>
            protocol_hint(reason)
        )

        {:stop, {:shutdown, {:terminal, reason}}, %State{state | fatal: true}}
    end
  end

  # Java (AbstractHeartbeatRequestManager) reports an UNSUPPORTED_VERSION
  # heartbeat with CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG: the protocol is
  # disabled on a broker that still advertises the API, or the start-up
  # check could not reach a broker.
  defp protocol_hint(reason) when reason in [:api_not_supported_by_broker, :unsupported_version],
    do: ": " <> @protocol_maybe_not_supported_msg

  defp protocol_hint(_reason), do: ""

  defp retry_heartbeat(%State{} = state, reason) do
    failures = state.heartbeat_failures + 1

    delay =
      Retry.backoff_delay(
        failures - 1,
        state.config.heartbeat_retry_backoff_ms,
        state.config.heartbeat_retry_max_backoff_ms
      )

    Logger.warning(
      "ConsumerGroupHeartbeat for #{state.group_name} failed with #{inspect(reason)}; " <>
        "retrying in #{delay} ms (attempt #{failures})"
    )

    {:noreply, %State{state | heartbeat_pending: false} |> reset_sent(failures) |> schedule_heartbeat(delay)}
  end

  # Java resets the sent fields on every failed heartbeat, so the next one is
  # a full request.
  defp reset_sent(%State{} = state, failures),
    do: %State{state | heartbeat_failures: failures, send_full: true, sent_partitions: nil}

  # Keeps the earlier of the pending and the new deadline: a heartbeat asked
  # for "now" (a report, a consumer's rejoin_required) must not be pushed
  # back by the interval timer a reply that was already queued sets.
  # Fencing and shutdown cancel the timer explicitly.
  defp schedule_heartbeat(%State{} = state, delay) do
    case state.heartbeat_timer do
      {_ref, timer} ->
        case Process.read_timer(timer) do
          # Fired, its message not handled yet (handling it clears the
          # timer): a heartbeat is about to go out.
          false ->
            state

          remaining when remaining <= delay ->
            state

          _ ->
            Process.cancel_timer(timer)
            start_heartbeat_timer(state, delay)
        end

      nil ->
        start_heartbeat_timer(state, delay)
    end
  end

  defp start_heartbeat_timer(%State{} = state, delay) do
    ref = make_ref()
    %State{state | heartbeat_timer: {ref, Process.send_after(self(), {:heartbeat, ref}, delay)}}
  end

  defp cancel_heartbeat(%State{heartbeat_timer: {_ref, timer}} = state) do
    Process.cancel_timer(timer)
    %State{state | heartbeat_timer: nil}
  end

  defp cancel_heartbeat(state), do: state

  # ---------------------------------------------------------------------------
  # Fencing (Java AbstractMembershipManager.transitionToFenced)

  defp fence(%State{phase: :joining} = state, reason) do
    # Not a member yet: nothing to release. Retry the join (same member id).
    failures = state.heartbeat_failures + 1

    delay =
      Retry.backoff_delay(
        failures - 1,
        state.config.heartbeat_retry_backoff_ms,
        state.config.heartbeat_retry_max_backoff_ms
      )

    Logger.warning("Join of #{state.group_name} rejected with #{inspect(reason)}; rejoining in #{delay} ms")
    {:noreply, state |> reset_sent(failures) |> schedule_heartbeat(delay)}
  end

  defp fence(%State{} = state, reason) do
    lost = map_size(state.owned)

    Logger.warning(
      "Member #{state.member_id} of #{state.group_name} fenced (#{inspect(reason)}, epoch #{state.member_epoch}); " <>
        "releasing #{lost} partition(s) without committing and rejoining"
    )

    Telemetry.emit_member_fenced(state.group_name, state.member_id, state.member_epoch, reason, lost)
    %State{} = state = state |> cancel_heartbeat() |> cancel_resolve() |> abort_lookup()
    state = %State{state | phase: :fenced, reconcile: nil}
    step(abort_push(state))
  end

  # An epoch push is useless once fenced, and may be stuck behind a fetch
  # long-poll: drop it.
  defp abort_push(%State{push: %{pid: pid, ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Process.exit(pid, :kill)
    %State{state | push: nil}
  end

  defp abort_push(state), do: state

  defp abort_inflight(%State{inflight: %{pid: pid, ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Process.exit(pid, :kill)
    %State{state | inflight: nil}
  end

  defp abort_inflight(state), do: state

  defp rejoin(%State{} = state) do
    state = %State{
      state
      | phase: :joining,
        member_epoch: 0,
        # The broker removed the fenced member: until the rejoin is sent
        # there is nothing to leave.
        join_sent: false,
        owned: %{},
        reported: MapSet.new(),
        target: nil,
        sent_partitions: nil,
        send_full: true,
        reconcile: nil
    }

    {:noreply, schedule_heartbeat(state, 0)}
  end

  # ---------------------------------------------------------------------------
  # Reconciliation

  # One step at a time: an async operation (revoke, assign, lose) ends with
  # step/1 again, which then works on the newest target. Order: revoke, then
  # assign, then push the current epoch to the partitions kept, then
  # acknowledge. A push in flight does not hold back a revoke or an assign
  # (it can wait behind a fetch long-poll); the acknowledgement waits for it.
  defp step(%State{op: op} = state) when op != nil, do: {:noreply, state}

  # A restart in flight is waited for (restart_done/3 runs step/1 again):
  # rejoining meanwhile would leave its consumer running at the old epoch.
  defp step(%State{phase: :fenced} = state) do
    case running_keys(state) do
      [] when map_size(state.starts) > 0 -> {:noreply, state}
      [] -> rejoin(state)
      keys -> {:noreply, start_stop_op(state, :lose, keys)}
    end
  end

  defp step(%State{phase: :stable, target: %MapSet{}} = state) do
    state = state |> resolve_topic_names() |> start_due_restarts()
    revoked = state.owned |> Map.keys() |> Enum.reject(&MapSet.member?(state.target, &1))

    cond do
      revoked == [] ->
        assign_or_push(state, pending_assign(state))

      # A revoked partition whose consumer is being restarted: wait for the
      # start (restart_done/3 runs step/1 again), then stop it. Reporting it
      # revoked earlier could let its new owner consume it alongside.
      Enum.any?(revoked, &(state.owned[&1].status == :starting)) ->
        {:noreply, state}

      true ->
        revoke(begin_reconcile(state), revoked)
    end
  end

  defp step(state), do: {:noreply, state}

  defp assign_or_push(%State{push: push} = state, []) when push != nil, do: {:noreply, state}

  defp assign_or_push(state, []) do
    case stale_epoch_keys(state) do
      [] -> {:noreply, complete_reconciliation(state)}
      stale -> {:noreply, start_push(state, stale)}
    end
  end

  # Revoke first: partitions are only assigned once the broker has the
  # reduced owned set (no report pending, none in flight carrying a larger
  # set). heartbeat_done/3 runs step/1 again.
  defp assign_or_push(state, added) do
    if revocation_reported?(state),
      do: {:noreply, start_assign(begin_reconcile(state), added)},
      else: {:noreply, state}
  end

  defp revocation_reported?(%State{reported: reported, sent_partitions: sent, inflight: inflight}) do
    in_flight_ok = is_nil(inflight) or is_nil(inflight.partitions) or MapSet.subset?(inflight.partitions, reported)
    in_flight_ok and (is_nil(sent) or MapSet.subset?(sent, reported))
  end

  defp begin_reconcile(%State{reconcile: nil} = state),
    do: %State{state | reconcile: %{started_at: System.monotonic_time(), revoked: 0, assigned: 0}}

  defp begin_reconcile(state), do: state

  defp running_keys(state), do: for({key, %{status: :running}} <- state.owned, do: key)

  # Restarts whose timer fired while their partition was out of the target
  # (see handle_info({:restart_partition, ...})) and that are back in it.
  # Without this such a partition would stay :restarting, with no consumer,
  # yet be reported as owned.
  defp start_due_restarts(%State{} = state) do
    for {key, %{status: :restarting, token: :due} = entry} <- state.owned,
        in_target?(state, key),
        reduce: state,
        do: (acc -> start_restart(acc, key, %{entry | token: nil}))
  end

  defp in_target?(%State{target: %MapSet{} = target}, key), do: MapSet.member?(target, key)
  defp in_target?(_state, _key), do: false

  # Target partitions without a consumer whose topic name is known.
  defp pending_assign(state) do
    state.target
    |> Enum.reject(&Map.has_key?(state.owned, &1))
    |> Enum.filter(fn {topic_id, _} -> Map.has_key?(state.topic_names, topic_id) end)
    |> Enum.sort()
  end

  defp stale_epoch_keys(state) do
    now = now_ms()

    for {key, %{status: :running, epoch: epoch} = entry} <- state.owned,
        epoch != state.member_epoch,
        is_nil(entry.push_after) or entry.push_after <= now,
        do: key
  end

  defp complete_reconciliation(%State{} = state) do
    owned = state.owned |> Map.keys() |> MapSet.new()

    if state.reconcile do
      %{started_at: started_at, revoked: revoked, assigned: assigned} = state.reconcile
      duration = System.monotonic_time() - started_at

      Logger.info(
        "Member #{state.member_id} of #{state.group_name} reconciled epoch #{state.member_epoch}: " <>
          "#{revoked} revoked, #{assigned} assigned, #{MapSet.size(owned)} owned"
      )

      Telemetry.emit_reconcile(state.group_name, state.member_id, state.member_epoch, %{
        revoked: revoked,
        assigned: assigned,
        duration: duration
      })
    end

    state = %State{state | reconcile: nil, reported: owned}
    state = maybe_schedule_resolve(state)
    if owned != state.sent_partitions, do: schedule_heartbeat(state, 0), else: state
  end

  # -- topic names ------------------------------------------------------------

  defp unresolved_topic_ids(%State{target: nil}), do: []

  defp unresolved_topic_ids(state) do
    state.target
    |> Enum.map(fn {topic_id, _} -> topic_id end)
    |> Enum.uniq()
    |> Enum.reject(&Map.has_key?(state.topic_names, &1))
  end

  # Starts a lookup of the unknown topic ids by id (Metadata v12+), at most
  # one at a time and at most once per backoff period (resolve_timer set =
  # waiting). It runs in a monitored process (see run_async/1): the client
  # call can take its whole call budget, longer than the session timeout, and
  # the manager keeps heartbeating, handling consumer exits and shutting down
  # meanwhile. The step goes on with the names already known; lookup_done/2
  # runs it again with the new ones.
  defp resolve_topic_names(%State{resolve_timer: nil, lookup: nil} = state) do
    case unresolved_topic_ids(state) do
      [] ->
        state

      ids ->
        client = state.client
        {pid, ref} = run_async(fn -> lookup_topic_names(client, ids) end)
        %State{state | lookup: %{pid: pid, ref: ref, ids: ids}}
    end
  end

  defp resolve_topic_names(state), do: state

  # A worker that died without a result found no names. Ids still unnamed
  # are looked up again after the backoff.
  defp lookup_done(%State{} = state, outcome) do
    names =
      case outcome do
        {:op_result, names} when is_map(names) -> names
        _ -> %{}
      end

    state
    |> Map.put(:topic_names, Map.merge(state.topic_names, names))
    |> maybe_schedule_resolve()
    |> step()
  end

  defp abort_lookup(%State{lookup: %{pid: pid, ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    Process.exit(pid, :kill)
    %State{state | lookup: nil}
  end

  defp abort_lookup(state), do: state

  defp lookup_topic_names(client, ids) do
    case KafkaExAPI.metadata(client, topic_ids: ids) do
      {:ok, %ClusterMetadata{} = metadata} ->
        names_from(metadata, ids)

      # Any failed id (unknown, or e.g. :topic_authorization_failed) fails the
      # whole request, and the top-level error is only the first failure's
      # code: ask again for the others, whatever the code, so one
      # unauthorized topic does not keep the rest unnamed (and unconsumed).
      {:error, %{error: error, metadata: %{topic_ids: failed}}} when is_list(failed) ->
        if error != :unknown_topic_id,
          do: Logger.warning("Metadata lookup of #{length(failed)} topic id(s) failed with #{inspect(error)}")

        case ids -- failed do
          [] -> %{}
          rest when rest != ids -> lookup_topic_names(client, rest)
          _ -> %{}
        end

      {:error, reason} ->
        Logger.warning("Metadata lookup of #{length(ids)} topic id(s) failed: #{inspect(reason)}")
        %{}
    end
  catch
    :exit, reason ->
      Logger.warning("Metadata lookup of #{length(ids)} topic id(s) failed: #{inspect(reason)}")
      %{}
  end

  defp names_from(metadata, ids) do
    for id <- ids, {:ok, name} <- [ClusterMetadata.topic_name_by_id(metadata, id)], into: %{}, do: {id, name}
  end

  # A lookup in flight decides when it ends (lookup_done/2).
  defp maybe_schedule_resolve(%State{lookup: lookup} = state) when lookup != nil, do: state

  defp maybe_schedule_resolve(%State{resolve_timer: nil} = state) do
    case unresolved_topic_ids(state) do
      [] ->
        %State{state | resolve_failures: 0}

      ids ->
        failures = state.resolve_failures + 1

        delay =
          Retry.backoff_delay(
            failures - 1,
            state.config.topic_resolution_backoff_ms,
            state.config.topic_resolution_max_backoff_ms
          )

        Logger.warning(
          "#{length(ids)} assigned topic id(s) of #{state.group_name} have no known name yet; " <>
            "not consuming them, retrying the lookup in #{delay} ms"
        )

        ref = make_ref()
        timer = Process.send_after(self(), {:resolve_topics, ref}, delay)
        %State{state | resolve_timer: {ref, timer}, resolve_failures: failures}
    end
  end

  defp maybe_schedule_resolve(state), do: state

  defp cancel_resolve(%State{resolve_timer: {_ref, timer}} = state) do
    Process.cancel_timer(timer)
    %State{state | resolve_timer: nil}
  end

  defp cancel_resolve(state), do: state

  # -- revoke -----------------------------------------------------------------

  defp revoke(%State{} = state, keys) do
    {running, inactive} = Enum.split_with(keys, &(state.owned[&1].status == :running))
    # Not running (restarting or stopped): nothing to stop. A pending restart
    # timer for one of them finds no entry (or a newer token) and is ignored.
    state = %State{state | owned: Map.drop(state.owned, inactive), reconcile: count(state.reconcile, :revoked, keys)}

    case running do
      [] -> revoked(state, inactive)
      _ -> {:noreply, start_stop_op(%State{state | revoking: inactive}, :revoke, running)}
    end
  end

  defp count(reconcile, key, keys), do: Map.update!(reconcile, key, &(&1 + length(keys)))

  # Stops consumers concurrently (GenConsumer.Supervisor.stop_partitions/3) in
  # a monitored process; its exit reason carries the result.
  defp start_stop_op(%State{} = state, kind, keys) do
    for key <- keys, %{ref: ref} when is_reference(ref) <- [state.owned[key]], do: Process.demonitor(ref, [:flush])
    topic_partitions = Enum.map(keys, &topic_partition(state, &1))
    supervisor = state.consumer_supervisor
    stop_opts = if kind == :lose, do: [lost: true], else: []

    {pid, ref} =
      run_async(fn -> GenConsumerSupervisor.stop_partitions(supervisor, topic_partitions, stop_opts) end)

    %State{state | op: %{kind: kind, pid: pid, ref: ref, keys: keys}}
  end

  defp revoked(%State{} = state, keys) do
    state = %State{state | reported: MapSet.difference(state.reported, MapSet.new(keys))}
    # Report the reduced owned set now (revoke first, then assign), so the
    # broker can hand the partitions to their new owners without waiting for
    # this member's assignments. Java reports once, after revoking and
    # assigning; reporting early is allowed (KIP-848) and shortens the time
    # the revoked partitions are not consumed by anyone.
    state =
      if state.phase == :stable and state.reported != state.sent_partitions,
        do: schedule_heartbeat(state, 0),
        else: state

    step(state)
  end

  # -- assign -----------------------------------------------------------------

  defp start_assign(%State{} = state, keys) do
    starts = Enum.map(keys, &{&1, topic_partition_for(state, &1)})
    supervisor = state.consumer_supervisor
    membership = start_membership(state)
    owner = self()

    # Each result is sent as soon as its start returns (ordered: false), from
    # this worker, so all of them arrive before its own result.
    {pid, ref} =
      run_async(fn ref ->
        starts
        |> Task.async_stream(
          fn {key, tp} -> {key, GenConsumerSupervisor.start_partition(supervisor, tp, membership)} end,
          max_concurrency: @start_concurrency,
          timeout: :infinity,
          ordered: false
        )
        |> Enum.each(fn {:ok, {key, result}} -> send(owner, {@async_started, ref, key, result}) end)
      end)

    names = Map.new(starts, fn {key, {topic, _}} -> {key, topic} end)
    %State{state | op: %{kind: :assign, pid: pid, ref: ref, keys: keys, epoch: state.member_epoch, names: names}}
  end

  defp start_membership(state),
    do: [member_id: state.member_id, member_epoch: state.member_epoch, group_manager_pid: self()]

  # Restarts a crashed consumer in a monitored process: the start runs the
  # consumer's whole init inside the consumer supervisor (its own client
  # connecting to every bootstrap broker), possibly queued behind an assign,
  # and the manager keeps heartbeating meanwhile. restart_done/3 owns the
  # result.
  defp start_restart(%State{} = state, key, entry) do
    tp = {entry.topic, elem(key, 1)}
    supervisor = state.consumer_supervisor
    membership = start_membership(state)
    {pid, ref} = run_async(fn -> GenConsumerSupervisor.start_partition(supervisor, tp, membership) end)
    start = %{pid: pid, key: key, epoch: state.member_epoch}

    %State{
      state
      | owned: Map.put(state.owned, key, %{entry | status: :starting}),
        starts: Map.put(state.starts, ref, start)
    }
  end

  # A revoke or a fence waits for a restart in flight, so the entry is still
  # :starting here.
  defp restart_done(%State{} = state, %{key: key, epoch: epoch}, outcome) do
    case state.owned do
      %{^key => %{status: :starting} = entry} ->
        result = restart_result(state, entry, key, outcome)
        step(apply_start(state, key, entry, result, epoch))

      _ ->
        {:noreply, state}
    end
  end

  # The start process itself died (it should not): whatever the supervisor
  # started is adopted, else the restart is retried.
  defp restart_result(_state, _entry, _key, {:op_result, result}), do: result

  defp restart_result(state, entry, key, reason) do
    case GenConsumerSupervisor.lookup_partition(state.consumer_supervisor, {entry.topic, elem(key, 1)}) do
      {:ok, pid} -> {:ok, pid}
      :error -> {:error, {:start_process_died, reason}}
    end
  end

  defp apply_start(%State{} = state, key, entry, result, epoch) do
    case result do
      {:ok, pid} ->
        %State{state | owned: Map.put(state.owned, key, running_entry(entry, pid, epoch))}

      {:error, {:already_started, pid}} ->
        %State{state | owned: Map.put(state.owned, key, running_entry(entry, pid, epoch))}

      # A failed start is not uptime: started_at is moved to now, so the
      # backoff keeps doubling while starts keep failing instead of falling
      # back to the base delay once the last good start is a minute old.
      {:error, reason} ->
        Logger.warning("Starting the consumer of #{inspect({entry.topic, elem(key, 1)})} failed: #{inspect(reason)}")
        schedule_restart(state, key, %{entry | started_at: now_ms()})
    end
  end

  defp running_entry(entry, pid, epoch) do
    %{
      entry
      | pid: pid,
        ref: Process.monitor(pid),
        epoch: epoch,
        status: :running,
        started_at: now_ms(),
        push_after: nil
    }
  end

  defp new_entry(topic),
    do: %{
      topic: topic,
      pid: nil,
      ref: nil,
      epoch: nil,
      status: :restarting,
      crashes: 0,
      started_at: now_ms(),
      token: nil,
      push_after: nil
    }

  # -- epoch push -------------------------------------------------------------

  defp start_push(%State{} = state, keys) do
    epoch = state.member_epoch
    timeout = state.config.membership_update_timeout_ms
    targets = Enum.map(keys, &{&1, state.owned[&1].pid})

    {pid, ref} =
      run_async(fn ->
        targets
        |> Task.async_stream(
          fn {key, pid} -> {key, GenConsumer.update_membership(pid, [member_epoch: epoch], timeout)} end,
          max_concurrency: max(length(targets), 1),
          timeout: :infinity
        )
        |> Map.new(fn {:ok, result} -> result end)
      end)

    %State{state | push: %{pid: pid, ref: ref, epoch: epoch, targets: Map.new(targets)}}
  end

  # -- operation results ------------------------------------------------------

  defp op_done(%State{} = state, %{kind: kind} = op, {:op_result, result}) when kind in [:revoke, :lose] do
    log_stop_failures(state, result)

    case kind do
      :revoke ->
        # Also the revoked partitions that had no running consumer.
        keys = op.keys ++ state.revoking
        revoked(%State{state | owned: Map.drop(state.owned, op.keys), revoking: []}, keys)

      :lose ->
        step(%State{state | owned: Map.drop(state.owned, op.keys)})
    end
  end

  # Every start was already applied as it returned (the {@async_started, ...}
  # messages arrive before the worker's result).
  defp op_done(%State{} = state, %{kind: :assign} = op, {:op_result, _}) do
    step(%State{state | reconcile: state.reconcile && count(state.reconcile, :assigned, op.keys)})
  end

  # The operation process itself died (it should not). Re-read what is
  # running from the consumer supervisor and carry on from there.
  defp op_done(state, op, reason) do
    Logger.error("Consumer #{op.kind} operation for #{state.group_name} died: #{inspect(reason)}")
    step(readopt(state, op))
  end

  # A consumer that was restarted or stopped meanwhile (another pid) keeps
  # its own epoch. One whose update failed (e.g. :timeout behind a long
  # fetch) is retried after a backoff; it does not hold back the
  # acknowledgement.
  defp push_done(%State{} = state, push, {:op_result, results}) do
    retry_at = now_ms() + state.config.partition_restart_backoff_ms

    owned =
      Enum.reduce(results, state.owned, fn {key, result}, owned ->
        case owned do
          %{^key => %{pid: pid} = entry} when pid == :erlang.map_get(key, push.targets) ->
            Map.put(owned, key, pushed(entry, result, push.epoch, retry_at))

          _ ->
            owned
        end
      end)

    if Enum.any?(results, fn {_key, result} -> result != :ok end),
      do: Process.send_after(self(), :step, state.config.partition_restart_backoff_ms)

    step(%State{state | owned: owned})
  end

  defp push_done(%State{} = state, push, reason) do
    Logger.error("Member epoch update for #{state.group_name} died: #{inspect(reason)}")
    retry_at = now_ms() + state.config.partition_restart_backoff_ms

    owned =
      Enum.reduce(push.targets, state.owned, fn {key, _pid}, owned ->
        case owned do
          %{^key => entry} -> Map.put(owned, key, %{entry | push_after: retry_at})
          _ -> owned
        end
      end)

    Process.send_after(self(), :step, state.config.partition_restart_backoff_ms)
    step(%State{state | owned: owned})
  end

  defp pushed(entry, :ok, epoch, _retry_at), do: %{entry | epoch: epoch, push_after: nil}
  defp pushed(entry, _error, _epoch, retry_at), do: %{entry | push_after: retry_at}

  # An assign's consumers that already started are owned and monitored.
  defp readopt(state, op) do
    keys = if op.kind == :assign, do: Enum.reject(op.keys, &Map.has_key?(state.owned, &1)), else: op.keys

    Enum.reduce(keys, state, fn key, %State{} = acc ->
      entry = acc.owned[key] || new_entry(op[:names][key])
      tp = {entry.topic, elem(key, 1)}

      case GenConsumerSupervisor.lookup_partition(acc.consumer_supervisor, tp) do
        {:ok, pid} -> %State{acc | owned: Map.put(acc.owned, key, running_entry(entry, pid, entry.epoch || op[:epoch]))}
        :error -> %State{acc | owned: Map.delete(acc.owned, key)}
      end
    end)
  end

  defp log_stop_failures(state, result) when is_map(result) do
    for {tp, {:error, reason}} <- result, reason != :not_found do
      Logger.warning("Stopping the consumer of #{inspect(tp)} in #{state.group_name} failed: #{inspect(reason)}")
    end

    :ok
  end

  defp log_stop_failures(_state, _result), do: :ok

  # ---------------------------------------------------------------------------
  # Consumer exits

  defp consumer_down(%State{} = state, key, entry, reason) do
    tp = {entry.topic, elem(key, 1)}

    case reason do
      {:shutdown, {:terminal_error, error}} ->
        Logger.error("Consumer of #{inspect(tp)} stopped with terminal error #{inspect(error)}; stopping the member")
        {:stop, {:shutdown, {:terminal, error}}, %State{state | owned: Map.delete(state.owned, key)}}

      {:shutdown, {:rejoin_required, error}} ->
        Logger.warning("Consumer of #{inspect(tp)} was rejected as #{inspect(error)}; restarting it after a heartbeat")
        {:noreply, state |> schedule_restart(key, entry) |> request_heartbeat()}

      # The consumer supervisor stops its children with exactly :shutdown on a
      # group stop, before this process learns of it.
      :shutdown ->
        Logger.info("Consumer of #{inspect(tp)} stopped (:shutdown); not restarting it")
        {:noreply, mark_stopped(state, key, entry)}

      clean when clean == :normal or (is_tuple(clean) and elem(clean, 0) == :shutdown) ->
        Logger.error(
          "Consumer of #{inspect(tp)} stopped on its own (#{inspect(clean)}); not restarting it, " <>
            "the partition is not consumed until the broker assigns it elsewhere"
        )

        Telemetry.emit_partition_stopped(
          state.group_name,
          state.member_id,
          state.member_epoch,
          entry.topic,
          elem(key, 1),
          clean
        )

        {:noreply, mark_stopped(state, key, entry)}

      crash ->
        Logger.error("Consumer of #{inspect(tp)} exited abnormally: #{inspect(crash)}")
        {:noreply, schedule_restart(state, key, entry)}
    end
  end

  defp mark_stopped(%State{} = state, key, entry) do
    stopped = %{entry | pid: nil, ref: nil, epoch: nil, status: :stopped}
    %State{state | owned: Map.put(state.owned, key, stopped)}
  end

  defp schedule_restart(%State{} = state, key, entry) do
    crashes = if now_ms() - entry.started_at >= @crash_reset_ms, do: 1, else: entry.crashes + 1

    delay =
      Retry.backoff_delay(
        crashes - 1,
        state.config.partition_restart_backoff_ms,
        state.config.partition_restart_max_backoff_ms
      )

    token = make_ref()
    Process.send_after(self(), {:restart_partition, key, token}, delay)

    entry = %{entry | pid: nil, ref: nil, epoch: nil, status: :restarting, crashes: crashes, token: token}
    %State{state | owned: Map.put(state.owned, key, entry)}
  end

  # ---------------------------------------------------------------------------
  # Shutdown

  @impl true
  def terminate(reason, %State{} = state) do
    terminal = terminal_reason(reason)
    if terminal, do: Telemetry.emit_member_terminated(state.group_name, state.member_id, terminal)

    # Lost (stopped without a commit) when the member's epoch can no longer
    # be valid: it was fenced, or a heartbeat failed with a terminal error
    # (Java transitionToFatal invokes onPartitionsLost and sends no leave).
    # A consumer's own terminal error (e.g. a commit error) leaves the
    # member's epoch valid: the other consumers commit and the member leaves.
    # The reply to a heartbeat in flight may move the member to a new epoch:
    # it is awaited (bounded) before the keepalive starts, which must send
    # that epoch. The keepalive reads the heartbeat timer, cancelled after.
    state = await_inflight(state, state.config.leave_timeout_ms)
    lost = state.fatal or state.phase == :fenced
    keepalive = start_keepalive(state)
    state = state |> cancel_heartbeat() |> abort_push() |> abort_inflight() |> abort_lookup()
    timeout = consumer_shutdown(state.consumer_opts) + @terminate_margin

    # An assign and restarts in flight finish first (bounded by consumer
    # init, one shared deadline) so their consumers are stopped too. Every
    # live consumer is then stopped concurrently, those a revoke or lose in
    # flight is stopping included: a consumer already terminating never
    # handles the second stop request, which just waits for the same exit.
    # The whole stop takes about one consumer :shutdown.
    deadline = now_ms() + timeout
    state = if match?(%{kind: :assign}, state.op), do: await_op(state, timeout), else: state
    state = await_starts(state, deadline)
    stop_all_consumers(state, lost)
    await_op(state, timeout)
    stop_keepalive(keepalive)

    if leave_on_shutdown?(state), do: leave(state)
    stop_client(state)
    :ok
  end

  # Java AbstractMembershipManager.leaveGroup skips the leave only when the
  # member is not in the group (UNSUBSCRIBED, FENCED, FATAL, STALE): a
  # joining member leaves too. Once a join was sent the broker may have
  # registered this member id and assigned it partitions; without a leave
  # nobody consumes them until the session expires (and a static member's
  # restart is rejected with :unreleased_instance_id). A leave for a member
  # the broker never registered just fails, bounded by :leave_timeout_ms. It
  # goes through the same client as the aborted join, so it is sent after it.
  defp leave_on_shutdown?(%State{fatal: true}), do: false
  defp leave_on_shutdown?(%State{phase: :stable}), do: true
  defp leave_on_shutdown?(%State{phase: :joining, join_sent: join_sent}), do: join_sent
  defp leave_on_shutdown?(_state), do: false

  # Heartbeats go on while the consumers stop (Java keeps heartbeating in
  # PREPARE_LEAVING: MemberState is not one shouldSkipHeartbeat skips), and
  # stop only right before the leave. Stopping them first would let a slow
  # shutdown (an assign in flight whose consumers are slow to start, then
  # consumers stopped mid fetch long-poll) outlast the session timeout: the
  # broker would expire the member and give its partitions to another member
  # while these consumers still consume them, and their final commits would
  # be rejected (duplicates). Only a member in the group with a valid epoch
  # heartbeats; a fenced or failed one has nothing to keep alive. The
  # keepalive sends the current epoch and the owned partitions, every other
  # field left out (unchanged), at the broker's interval, the first when the
  # cancelled timer would have fired. Only a reply's epoch and interval are
  # used: the leave follows.
  defp start_keepalive(%State{phase: :stable, fatal: false, client: client} = state) when is_pid(client) do
    if Process.alive?(client) do
      # The owned partitions go on every keepalive: the broker accepts the
      # previous epoch only with owned partitions that are a subset of the
      # assignment, so a keepalive that raced an epoch bump is not fenced.
      opts =
        [member_id: state.member_id, member_epoch: state.member_epoch]
        |> put_present(:instance_id, state.instance_id)
        |> Keyword.put(:topic_partitions, encode_partitions(state.reported))

      interval = max(state.heartbeat_interval_ms || 0, 1)
      group = state.group_name

      first =
        case state.heartbeat_timer do
          {_ref, timer} -> Process.read_timer(timer) || 0
          nil -> interval
        end

      spawn_monitor(fn -> keepalive_loop(client, group, opts, min(first, interval), interval) end)
    end
  end

  defp start_keepalive(_state), do: nil

  # Only the epoch and interval of the reply are kept (its assignment is not
  # reconciled: the member is leaving). A fencing or terminal reply means
  # the epoch is gone: the consumers stop without a commit and no leave is
  # sent, as on a fence or a terminal heartbeat error. Past the timeout the keepalive still carries the owned
  # partitions, which the broker accepts with the previous epoch.
  defp await_inflight(%State{inflight: nil} = state, _timeout), do: state

  defp await_inflight(%State{inflight: %{ref: ref}} = state, timeout) do
    receive do
      {@async_result, ^ref, result} ->
        Process.demonitor(ref, [:flush])
        inflight_reply(%State{state | inflight: nil}, result)

      {:DOWN, ^ref, :process, _, _} ->
        %State{state | inflight: nil}
    after
      timeout -> state
    end
  end

  defp inflight_reply(%State{phase: :stable} = state, {:ok, %ConsumerGroupHeartbeat{} = reply}) do
    interval = if reply.heartbeat_interval_ms > 0, do: reply.heartbeat_interval_ms, else: state.heartbeat_interval_ms
    epoch = if reply.member_epoch > 0, do: reply.member_epoch, else: state.member_epoch
    %State{state | member_epoch: epoch, heartbeat_interval_ms: interval}
  end

  defp inflight_reply(%State{} = state, {:error, reason}) do
    cond do
      Retry.consumer_group_heartbeat_rejoin_error?(reason) ->
        Telemetry.emit_member_fenced(
          state.group_name,
          state.member_id,
          state.member_epoch,
          reason,
          map_size(state.owned)
        )

        %State{state | phase: :fenced}

      # As heartbeat_error/2: a transient failure keeps the epoch; anything
      # else is terminal and leaves no valid epoch.
      reason == :topic_authorization_failed or Retry.consumer_group_heartbeat_retryable?(reason) or
          match?({:heartbeat_process_died, _}, reason) ->
        state

      true ->
        Logger.error("ConsumerGroupHeartbeat for #{state.group_name} failed with terminal #{inspect(reason)}")
        %State{state | fatal: true}
    end
  end

  defp inflight_reply(state, _result), do: state

  defp keepalive_loop(client, group, opts, delay, interval) do
    Process.sleep(delay)

    case send_heartbeat(client, group, opts) do
      # A new epoch (a new target, not reconciled any more) is sent back, as
      # the broker expects the member's current epoch.
      {:ok, %ConsumerGroupHeartbeat{member_epoch: epoch, heartbeat_interval_ms: next}} ->
        opts = if epoch > 0, do: Keyword.put(opts, :member_epoch, epoch), else: opts
        next = if next > 0, do: next, else: interval
        keepalive_loop(client, group, opts, next, next)

      {:error, reason} ->
        # Retried as heartbeat_error/2 retries; fenced or terminal: the
        # member is gone, nothing to keep alive.
        if reason == :topic_authorization_failed or Retry.consumer_group_heartbeat_retryable?(reason),
          do: keepalive_loop(client, group, opts, interval, interval),
          else: :ok
    end
  end

  # A heartbeat the keepalive already handed to the client is answered before
  # the leave, which goes through the same client.
  defp stop_keepalive({pid, ref}) do
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    end
  end

  defp stop_keepalive(_none), do: :ok

  defp await_starts(%State{starts: starts} = state, _deadline) when map_size(starts) == 0, do: state

  defp await_starts(%State{starts: starts} = state, deadline) do
    receive do
      {@async_result, ref, _result} when is_map_key(starts, ref) ->
        Process.demonitor(ref, [:flush])
        await_starts(%State{state | starts: Map.delete(starts, ref)}, deadline)

      {:DOWN, ref, :process, _, _} when is_map_key(starts, ref) ->
        await_starts(%State{state | starts: Map.delete(starts, ref)}, deadline)
    after
      max(deadline - now_ms(), 0) ->
        for {ref, %{pid: pid}} <- starts do
          Process.demonitor(ref, [:flush])
          Process.exit(pid, :kill)
        end

        %State{state | starts: %{}}
    end
  end

  defp terminal_reason({:shutdown, {:terminal, reason}}), do: reason
  defp terminal_reason({:shutdown, {:client_died, reason}}), do: {:client_died, reason}
  defp terminal_reason({exception, stacktrace}) when is_list(stacktrace), do: {:crashed, crash_module(exception)}
  defp terminal_reason(_), do: nil

  defp crash_module(%{__struct__: module}), do: module
  defp crash_module(_), do: :unknown

  # A revoke or assign in flight: let it finish, so its consumers are dead
  # (revoke) or known (assign) before the rest are stopped.
  defp await_op(%State{op: nil} = state, _timeout), do: state

  defp await_op(%State{op: %{ref: ref, pid: pid}} = state, timeout) do
    receive do
      {@async_result, ^ref, _result} ->
        Process.demonitor(ref, [:flush])
        %State{state | op: nil}

      {:DOWN, ^ref, :process, _, _} ->
        %State{state | op: nil}
    after
      timeout ->
        Process.demonitor(ref, [:flush])
        Process.exit(pid, :kill)
        %State{state | op: nil}
    end
  end

  # Every consumer this member started is under its consumer supervisor.
  defp stop_all_consumers(%State{consumer_supervisor: nil}, _lost), do: :ok

  defp stop_all_consumers(%State{consumer_supervisor: supervisor}, lost) do
    case supervisor |> GenConsumerSupervisor.partition_pids() |> Map.keys() do
      [] -> :ok
      tps -> GenConsumerSupervisor.stop_partitions(supervisor, tps, lost: lost)
    end

    :ok
  end

  defp leave(%State{client: client} = state) do
    if Process.alive?(client), do: do_leave(state)
  end

  defp do_leave(%State{} = state) do
    # -2: a static member leaves temporarily and keeps its assignment until
    # its session expires; -1: a dynamic member leaves for good.
    epoch = if state.instance_id, do: -2, else: -1
    opts = put_present([member_id: state.member_id, member_epoch: epoch], :instance_id, state.instance_id)
    timeout = state.config.leave_timeout_ms
    task = Task.async(fn -> send_heartbeat(state.client, state.group_name, opts) end)

    result =
      case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
        {:ok, {:ok, _}} -> :ok
        {:ok, {:error, reason}} -> {:error, reason}
        {:exit, reason} -> {:error, {:exit, reason}}
        nil -> {:error, :timeout}
      end

    Logger.info("Member #{state.member_id} left #{state.group_name} (epoch #{epoch}): #{inspect(result)}")
    Telemetry.emit_member_left(state.group_name, state.member_id, epoch, result)
  end

  defp stop_client(%State{owns_client: true, client: client}) do
    if Process.alive?(client) do
      Process.unlink(client)

      try do
        GenServer.stop(client, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end
  end

  defp stop_client(_state), do: :ok

  # ---------------------------------------------------------------------------
  # Helpers

  # Runs `fun` in a monitored (not linked) process that sends
  # {@async_result, monitor_ref, result} back. A result for a worker that was
  # aborted meanwhile (no slot holds its ref) is ignored. A 1-arity `fun` is
  # given the monitor ref (to tag messages it sends before its result).
  defp run_async(fun) do
    owner = self()

    {pid, ref} =
      spawn_monitor(fn ->
        ref = receive(do: ({@async_result, :ref, ref} -> ref))
        result = if is_function(fun, 1), do: fun.(ref), else: fun.()
        send(owner, {@async_result, ref, result})
      end)

    send(pid, {@async_result, :ref, ref})
    {pid, ref}
  end

  defp async_finished(%State{inflight: %{ref: ref} = inflight} = state, ref, outcome) do
    Process.demonitor(ref, [:flush])

    result =
      case outcome do
        {:op_result, result} -> result
        other -> {:error, {:heartbeat_process_died, other}}
      end

    heartbeat_done(%State{state | inflight: nil}, inflight, result)
  end

  defp async_finished(%State{op: %{ref: ref} = op} = state, ref, outcome) do
    Process.demonitor(ref, [:flush])
    op_done(%State{state | op: nil}, op, outcome)
  end

  defp async_finished(%State{push: %{ref: ref} = push} = state, ref, outcome) do
    Process.demonitor(ref, [:flush])
    push_done(%State{state | push: nil}, push, outcome)
  end

  defp async_finished(%State{lookup: %{ref: ref}} = state, ref, outcome) do
    Process.demonitor(ref, [:flush])
    lookup_done(%State{state | lookup: nil}, outcome)
  end

  defp async_finished(%State{starts: starts} = state, ref, outcome) when is_map_key(starts, ref) do
    Process.demonitor(ref, [:flush])
    {start, starts} = Map.pop(starts, ref)
    restart_done(%State{state | starts: starts}, start, outcome)
  end

  defp async_finished(state, _ref, _outcome), do: {:noreply, state}

  defp topic_partition(state, {_topic_id, partition} = key), do: {state.owned[key].topic, partition}

  defp topic_partition_for(state, {topic_id, partition}), do: {Map.fetch!(state.topic_names, topic_id), partition}

  defp now_ms, do: System.monotonic_time(:millisecond)
end
