defmodule KafkaEx.Consumer.GenConsumer do
  @moduledoc """
  A behaviour module for implementing a Kafka consumer.

  A `KafkaEx.Consumer.GenConsumer` is an Elixir process that consumes messages from
  Kafka. A single `KafkaEx.Consumer.GenConsumer` process consumes from a single
  partition of a Kafka topic. Several `KafkaEx.Consumer.GenConsumer` processes can be
  used to consume from multiple partitions or even multiple topics. Partition
  assignments for a group of `KafkaEx.Consumer.GenConsumer`s can be defined manually
  using `KafkaEx.Consumer.GenConsumer.Supervisor` or coordinated across a cluster of
  nodes using `KafkaEx.Consumer.ConsumerGroup`.

  A `KafkaEx.Consumer.GenConsumer` must implement three callbacks.  Two of these will be
  defined with default behavior if you add `use KafkaEx.Consumer.GenConsumer` to your
  module, leaving just `c:handle_message_set/2` to be implemented.  This is the
  recommended usage.

  ## Example

  The `KafkaEx.Consumer.GenConsumer` behaviour abstracts common Kafka consumer
  interactions.  `KafkaEx.Consumer.GenConsumer` will take care of the details of
  determining a starting offset, fetching messages from a Kafka broker, and
  committing offsets for consumed messages. Developers are only required to
  implement `c:handle_message_set/2` to process messages.

  The following is a minimal example that logs each message as it's consumed:

  ```
  defmodule ExampleGenConsumer do
    use KafkaEx.Consumer.GenConsumer

    require Logger

    # note - messages are delivered in batches
    def handle_message_set(message_set, state) do
      for %Record{value: message} <- message_set do
        Logger.debug(fn -> "message: " <> inspect(message) end)
      end
      {:async_commit, state}
    end
  end
  ```

  `c:handle_message_set/2` will be called with the batch of messages fetched
  from the broker.  The number of messages in a batch is determined by the
  number of messages available and the `max_bytes` and `min_bytes` parameters
  of the fetch request (which can be configured in KafkaEx).  In this example,
  because `c:handle_message_set/2` always returns `{:async_commit, new_state}`,
  the message offsets will be automatically committed asynchronously.

  ## Committing Offsets

  `KafkaEx.Consumer.GenConsumer` manages a consumer's offsets by committing the offsets
  of consumed messages.  KafkaEx supports two commit strategies: asynchronous
  and synchronous.  The return value of `c:handle_message_set/2` determines
  which strategy is used:

  * `{:sync_commit, new_state}` causes synchronous offset commits.
  * `{:async_commit, new_state}` causes asynchronous offset commits.

  Note that with both of the offset commit strategies, only if the final offset
  in the message set is committed and this is done after the messages are
  consumed.  If you want to commit the offset of every message consumed, use
  the synchronous offset commit strategy and implement calls to
  `KafkaEx.offset_commit/2` within your consumer as appropriate.

  ### Synchronous offset commits

  When `c:handle_message_set/2` returns `{:sync_commit, new_state}`, the offset
  of the final message in the message set is committed immediately before
  fetching any more messages.  This strategy requires a significant amount of
  communication with the broker and could correspondingly degrade consumer
  performance, but it will keep the offset commits tightly synchronized with
  the consumer state.

  Choose the synchronous offset commit strategy if you want to favor
  consistency of offset commits over performance, or if you have a low rate of
  message arrival.  The definition of a "low rate" depends on the situation,
  but tens of messages per second could be considered a "low rate" in most
  situations.

  ### Asynchronous offset commits

  When `c:handle_message_set/2` returns `{:async_commit, new_state}`, KafkaEx
  will not commit offsets after every message set consumed.  To avoid
  excessive network calls, the offsets are committed periodically (and when
  the worker terminates).

  How often a `KafkaEx.Consumer.GenConsumer` auto-commits offsets is controlled by the two
  configuration values `:commit_interval` and `:commit_threshold`.

  * `:commit_interval` is the maximum time (in milliseconds) that a
    `KafkaEx.Consumer.GenConsumer` will delay committing the offset for an acknowledged
    message. The interval is a deadline checked at the end of a fetch cycle, not a
    timer, so a fetch backoff (see *Fetch failure handling*) stretches it.

  * `:commit_threshold` is the maximum number of acknowledged messages that a
    `KafkaEx.Consumer.GenConsumer` will allow to be uncommitted before triggering a
    commit.

  These can be set globally in the `:kafka_ex` app's environment or on a
  per-consumer basis by passing options to `start_link/5`:

  ```
  # In config/config.exs
  config :kafka_ex,
    commit_interval: 5000,
    commit_threshold: 100

  # As options to start_link/5
  KafkaEx.Consumer.GenConsumer.start_link(MyConsumer, "my_group", "topic", 0,
                                 commit_interval: 5000,
                                 commit_threshold: 100)
  ```

  For low-volume topics, `:commit_interval` is the dominant factor for how
  often a `KafkaEx.Consumer.GenConsumer` auto-commits. For high-volume topics,
  `:commit_threshold` is the dominant factor.

  ## Fetch failure handling

  A fetch error that `KafkaEx.Support.Retry.fetch_retryable?/1` accepts — a leader
  move (`:not_leader_for_partition`, `:leader_not_available`), `:no_broker`, a
  closed socket, a timeout — makes the consumer back off and retry rather than
  stop. Stopping would take the whole group down, because
  `KafkaEx.Consumer.ConsumerGroup` supervises with `max_restarts: 0`. Retries are
  unbounded, matching brod, KafkaJS and librdkafka, with jittered exponential
  backoff from `:fetch_retry_base_delay_ms` (250) to `:fetch_retry_max_delay_ms`
  (5000), both settable in the `:kafka_ex` app environment. A warning naming the
  error and the consecutive failure count is logged while the backoff ramps, then
  roughly once a minute. Any other fetch error still stops the consumer.

  Retries never give up, so a partition that is not merely mid-leader-move but genuinely stuck (a
  deleted topic, a leader that never returns) is surfaced once, after
  `:fetch_unavailable_warn_ms` (default 30000, settable in the `:kafka_ex` app environment) of
  continuous failure, as a `Logger.error` and a `[:kafka_ex, :consumer, :partition_unavailable]`
  telemetry event. The consumer keeps retrying; the signal just makes a non-recovering partition
  alertable without a human reading the logs. This mirrors librdkafka's
  `topic.metadata.propagation.max.ms`.

  The same backoff covers a failed `:offset_out_of_range` reset, which needs a
  live leader of its own. It does **not** cover establishing the starting offset
  at startup: `load_offsets/1` still raises if the committed offset or the reset
  offset cannot be read, so a consumer that starts while its leader is moving
  still fails to start.

  ## KIP-848 membership (`:member_epoch`)

  A consumer started with `:member_epoch` (and `:member_id`) is a member of a
  consumer-protocol group (KIP-848). It loads its starting offset with
  OffsetFetch v9+ carrying `member_id`/`member_epoch` and commits with
  OffsetCommit v9+ `generation_id_or_member_epoch = member_epoch` (no
  generation id). Without `:member_epoch` (classic groups) both requests are
  exactly as before. The owner (see `KafkaEx.Consumer.GenConsumer.Supervisor`)
  moves a running consumer to a new epoch with `update_membership/3`.

  The broker validates the membership on both requests (Kafka 4.4
  `ConsumerGroup.validateOffsetFetch` / `validateOffsetCommit`):

    * Startup OffsetFetch rejected with `:stale_member_epoch` (OffsetFetch
      needs the member's exact current epoch): the consumer stays up without
      an offset and retries with an exponential backoff from
      `:load_offsets_retry_backoff_ms` (capped at 30 s, one warning), or at
      once when `update_membership/3` delivers the current epoch.
    * Startup OffsetFetch rejected with `:unknown_member_id` or
      `:fenced_member_epoch` (the group no longer knows or has fenced the
      member): the consumer casts `{:rejoin_required, reason, member_epoch}`
      to its `:group_manager_pid` and exits cleanly with
      `{:shutdown, {:rejoin_required, reason}}`, instead of raising into a
      crash. The same exit follows a commit rejected with these errors (see
      "Commit failure handling").
    * Commits: see "Commit failure handling" for `:stale_member_epoch`.
    * `:member_epoch` with OffsetCommit pinned below v9 (`:api_versions`):
      every commit fails with `:invalid_api_version`, a terminal error.

  A `:member_epoch` that is not an int32 `>= 0`, or one without a binary
  `:member_id`, makes `start_link/5` return `{:error, :invalid_membership}`.

  ## Handler state and interaction

  Use the `c:init/2` to initialize consumer state and `c:handle_call/3`,
  `c:handle_cast/2`, or `c:handle_info/2` to interact.

  Example:

  ```
  defmodule MyConsumer do
    use KafkaEx.Consumer.GenConsumer

    defmodule State do
      defstruct messages: [], calls: 0
    end

    def init(_topic, _partition) do
      {:ok, %State{}}
    end

    def init(_topic, _partition, extra_args) do
      {:ok, %State{}}
    end

    def handle_message_set(message_set, state) do
      {:async_commit, %{state | messages: state.messages ++ message_set}}
    end

    def handle_call(:messages, _from, state) do
      {:reply, state.messages, %{state | calls: state.calls + 1}}
    end
  end

  {:ok, pid} = GenConsumer.start_link(MyConsumer, "consumer_group", "topic", 0)
  GenConsumer.call(pid, :messages)
  ```

  **NOTE** If you do not implement `c:handle_call/3` or `c:handle_cast/2`, any
  calls to `GenConsumer.call/3` or casts to `GenConsumer.cast/2` will raise an
  error. Similarly, any messages sent to a `GenConsumer` will log an error if
  there is no corresponding `c:handle_info/2` callback defined.

  ## Commit failure handling

  When an `OffsetCommit` request fails, the commit error is classified and
  handled according to the reference Kafka client patterns (Java
  `ConsumerCoordinator.OffsetCommitResponseHandler`, librdkafka
  `rdkafka_cgrp.c`, brod `brod_group_coordinator`):

  * **Terminal** — `:fenced_instance_id` (KIP-345),
    `:group_authorization_failed`, `:topic_authorization_failed`,
    `:offset_metadata_too_large`, `:invalid_commit_offset_size`, and
    `:invalid_api_version` (a `:member_epoch` is set but OffsetCommit is
    pinned below v9 via `:api_versions`, so no commit can ever succeed).
    The consumer stops immediately without rejoining. Under
    `restart: :transient` the supervisor does not respawn.

  * **Fatal (rejoin-required)** — `:illegal_generation`,
    `:unknown_member_id`, and their OffsetCommit v9+ counterparts
    `:group_id_not_found`, `:stale_member_epoch`, `:fenced_member_epoch`
    (see `KafkaEx.Support.Retry.commit_fatal_error?/1`). The consumer casts `{:rejoin_required, reason,
    stale_generation_id}` to the group manager and self-stops (for a KIP-848
    member the third element is the member epoch the commit was sent with,
    and `:stale_member_epoch` is not fatal, see below). The manager
    resets member_id/generation_id (matching Java `resetGeneration()`) and
    runs a rebalance. GenConsumers on the new generation are spawned
    afresh.

  * **KIP-848 `:stale_member_epoch`** (only when `:member_epoch` is set) —
    the epoch the commit was built with is no longer accepted for the
    partition, e.g. `update_membership/3` arrived while the commit was in
    flight, or the partition is being revoked. A commit already sent keeps
    the epoch it was built with. The error is logged and reported as
    `:transient`; the consumer keeps running, the offsets stay acked and the
    next commit (interval, threshold, `:sync_commit` or shutdown) sends them
    with the current epoch. Revocation itself is the group manager's job.

  * **Retryable** — `:rebalance_in_progress` (broker-signalled; the next
    heartbeat will drive the rebalance), `:unstable_offset_commit`
    (KIP-447), and standard transient errors (`:timeout`,
    `:coordinator_not_available`, …). The commit is retried with
    exponential backoff.

  All three paths emit `[:kafka_ex, :consumer, :commit_failed]` telemetry
  with metadata `%{group_id, topic, partition, offset, kind, error}`
  where `kind` is `:fatal | :terminal | :transient`. This is the primary
  user-observable signal for commit failures (kafka_ex does not currently
  provide a synchronous `handle_commit_failure/2` callback — subscribe to
  the telemetry event instead).

  At-least-once semantics are preserved: any uncommitted messages since
  the last successful commit will be re-delivered after the rejoin.

  ## Testing

  A `KafkaEx.Consumer.GenConsumer` can be unit-tested without a running Kafka broker by sending
  messages directly to its `c:handle_message_set/2` function. The following
  recipe can be used as a starting point when testing a `KafkaEx.Consumer.GenConsumer`:

  ```
  defmodule ExampleGenConsumerTest do
    use ExUnit.Case, async: true

    alias KafkaEx.Messages.Fetch.Record

    @topic "topic"
    @partition 0

    setup do
      {:ok, state} = ExampleGenConsumer.init(@topic, @partition)
      {:ok, %{state: state}}
    end

    test "it acks a message", %{state: state} do
      message_set = [Record.build(offset: 0, value: "hello")]
      {response, _new_state} =
        ExampleGenConsumer.handle_message_set(message_set, state)
      assert response == :async_commit
    end
  end
  ```
  """

  use GenServer

  alias KafkaEx.API, as: KafkaExAPI
  alias KafkaEx.Client
  alias KafkaEx.Config
  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Support.Retry
  alias KafkaEx.Support.VersionHelper
  alias KafkaEx.Telemetry

  require Logger

  @typedoc """
  Option values used when starting a `KafkaEx.Consumer.GenConsumer`.
  """
  @type option ::
          {:client, GenServer.server()}
          | {:commit_interval, non_neg_integer}
          | {:commit_threshold, non_neg_integer}
          | {:auto_offset_reset, :none | :earliest | :latest}
          | {:api_versions, map()}
          | {:shutdown, timeout()}
          | {:extra_consumer_args, map()}
          | {:member_id, binary()}
          | {:generation_id, integer()}
          | {:member_epoch, non_neg_integer()}
          | {:group_manager_pid, pid()}

  @typedoc """
  Options used when starting a `KafkaEx.Consumer.GenConsumer`.
  """
  @type options :: [option | GenServer.option()]

  @doc """
  Invoked when the server is started. `start_link/5` will block until it
  returns.

  `topic` and `partition` are the arguments passed to `start_link/5`. They
  identify the Kafka partition that the `KafkaEx.Consumer.GenConsumer` will consume from.

  Returning `{:ok, state}` will cause `start_link/5` to return `{:ok, pid}` and
  the process to start consuming from its assigned partition. `state` becomes
  the consumer's state.

  Any other return value will cause the `start_link/5` to return `{:error,
  error}` and the process to exit.
  """
  @callback init(topic :: binary, partition :: non_neg_integer) ::
              {:ok, state :: term}
              | {:stop, reason :: term}

  @doc """
  Invoked when the server is started. `start_link/5` will block until it
  returns.

  `topic` and `partition` are the arguments passed to `start_link/5`. They
  identify the Kafka partition that the `KafkaEx.Consumer.GenConsumer` will consume from.

  `extra_args` is the value of the `extra_consumer_args` option to `start_link/5`.

  The default implementation of this function calls `init/2`.

  Returning `{:ok, state}` will cause `start_link/5` to return `{:ok, pid}` and
  the process to start consuming from its assigned partition. `state` becomes
  the consumer's state.

  Any other return value will cause the `start_link/5` to return `{:error,
  error}` and the process to exit.
  """
  @callback init(
              topic :: binary,
              partition :: non_neg_integer,
              extra_args :: map()
            ) :: {:ok, state :: term} | {:stop, reason :: term}

  @doc """
  Invoked for each message set consumed from a Kafka topic partition.

  `message_set` is a message set fetched from a Kafka broker and `state` is the
  current state of the `KafkaEx.Consumer.GenConsumer`.

  Returning `{:async_commit, new_state}` acknowledges `message` and continues
  to consume from the Kafka queue with new state `new_state`. Acknowledged
  messages will be auto-committed (possibly at a later time) based on the
  `:commit_interval` and `:commit_threshold` options.

  Returning `{:sync_commit, new_state}` commits `message` synchronously before
  continuing to consume from the Kafka queue with new state `new_state`.
  Committing a message synchronously means that no more messages will be
  consumed until the message's offset is committed. `:sync_commit` should be
  used sparingly, since committing every message synchronously would impact a
  consumer's performance and could result in excessive network traffic.
  """
  @callback handle_message_set(message_set :: [Record.t()], state :: term) ::
              {:async_commit, new_state :: term}
              | {:sync_commit, new_state :: term}

  @doc """
  Invoked by `KafkaEx.Consumer.GenConsumer.call/3`.

  Note the default implementation will cause a `RuntimeError`.  If you want to
  interact with your consumer, you must implement a handle_call function.
  """
  @callback handle_call(call :: term, from :: GenServer.from(), state :: term) ::
              {:reply, reply_value :: term, new_state :: term}
              | {:stop, reason :: term, reply_value :: term, new_state :: term}
              | {:stop, reason :: term, new_state :: term}

  @doc """
  Invoked by `KafkaEx.Consumer.GenConsumer.cast/2`.

  Note the default implementation will cause a `RuntimeError`.  If you want to
  interact with your consumer, you must implement a handle_cast function.
  """
  @callback handle_cast(cast :: term, state :: term) ::
              {:noreply, new_state :: term}
              | {:stop, reason :: term, new_state :: term}

  @doc """
  Invoked by sending messages to the consumer.

  Note the default implementation will log error messages.  If you want to
  interact with your consumer, you must implement a handle_info function.
  """
  @callback handle_info(info :: term, state :: term) ::
              {:noreply, new_state :: term}
              | {:stop, reason :: term, new_state :: term}

  defmacro __using__(_opts) do
    quote do
      @behaviour KafkaEx.Consumer.GenConsumer
      alias KafkaEx.Messages.Fetch.Record

      def init(_topic, _partition) do
        {:ok, nil}
      end

      def init(topic, partition, _extra_args) do
        init(topic, partition)
      end

      def handle_call(msg, _from, consumer_state) do
        # taken from the GenServer handle_call implementation
        proc =
          case Process.info(self(), :registered_name) do
            {_, []} -> self()
            {_, name} -> name
          end

        # We do this to trick Dialyzer to not complain about non-local returns.
        case :erlang.phash2(1, 1) do
          0 ->
            raise "attempted to call KafkaEx.Consumer.GenConsumer #{inspect(proc)} " <>
                    "but no handle_call/3 clause was provided"

          1 ->
            {:reply, {:bad_call, msg}, consumer_state}
        end
      end

      def handle_cast(msg, consumer_state) do
        # taken from the GenServer handle_cast implementation
        proc =
          case Process.info(self(), :registered_name) do
            {_, []} -> self()
            {_, name} -> name
          end

        # We do this to trick Dialyzer to not complain about non-local returns.
        case :erlang.phash2(1, 1) do
          0 ->
            raise "attempted to cast KafkaEx.Consumer.GenConsumer #{inspect(proc)} " <>
                    " but no handle_cast/2 clause was provided"

          1 ->
            {:noreply, consumer_state}
        end
      end

      def handle_info(msg, consumer_state) do
        # taken from the GenServer handle_info implementation
        proc =
          case Process.info(self(), :registered_name) do
            {_, []} -> self()
            {_, name} -> name
          end

        pattern = ~c"~p ~p received unexpected message in handle_info/2: ~p~n"
        :error_logger.error_msg(pattern, [__MODULE__, proc, msg])
        {:noreply, consumer_state}
      end

      defoverridable init: 2,
                     init: 3,
                     handle_call: 3,
                     handle_cast: 2,
                     handle_info: 2
    end
  end

  defmodule State do
    @moduledoc false
    defstruct [
      :consumer_module,
      :consumer_state,
      :commit_interval,
      :commit_threshold,
      :client,
      :group,
      :topic,
      :partition,
      :member_id,
      :generation_id,
      # KIP-848 consumer-protocol member epoch; nil for classic groups.
      :member_epoch,
      :current_offset,
      :committed_offset,
      :acked_offset,
      :last_commit,
      :auto_offset_reset,
      :fetch_options,
      :api_versions,
      :group_manager_pid,
      :fetch_retry_at,
      :fetch_failing_since,
      # KIP-848: monotonic ms before which a startup OffsetFetch rejected with
      # :stale_member_epoch is retried (nil = not waiting); update_membership/3
      # clears it.
      :load_offsets_retry_at,
      load_offsets_stale_count: 0,
      # true only when we started the client — a caller-supplied (shared) :client must not be stopped.
      owns_client: false,
      fetch_error_count: 0,
      fetch_unavailable_reported: false
    ]

    @type t :: %__MODULE__{
            consumer_module: module(),
            consumer_state: term(),
            commit_interval: pos_integer(),
            commit_threshold: non_neg_integer(),
            client: pid(),
            group: binary(),
            topic: binary(),
            partition: non_neg_integer(),
            member_id: binary() | nil,
            generation_id: integer() | nil,
            member_epoch: non_neg_integer() | nil,
            current_offset: integer() | nil,
            committed_offset: integer() | nil,
            acked_offset: integer() | nil,
            last_commit: integer() | nil,
            auto_offset_reset: :none | :earliest | :latest,
            fetch_options: Keyword.t(),
            api_versions: map(),
            group_manager_pid: pid() | nil,
            fetch_retry_at: integer() | nil,
            fetch_failing_since: integer() | nil,
            load_offsets_retry_at: integer() | nil,
            load_offsets_stale_count: non_neg_integer(),
            owns_client: boolean(),
            fetch_error_count: non_neg_integer(),
            fetch_unavailable_reported: boolean()
          }
  end

  @commit_interval 5_000
  @commit_threshold 100
  @auto_offset_reset :latest

  # Commit retry settings (issue #425)
  # Uses KafkaEx.Support.Retry for unified retry logic with exponential backoff
  @commit_max_attempts 3
  @commit_base_delay_ms 100

  # Low cap — a leader move is usually sub-second; jittered against a synchronized restart.
  @fetch_retry_base_delay_ms 250
  @fetch_retry_max_delay_ms 5_000

  # After this long of continuous failure a partition is clearly stuck, not mid-move; surface it once.
  @fetch_unavailable_warn_ms 30_000

  # Client API

  @doc """
  Starts a `KafkaEx.Consumer.GenConsumer` process linked to the current process.

  This can be used to start the `KafkaEx.Consumer.GenConsumer` as part of a supervision tree.

  Once the consumer has been started, the `c:init/2` function of
  `consumer_module` is called with `topic` and `partition` as its arguments.
  `group_name` is the consumer group name that will be used for managing
  consumer offsets.

  ### Options

  * `:client` - An existing `KafkaEx.Client` pid to use for Kafka operations.
    If not provided, a new client will be started automatically.

  * `:commit_interval` - The interval in milliseconds that the consumer will
    wait to commit offsets of handled messages.  Default 5_000.

  * `:commit_threshold` - Threshold number of messages consumed to commit
    offsets to the broker.  Default 100.

  * `:auto_offset_reset` - The policy for choosing a start offset when there is
    no valid committed offset — either none exists yet (new consumer group) or
    an `:offset_out_of_range` error occurs. `:latest` (the default) moves to the
    most recent offset, `:earliest` to the oldest available. `:none` raises
    instead of guessing, so a missing/out-of-range offset surfaces loudly rather
    than silently replaying or skipping data.

  * `:fetch_options` - Optional keyword list that is passed along to the
    fetch operation.

  * `:shutdown` - Optional amount of time in milliseconds that the supervisor
    will wait for a `GenConsumer` to terminate after emitting a
    `Process.exit(child, :shutdown)` signal. Defaults to `5_000`.

  * `:extra_consumer_args` - Optional parameter that is passed along to the
    `GenConsumer.init` call in the consumer module. Note that if `init/3` is not
    implemented, the default implementation calls to `init/2`, dropping the extra
    arguments.

  * `:member_id` / `:generation_id` - classic group membership the consumer
    commits with (set by `KafkaEx.Consumer.ConsumerGroup`).

  * `:member_epoch` - KIP-848 member epoch (int32 `>= 0`; requires a binary
    `:member_id`). See "KIP-848 membership" in the moduledoc.

  * `:group_manager_pid` - the process told about errors only a rejoin can
    fix (`{:rejoin_required, reason, generation_id_or_member_epoch}` cast).

  **NOTE** `:commit_interval`, `:auto_offset_reset` and `:commit_threshold` default to the
  application config (e.g., `Application.get_env/2`) if that value is present, or the stated
  default if the application config is not present.

  Any valid options for `GenServer.start_link/3` can also be specified.

  ### Return Values

  This function has the same return values as `GenServer.start_link/3`.
  """
  @spec start_link(
          consumer_module :: module,
          consumer_group_name :: binary,
          topic_name :: binary,
          partition_id :: non_neg_integer,
          options
        ) :: GenServer.on_start()
  def start_link(consumer_module, group_name, topic, partition, opts \\ []) do
    {server_opts, consumer_opts} = Keyword.split(opts, [:debug, :name, :timeout, :spawn_opt])

    GenServer.start_link(
      __MODULE__,
      {consumer_module, group_name, topic, partition, consumer_opts},
      server_opts
    )
  end

  # Not used by the internal consumer-group supervision path — the effective
  # child spec comes from the inline builder in
  # `KafkaEx.Consumer.GenConsumer.Supervisor.start_link/1`, which already
  # sets `restart: :transient`. This function exists so external code that
  # places a GenConsumer directly under its own Supervisor inherits the
  # same `:transient` restart strategy (stop-on-normal-or-shutdown exit).
  @doc false
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      type: :worker
    }
  end

  @doc """
  Returns the topic and partition id for this consumer process
  """
  @spec partition(GenServer.server()) ::
          {topic :: binary, partition_id :: non_neg_integer}
  def partition(gen_consumer, timeout \\ 5000) do
    GenServer.call(gen_consumer, :partition, timeout)
  end

  @doc """
  Forwards a `GenServer.call/3` to the consumer implementation with the
  consumer's state.

  The implementation must return a `GenServer.call/3`-compatible value of the
  form `{:reply, reply_value, new_consumer_state}`.  The GenConsumer will
  turn this into an immediate timeout, which drives continued message
  consumption.

  See the moduledoc for an example.
  """
  @spec call(GenServer.server(), term, timeout) :: term
  def call(gen_consumer, message, timeout \\ 5000) do
    GenServer.call(gen_consumer, {:consumer_call, message}, timeout)
  end

  @doc """
  Forwards a `GenServer.cast/2` to the consumer implementation with the
  consumer's state.

  The implementation must return a `GenServer.cast/2`-compatible value of the
  form `{:noreply, new_consumer_state}`. The GenConsumer will turn this into an
  immediate timeout, which drives continued message consumption.
  """
  @spec cast(GenServer.server(), term) :: term
  def cast(gen_consumer, message) do
    GenServer.cast(gen_consumer, {:consumer_cast, message})
  end

  # int32 on the wire (OffsetCommit generation_id_or_member_epoch). Negative
  # epochs (-1 / -2) are ConsumerGroupHeartbeat leave markers, never a
  # member's epoch.
  @max_member_epoch 2_147_483_647
  @membership_keys [:member_id, :member_epoch]
  # Above a default fetch long-poll (:max_wait_time 10 s + 5 s network buffer)
  # plus a commit. It is not a worst case: a fetch or commit whose retries all
  # run (RequestBudget.call_budget/2, ~50 s for a fetch) can exceed it, and
  # then the call returns {:error, :timeout} while the update may still be
  # applied once the consumer gets to it.
  @default_update_membership_timeout 60_000

  @doc """
  Updates the KIP-848 membership a running consumer commits with, without
  restarting it.

  `membership` is a keyword list with any of:

    * `:member_id` - the member id (binary)
    * `:member_epoch` - the member's current epoch (int32, `>= 0`)

  Keys that are not given keep their current value. Returns `:ok` once the
  new membership is in the consumer's state: every commit and OffsetFetch the
  consumer *starts* after this returns uses it. A consumer waiting out a
  `:stale_member_epoch` on its startup OffsetFetch retries it at once.

  A commit that is already in flight when the update arrives (including its
  retries) completes with the membership it was built with; the consumer
  handles one message at a time, so the update is applied right after it. If
  that commit is rejected with `:stale_member_epoch`, the offsets stay acked
  and the next commit sends them with the new epoch (see "Commit failure
  handling").

  ## Latency

  This is a `GenServer.call/3`, so it queues behind whatever the consumer is
  doing: a fetch long-poll (up to `:max_wait_time` in `:fetch_options`, 10 s
  by default, plus the network buffer) or a commit with its retries. The
  default `timeout` (60 s) is above one default long-poll plus a commit; pass
  a larger one with a larger `:max_wait_time`. It is not a worst case: a fetch
  or commit that runs through all its retries can take longer, and the call
  then returns `{:error, :timeout}` although the update is still applied once
  the consumer gets to it. Updating N consumers one after another can take N
  long-polls: fan the calls out concurrently.

  ## Errors

  Never exits the caller:

    * `{:error, :invalid_membership}` - an unknown key, a value of the wrong
      type, or an epoch out of range (nothing is sent to the consumer); or
      the consumer was started without a `:member_epoch` (a classic
      consumer, which is never moved onto the KIP-848 path, so it never
      holds an epoch without a member id). The consumer's membership is left
      unchanged.
    * `{:error, :noproc}` - the consumer is not running, or exited before it
      applied the update
    * `{:error, :timeout}` - no reply within `timeout`; the update may still be
      applied afterwards
  """
  @spec update_membership(GenServer.server(), Keyword.t(), timeout) ::
          :ok | {:error, :invalid_membership | :noproc | :timeout}
  def update_membership(gen_consumer, membership, timeout \\ @default_update_membership_timeout) do
    if valid_membership_update?(membership) do
      try do
        GenServer.call(gen_consumer, {:update_membership, membership}, timeout)
      catch
        :exit, {:timeout, _} -> {:error, :timeout}
        :exit, _ -> {:error, :noproc}
      end
    else
      {:error, :invalid_membership}
    end
  end

  defp valid_membership_update?(membership) when is_list(membership) do
    Keyword.keyword?(membership) and
      Enum.all?(membership, fn
        {:member_id, id} -> is_binary(id)
        {:member_epoch, epoch} -> valid_member_epoch?(epoch)
        _ -> false
      end)
  end

  defp valid_membership_update?(_membership), do: false

  defp valid_member_epoch?(epoch), do: is_integer(epoch) and epoch >= 0 and epoch <= @max_member_epoch

  # GenServer callbacks

  def init({consumer_module, group_name, topic, partition, opts}) do
    # Rejected here, not on the first commit: the request would be refused
    # by the client on every commit and the consumer would never commit.
    if valid_start_membership?(opts),
      do: do_init(consumer_module, group_name, topic, partition, opts),
      else: {:stop, :invalid_membership}
  end

  # Without :member_epoch (classic) nothing is checked, as before. With one,
  # the member id is required too (KIP-848 commits and fetches carry both).
  defp valid_start_membership?(opts) do
    case Keyword.get(opts, :member_epoch) do
      nil -> true
      epoch -> valid_member_epoch?(epoch) and is_binary(Keyword.get(opts, :member_id))
    end
  end

  defp do_init(consumer_module, group_name, topic, partition, opts) do
    commit_interval =
      Keyword.get(
        opts,
        :commit_interval,
        Application.get_env(:kafka_ex, :commit_interval, @commit_interval)
      )

    commit_threshold =
      Keyword.get(
        opts,
        :commit_threshold,
        Application.get_env(:kafka_ex, :commit_threshold, @commit_threshold)
      )

    auto_offset_reset =
      opts
      |> Keyword.get(
        :auto_offset_reset,
        Application.get_env(:kafka_ex, :auto_offset_reset, @auto_offset_reset)
      )
      |> Config.validate_auto_offset_reset!()

    extra_consumer_args =
      Keyword.get(
        opts,
        :extra_consumer_args
      )

    generation_id = Keyword.get(opts, :generation_id)
    member_id = Keyword.get(opts, :member_id)
    member_epoch = Keyword.get(opts, :member_epoch)
    group_manager_pid = Keyword.get(opts, :group_manager_pid)

    api_versions = Keyword.get(opts, :api_versions, %{})

    case consumer_module.init(topic, partition, extra_consumer_args) do
      {:ok, consumer_state} ->
        {client, owns_client} = resolve_client(opts, group_name, topic)

        default_fetch_options = [
          auto_commit: false
        ]

        given_fetch_options = Keyword.get(opts, :fetch_options, [])

        fetch_options = Keyword.merge(default_fetch_options, given_fetch_options)

        state = %State{
          consumer_module: consumer_module,
          consumer_state: consumer_state,
          commit_interval: commit_interval,
          commit_threshold: commit_threshold,
          auto_offset_reset: auto_offset_reset,
          client: client,
          owns_client: owns_client,
          group: group_name,
          topic: topic,
          partition: partition,
          generation_id: generation_id,
          member_id: member_id,
          member_epoch: member_epoch,
          fetch_options: fetch_options,
          api_versions: api_versions,
          group_manager_pid: group_manager_pid
        }

        Process.flag(:trap_exit, true)

        {:ok, state, 0}

      {:stop, reason} ->
        {:stop, reason}
    end
  end

  # Returns {client, owns_client?} — false for a caller-supplied (shared) :client.
  defp resolve_client(opts, group_name, topic) do
    case Keyword.get(opts, :client) do
      nil ->
        # Seed initial_topics so the client's first refresh is scoped to this
        # consumer's topic, not the whole cluster catalog.
        client_opts = [
          uris: Keyword.get(opts, :uris, Config.brokers()),
          use_ssl: Keyword.get(opts, :use_ssl, Config.use_ssl()),
          ssl_options: Keyword.get(opts, :ssl_options, Config.ssl_options()),
          auth: Keyword.get(opts, :auth, Config.auth_config()),
          consumer_group: group_name,
          initial_topics: [topic]
        ]

        {:ok, client} = Client.start_link(client_opts, :no_name)
        {client, true}

      client ->
        {client, false}
    end
  end

  def handle_call(:partition, _from, state) do
    {:reply, {state.topic, state.partition}, state, 0}
  end

  # Only a consumer started with a member epoch (KIP-848) can be updated: an
  # update must never move a classic consumer (generation_id, fatal
  # :stale_member_epoch) onto the KIP-848 path, nor give it an epoch without
  # a member id. A KIP-848 consumer started with a binary member id (init
  # requires one) and an update only sets binary ids and valid epochs
  # (valid_membership_update?/1), so its membership stays well-formed.
  def handle_call({:update_membership, _membership}, _from, %State{member_epoch: nil} = state) do
    {:reply, {:error, :invalid_membership}, state, 0}
  end

  def handle_call({:update_membership, membership}, _from, %State{} = state) do
    new_state = struct!(state, Keyword.take(membership, @membership_keys))
    {:reply, :ok, %State{new_state | load_offsets_retry_at: nil}, 0}
  end

  def handle_call(
        {:consumer_call, message},
        from,
        %State{
          consumer_module: consumer_module,
          consumer_state: consumer_state
        } = state
      ) do
    # NOTE we only support the {:reply, _, _} result format here
    #   which we turn into a timeout = 0 clause so that we continue to consume.
    #   any other GenServer flow control could have unintended consequences,
    #   so we leave that for later consideration
    consumer_reply =
      consumer_module.handle_call(
        message,
        from,
        consumer_state
      )

    case consumer_reply do
      {:reply, reply, new_consumer_state} ->
        {:reply, reply, %{state | consumer_state: new_consumer_state}, 0}

      {:stop, reason, new_consumer_state} ->
        {:stop, reason, %{state | consumer_state: new_consumer_state}}

      {:stop, reason, reply, new_consumer_state} ->
        {:stop, reason, reply, %{state | consumer_state: new_consumer_state}}
    end
  end

  def handle_cast(
        {:consumer_cast, message},
        %State{
          consumer_module: consumer_module,
          consumer_state: consumer_state
        } = state
      ) do
    # NOTE we only support the {:noreply, _} result format here
    #   which we turn into a timeout = 0 clause so that we continue to consume.
    #   any other GenServer flow control could have unintended consequences,
    #   so we leave that for later consideration
    consumer_reply =
      consumer_module.handle_cast(
        message,
        consumer_state
      )

    case consumer_reply do
      {:noreply, new_consumer_state} ->
        {:noreply, %{state | consumer_state: new_consumer_state}, 0}

      {:stop, reason, new_consumer_state} ->
        {:stop, reason, %{state | consumer_state: new_consumer_state}}
    end
  end

  def handle_info(
        :timeout,
        %State{current_offset: nil, last_commit: nil} = state
      ) do
    # Any inbound message re-arms this at timeout 0; honour the stale-epoch
    # retry deadline (update_membership/3 clears it).
    case load_offsets_retry_remaining(state) do
      0 -> load_offsets_cycle(state)
      remaining -> {:noreply, state, remaining}
    end
  end

  def handle_info(:timeout, %State{} = state) do
    # Any inbound message re-arms this at timeout 0, so honour the backoff deadline held in state.
    case fetch_backoff_remaining(state) do
      0 -> consume_cycle(state)
      remaining -> {:noreply, state, remaining}
    end
  end

  def handle_info(
        message,
        %State{
          consumer_module: consumer_module,
          consumer_state: consumer_state
        } = state
      ) do
    # NOTE we only support the {:noreply, _} result format here
    #   which we turn into a timeout = 0 clause so that we continue to consume.
    #   any other GenServer flow control could have unintended consequences,
    #   so we leave that for later consideration
    consumer_reply =
      consumer_module.handle_info(
        message,
        consumer_state
      )

    case consumer_reply do
      {:noreply, new_consumer_state} ->
        {:noreply, %{state | consumer_state: new_consumer_state}, 0}

      {:stop, reason, new_consumer_state} ->
        {:stop, reason, %{state | consumer_state: new_consumer_state}}
    end
  end

  def terminate(_reason, %State{} = state) do
    # Best-effort single-shot commit before shutdown. We deliberately do NOT
    # go through commit_with_retry: the retry ladder is 100+200+400ms of
    # Process.sleep which can exhaust the worker_shutdown budget when the
    # broker is unreachable. A brutal-kill mid-retry is worse than no commit
    # — the next consumer start will re-read from the last committed offset
    # and re-process at most one batch (at-least-once).
    try do
      terminal_commit(state)
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    if state.owns_client and Process.alive?(state.client) do
      Process.unlink(state.client)

      try do
        GenServer.stop(state.client, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  # Helpers

  defp consume(
         %State{
           client: client,
           topic: topic,
           partition: partition,
           current_offset: offset,
           fetch_options: fetch_options
         } = state
       ) do
    fetch_opts = VersionHelper.maybe_put_api_version(fetch_options, state.api_versions, :fetch)

    case KafkaExAPI.fetch(client, topic, partition, offset, fetch_opts) do
      {:ok, fetch_result} ->
        handle_new_fetch_response(fetch_result, clear_fetch_errors(state))

      {:error, :offset_out_of_range} ->
        case handle_offset_out_of_range(clear_fetch_errors(state)) do
          {:ok, new_state} -> handle_commit(:async_commit, new_state)
          {:error, reason} -> {:error, reason, state}
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp consume_cycle(%State{} = state) do
    case consume(state) do
      {:error, reason, new_state} ->
        handle_fetch_error(reason, new_state)

      {:noreply, new_state} ->
        {:noreply, new_state, 0}

      {:stop, _reason, _final_state} = stop ->
        stop
    end
  end

  defp handle_fetch_error(reason, %State{fetch_error_count: count} = state) do
    if Retry.fetch_retryable?(reason) do
      now = :erlang.monotonic_time(:milli_seconds)
      failing_since = state.fetch_failing_since || now
      delay = Retry.backoff_delay_jittered(count, fetch_retry_base_delay_ms(), fetch_retry_max_delay_ms())

      if log_fetch_retry?(count) do
        Logger.warning(
          "Fetch failed for #{state.topic}/#{state.partition} with #{inspect(reason)}, " <>
            "retrying in #{delay}ms (consecutive failures: #{count + 1})"
        )
      end

      new_state =
        %State{
          state
          | fetch_error_count: count + 1,
            fetch_retry_at: now + delay,
            fetch_failing_since: failing_since
        }
        |> maybe_report_unavailable(reason, now - failing_since)

      {:noreply, new_state, delay}
    else
      {:stop, reason, state}
    end
  end

  # Retries never stop, so a genuinely stuck partition would otherwise only show in logs; flag it once.
  defp maybe_report_unavailable(%State{fetch_unavailable_reported: true} = state, _reason, _elapsed), do: state

  defp maybe_report_unavailable(%State{} = state, reason, elapsed) do
    if elapsed >= fetch_unavailable_warn_ms() do
      Logger.error(
        "Partition #{state.topic}/#{state.partition} has been unavailable for #{elapsed}ms " <>
          "(#{inspect(reason)}); still retrying"
      )

      Telemetry.emit_partition_unavailable(state.group, state.topic, state.partition, elapsed, reason)
      %State{state | fetch_unavailable_reported: true}
    else
      state
    end
  end

  # Loud while the backoff ramps, then ~once a minute, so a stuck partition stays visible.
  defp log_fetch_retry?(count), do: count < 5 or rem(count, 12) == 0

  defp fetch_backoff_remaining(%State{fetch_retry_at: nil}), do: 0

  defp fetch_backoff_remaining(%State{fetch_retry_at: retry_at}),
    do: max(retry_at - :erlang.monotonic_time(:milli_seconds), 0)

  defp clear_fetch_errors(%State{fetch_error_count: 0, fetch_retry_at: nil, fetch_failing_since: nil} = state),
    do: state

  defp clear_fetch_errors(%State{} = state),
    do: %{
      state
      | fetch_error_count: 0,
        fetch_retry_at: nil,
        fetch_failing_since: nil,
        fetch_unavailable_reported: false
    }

  defp fetch_retry_base_delay_ms,
    do: Application.get_env(:kafka_ex, :fetch_retry_base_delay_ms, @fetch_retry_base_delay_ms)

  defp fetch_retry_max_delay_ms,
    do: Application.get_env(:kafka_ex, :fetch_retry_max_delay_ms, @fetch_retry_max_delay_ms)

  defp fetch_unavailable_warn_ms,
    do: Application.get_env(:kafka_ex, :fetch_unavailable_warn_ms, @fetch_unavailable_warn_ms)

  # The broker returned batches but every record was filtered out (control
  # batches — transaction commit/abort markers). Advance past them to next_offset, otherwise the
  # next fetch lands on the same batch and the consumer spins forever without
  # progressing. Mirrors brod/Java, which take the next position from batch
  # metadata. next_offset comes from the raw batches, so it is strictly ahead of
  # current_offset here.
  defp handle_new_fetch_response(%{records: [], next_offset: next}, %State{current_offset: current} = state)
       when is_integer(next) and is_integer(current) and next > current do
    new_state = %State{state | acked_offset: next, current_offset: next}
    handle_commit(:async_commit, new_state)
  end

  # No batches at all — caught up to the end of the partition. Do not advance;
  # poll again from the same offset.
  defp handle_new_fetch_response(%{records: []} = _fetch_result, state) do
    handle_commit(:async_commit, state)
  end

  defp handle_new_fetch_response(fetch_result, state) do
    handle_message_set(fetch_result.records, state)
  end

  defp handle_message_set(
         message_set,
         %State{
           consumer_module: consumer_module,
           consumer_state: consumer_state,
           group: group,
           topic: topic,
           partition: partition
         } = state
       ) do
    message_count = length(message_set)
    consumer_module_name = inspect(consumer_module)
    metadata = Telemetry.consumer_process_metadata(group, topic, partition, consumer_module_name)
    start_measurements = %{message_count: message_count}

    {sync_status, new_consumer_state} =
      :telemetry.span([:kafka_ex, :consumer, :process], Map.merge(metadata, start_measurements), fn ->
        result = consumer_module.handle_message_set(message_set, consumer_state)
        {result, %{commit_mode: elem(result, 0)}}
      end)

    %Record{offset: last_offset} = List.last(message_set)

    state_out = %State{
      state
      | consumer_state: new_consumer_state,
        acked_offset: last_offset + 1,
        current_offset: last_offset + 1
    }

    handle_commit(sync_status, state_out)
  end

  defp handle_offset_out_of_range(
         %State{
           client: client,
           topic: topic,
           partition: partition,
           auto_offset_reset: auto_offset_reset
         } = state
       ) do
    # The reset needs a live leader too, so it fails exactly when a leader is
    # moving. Hand that back rather than letting a MatchError kill the group.
    case reset_offset(client, topic, partition, auto_offset_reset) do
      {:ok, offset} ->
        report_offset_reset(state, auto_offset_reset, offset, :offset_out_of_range)

        {:ok,
         %State{
           state
           | current_offset: offset,
             committed_offset: offset,
             acked_offset: offset
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reset_offset(client, topic, partition, :earliest), do: KafkaExAPI.earliest_offset(client, topic, partition)
  defp reset_offset(client, topic, partition, :latest), do: KafkaExAPI.latest_offset(client, topic, partition)

  defp reset_offset(_client, topic, partition, :none),
    do: raise("Offset out of range while consuming topic #{topic}, partition #{partition}.")

  defp handle_commit(:sync_commit, %State{} = state), do: commit(state)

  defp handle_commit(
         :async_commit,
         %State{
           acked_offset: acked,
           committed_offset: committed,
           commit_threshold: threshold,
           last_commit: last_commit,
           commit_interval: interval
         } = state
       ) do
    case acked - committed do
      0 ->
        {:noreply, %State{state | last_commit: :erlang.monotonic_time(:milli_seconds)}}

      n when n >= threshold ->
        commit(state)

      _ ->
        if :erlang.monotonic_time(:milli_seconds) - last_commit >= interval do
          commit(state)
        else
          {:noreply, state}
        end
    end
  end

  defp commit(%State{acked_offset: offset, committed_offset: offset} = state) do
    {:noreply, state}
  end

  defp commit(state) do
    commit_with_retry(state)
  end

  # Single-shot commit used only by terminate/2 — no retries, no on-retry
  # callback, no fatal-error cast (group_manager_pid was already nilled by
  # terminate). The worker is dying regardless; best-effort only. Return
  # value is discarded by terminate/2.
  defp terminal_commit(%State{acked_offset: offset, committed_offset: offset}), do: :ok

  defp terminal_commit(
         %State{
           client: client,
           group: group,
           topic: topic,
           partition: partition,
           acked_offset: offset,
           api_versions: api_versions
         } = state
       ) do
    partitions = [%{partition_num: partition, offset: offset}]

    opts =
      state
      |> commit_membership_opts()
      |> VersionHelper.maybe_put_api_version(api_versions, :offset_commit)

    case KafkaExAPI.commit_offset(client, group, topic, partitions, opts) do
      {:ok, _} ->
        Logger.debug("Terminal commit offset #{topic}/#{partition}@#{offset} for #{group}")

      {:error, reason} ->
        Logger.warning("Terminal commit failed for #{topic}/#{partition}@#{offset} (#{group}): #{inspect(reason)}")
    end

    :ok
  end

  # Commit with retry and exponential backoff (issue #425)
  # Uses KafkaEx.Support.Retry for unified retry logic.
  # Handles transient failures like timeouts, especially in high-latency environments (AWS MSK)
  defp commit_with_retry(
         %State{
           client: client,
           group: group,
           topic: topic,
           partition: partition,
           acked_offset: offset
         } = state
       ) do
    partitions = [%{partition_num: partition, offset: offset}]

    opts =
      state
      |> commit_membership_opts()
      |> VersionHelper.maybe_put_api_version(state.api_versions, :offset_commit)

    commit_fn = fn ->
      KafkaExAPI.commit_offset(client, group, topic, partitions, opts)
    end

    retry_opts = [
      max_attempts: @commit_max_attempts,
      base_delay_ms: @commit_base_delay_ms,
      retryable?: &Retry.commit_retryable?/1,
      on_retry: fn error, attempt, delay ->
        Logger.warning(
          "Commit failed for #{topic}/#{partition}@#{offset} with #{inspect(error)}, " <>
            "retrying in #{delay}ms (attempt #{attempt}/#{@commit_max_attempts})"
        )
      end
    ]

    case Retry.with_retry(commit_fn, retry_opts) do
      {:ok, _result} ->
        Logger.debug("Committed offset #{topic}/#{partition}@#{offset} for #{group}")

        new_state = %State{
          state
          | committed_offset: offset,
            last_commit: :erlang.monotonic_time(:milli_seconds)
        }

        {:noreply, new_state}

      {:error, error} ->
        handle_commit_error(error, state)
    end
  end

  # The membership an OffsetCommit carries. A classic member sends exactly
  # what it always did; a KIP-848 member sends its epoch (v9+
  # generation_id_or_member_epoch) instead of a generation id.
  defp commit_membership_opts(%State{member_epoch: nil, member_id: member_id, generation_id: generation_id}),
    do: [member_id: member_id, generation_id: generation_id]

  defp commit_membership_opts(%State{member_epoch: member_epoch, member_id: member_id}),
    do: [member_id: member_id, member_epoch: member_epoch]

  # OffsetFetch v9+ carries member_id/member_epoch; the broker validates them
  # for consumer-protocol groups only. A classic member sends none, as before.
  defp fetch_membership_opts(%State{member_epoch: nil}), do: []

  defp fetch_membership_opts(%State{member_epoch: member_epoch, member_id: member_id}),
    do: [member_id: member_id, member_epoch: member_epoch]

  # Handle a commit failure. Dispatches on error classification:
  #
  #   * Terminal (KIP-345 :fenced_instance_id) — stop the consumer, do NOT
  #     rejoin. Another member has taken this group.instance.id; rejoining
  #     would split-brain. Matches Java/librdkafka/brod behaviour.
  #
  #   * Fatal (:illegal_generation, :unknown_member_id, and the v9+ codes in
  #     Retry.commit_fatal_error?/1) — cast to group manager
  #     with the stale generation_id so it can discard duplicate casts queued
  #     by sibling partitions, then self-stop.
  #
  #   * Transient — log and continue (retry next cycle).
  #
  # All three branches emit [:kafka_ex, :consumer, :commit_failed] telemetry
  # so operators can observe commit failures without parsing logs.
  defp handle_commit_error(error, %State{} = state) do
    cond do
      # KIP-848: the epoch this commit was built with is no longer accepted
      # for the partition (e.g. it moved on while the commit was in flight).
      # Not a reason to give the partition up: the offsets stay acked and the
      # next commit carries the current epoch. The group manager drives
      # revocation and fencing through its heartbeat. Classic: stays fatal.
      error == :stale_member_epoch and is_integer(state.member_epoch) ->
        handle_transient_commit_error(error, state)

      # A member epoch, but OffsetCommit pinned below v9 (see
      # RequestBuilder.offset_commit_request/2): every commit would fail the
      # same way, so stop instead of consuming without ever committing.
      error == :invalid_api_version ->
        handle_terminal_commit_error(error, state)

      Retry.commit_terminal_error?(error) ->
        handle_terminal_commit_error(error, state)

      Retry.commit_fatal_error?(error) ->
        handle_fatal_commit_error(error, state)

      true ->
        handle_transient_commit_error(error, state)
    end
  end

  defp handle_terminal_commit_error(error, %State{} = state) do
    %State{group: group, topic: topic, partition: partition, acked_offset: offset} = state

    Logger.error(
      "Commit terminal error #{inspect(error)} for #{topic}/#{partition}@#{offset} " <>
        "(group=#{group}): #{terminal_error_cause(error)}. Stopping without rejoin."
    )

    emit_commit_failed(state, error, :terminal)
    {:stop, {:shutdown, {:terminal_error, error}}, state}
  end

  defp terminal_error_cause(:fenced_instance_id), do: "another member owns this group.instance.id"

  defp terminal_error_cause(:invalid_api_version),
    do: "a member epoch needs OffsetCommit v9+, but a lower version is pinned (:api_versions)"

  defp terminal_error_cause(_error), do: "not recoverable by retrying or rejoining"

  defp handle_fatal_commit_error(error, %State{group_manager_pid: manager_pid} = state)
       when is_pid(manager_pid) do
    %State{topic: topic, partition: partition, acked_offset: offset} = state
    # What the failed commit was sent with: the member epoch for a KIP-848
    # member, the generation id for a classic one.
    gen = membership_generation(state)

    Logger.warning(
      "Commit fatal error #{inspect(error)} for #{topic}/#{partition}@#{offset}: " <>
        "signalling group manager (stale_gen=#{inspect(gen)}) and stopping"
    )

    emit_commit_failed(state, error, :fatal)
    GenServer.cast(manager_pid, {:rejoin_required, error, gen})
    {:stop, {:shutdown, {:rejoin_required, error}}, state}
  end

  defp handle_fatal_commit_error(error, %State{} = state) do
    Logger.error(
      "Commit fatal error #{inspect(error)} with no group_manager_pid; " <>
        "consumer cannot self-rejoin — stopping"
    )

    emit_commit_failed(state, error, :fatal)
    {:stop, {:shutdown, {:rejoin_required, error}}, state}
  end

  defp handle_transient_commit_error(error, %State{} = state) do
    %State{topic: topic, partition: partition, acked_offset: offset, group: group} = state

    Logger.error("Failed to commit offset #{topic}/#{partition}@#{offset} for #{group}: #{inspect(error)}")
    emit_commit_failed(state, error, :transient)
    {:noreply, state}
  end

  defp membership_generation(%State{member_epoch: nil, generation_id: generation_id}), do: generation_id
  defp membership_generation(%State{member_epoch: member_epoch}), do: member_epoch

  defp emit_commit_failed(%State{} = state, error, kind) do
    Telemetry.emit_commit_failed(state.group, state.topic, state.partition, state.acked_offset, kind, error)
  end

  # Test seam. Allows the test suite to exercise handle_commit_error without
  # mocking KafkaExAPI.commit_offset/5. Arg order matches handle_commit_error
  # (error, state) rather than the state-first convention, because this seam
  # is purely a 1:1 passthrough.
  @doc false
  @spec commit_for_test(Retry.error(), State.t()) ::
          {:noreply, State.t()} | {:stop, term(), State.t()}
  def commit_for_test(error, state), do: handle_commit_error(error, state)

  # OffsetFetch errors that reject the member itself (KIP-848 only).
  @membership_rejections [:stale_member_epoch, :unknown_member_id, :fenced_member_epoch]

  @load_offsets_max_attempts 6
  @default_load_offsets_retry_backoff_ms 500
  @load_offsets_max_delay_ms 5_000

  defp load_offsets(
         %State{
           client: client,
           group: group,
           topic: topic,
           partition: partition,
           auto_offset_reset: auto_offset_reset
         } = state
       ) do
    partitions = [%{partition_num: partition}]
    opts = state |> fetch_membership_opts() |> VersionHelper.maybe_put_api_version(state.api_versions, :offset_fetch)

    fetch = fn ->
      classify_committed_offset(KafkaExAPI.fetch_committed_offset(client, group, topic, partitions, opts))
    end

    result =
      Retry.with_retry(fetch,
        max_attempts: @load_offsets_max_attempts,
        base_delay_ms: load_offsets_backoff_ms(),
        max_delay_ms: @load_offsets_max_delay_ms,
        retryable?: &Retry.commit_retryable?/1,
        on_retry: fn reason, attempt, _delay ->
          Logger.warning(
            "Unable to load committed offsets for #{topic}/#{partition}: #{inspect(reason)}. " <>
              "Retrying (#{attempt}/#{@load_offsets_max_attempts})."
          )
        end
      )

    case result do
      {:ok, {:committed, offset}} ->
        %State{state | current_offset: offset, committed_offset: offset, acked_offset: offset}

      # KIP-848 (an epoch is set): the broker rejected the membership itself
      # (Kafka 4.4 ConsumerGroup.validateOffsetFetch: an epoch that is not the
      # member's current one is :stale_member_epoch, a member it does not know
      # is :unknown_member_id). Restarting with the same membership cannot
      # fix either; see load_offsets_cycle/1.
      {:error, reason} when reason in @membership_rejections and is_integer(state.member_epoch) ->
        {:membership_rejected, reason, state}

      {:ok, :reset} ->
        start_from_auto_offset_reset(state, auto_offset_reset)

      {:error, reason} ->
        raise "Unable to load committed offsets for #{topic}/#{partition}: #{inspect(reason)}"
    end
  end

  defp load_offsets_cycle(%State{} = state) do
    case load_offsets(state) do
      %State{} = loaded ->
        {:noreply,
         %State{
           loaded
           | last_commit: :erlang.monotonic_time(:milli_seconds),
             load_offsets_retry_at: nil,
             load_offsets_stale_count: 0
         }, 0}

      {:membership_rejected, :stale_member_epoch, %State{} = state} ->
        # The epoch this consumer was given is not the member's current one
        # (OffsetFetch needs an exact match), e.g. it moved on between the
        # owner starting the consumer and this fetch. The owner delivers the
        # current one with update_membership/3, which retries at once; until
        # then retry with an exponential backoff (capped).
        count = state.load_offsets_stale_count + 1
        delay = stale_epoch_backoff_ms(count)

        Logger.log(
          if(count == 1, do: :warning, else: :debug),
          "Committed offset fetch for #{state.topic}/#{state.partition} rejected with :stale_member_epoch " <>
            "(member_epoch=#{state.member_epoch}, attempt #{count}); waiting for a membership update, " <>
            "retrying in #{delay} ms"
        )

        state = %State{
          state
          | load_offsets_stale_count: count,
            load_offsets_retry_at: :erlang.monotonic_time(:milli_seconds) + delay
        }

        {:noreply, state, delay}

      {:membership_rejected, reason, %State{} = state} ->
        # The group no longer knows this member (session expired) or fenced
        # it: only a rejoin helps. Tell the owner and stop cleanly instead of
        # raising into a crash.
        Logger.warning(
          "Committed offset fetch for #{state.topic}/#{state.partition} rejected with #{inspect(reason)} " <>
            "(member_epoch=#{state.member_epoch}): signalling group manager and stopping"
        )

        if is_pid(state.group_manager_pid),
          do: GenServer.cast(state.group_manager_pid, {:rejoin_required, reason, state.member_epoch})

        {:stop, {:shutdown, {:rejoin_required, reason}}, state}
    end
  end

  defp load_offsets_retry_remaining(%State{load_offsets_retry_at: nil}), do: 0

  defp load_offsets_retry_remaining(%State{load_offsets_retry_at: retry_at}),
    do: max(retry_at - :erlang.monotonic_time(:milli_seconds), 0)

  @stale_epoch_max_backoff_ms 30_000

  @doc false
  # Backoff before the n-th (1-based) stale-epoch OffsetFetch retry:
  # base * 2^(n-1), capped at 30 s.
  @spec stale_epoch_backoff_ms(pos_integer()) :: non_neg_integer()
  def stale_epoch_backoff_ms(n) when is_integer(n) and n >= 1,
    do: min(load_offsets_backoff_ms() * Integer.pow(2, min(n - 1, 16)), @stale_epoch_max_backoff_ms)

  defp load_offsets_backoff_ms,
    do: Application.get_env(:kafka_ex, :load_offsets_retry_backoff_ms, @default_load_offsets_retry_backoff_ms)

  defp classify_committed_offset({:ok, [%{partition_offsets: [%{offset: offset, error_code: :no_error}]}]})
       when offset >= 0,
       do: {:ok, {:committed, offset}}

  defp classify_committed_offset({:ok, [%{partition_offsets: [%{error_code: :no_error}]}]}), do: {:ok, :reset}
  defp classify_committed_offset({:ok, []}), do: {:ok, :reset}
  defp classify_committed_offset({:error, reason}), do: {:error, reason}
  defp classify_committed_offset(other), do: {:error, {:unexpected_offset_fetch_response, other}}

  defp start_from_auto_offset_reset(%State{client: client, topic: topic, partition: partition} = state, reset) do
    offset =
      case reset do
        :latest ->
          {:ok, latest} = KafkaExAPI.latest_offset(client, topic, partition)
          latest

        :earliest ->
          {:ok, earliest} = KafkaExAPI.earliest_offset(client, topic, partition)
          earliest

        :none ->
          raise "No committed offset for #{topic}/#{partition} and auto_offset_reset is :none."
      end

    report_offset_reset(state, reset, offset, :no_committed_offset)

    %State{state | current_offset: offset, committed_offset: offset, acked_offset: offset}
  end

  defp report_offset_reset(%State{group: group, topic: topic, partition: partition}, reset, offset, reason) do
    Telemetry.emit_offset_reset(group, topic, partition, offset, reset, reason)

    case reason do
      :no_committed_offset ->
        Logger.info(
          "No committed offset for #{topic}/#{partition}; auto_offset_reset=#{inspect(reset)}, " <>
            "starting at offset #{offset}"
        )

      :offset_out_of_range ->
        Logger.warning(
          "Offset out of range for #{topic}/#{partition}; auto_offset_reset=#{inspect(reset)}, " <>
            "resetting to offset #{offset} (this skips or replays data)"
        )
    end
  end
end
