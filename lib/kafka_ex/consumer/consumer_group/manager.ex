defmodule KafkaEx.Consumer.ConsumerGroup.Manager do
  @moduledoc """
  Manages membership in a Kafka consumer group.

  This module implements the Kafka consumer group protocol, handling:

  - **Group Membership**: Joining and leaving consumer groups via JoinGroup/LeaveGroup requests
  - **Partition Assignment**: Coordinating partition distribution among group members
  - **Rebalancing**: Triggering rebalances when members join/leave or partitions change
  - **Heartbeats**: Maintaining group membership through periodic heartbeat messages

  ## Consumer Group Protocol Flow

  1. **Join Phase**: Send `JoinGroupRequest` to group coordinator. The coordinator
     blocks until all members have joined, then elects a leader.

  2. **Sync Phase**: The leader computes partition assignments and sends them via
     `SyncGroupRequest`. All members receive their assignments in the response.

  3. **Consume Phase**: Members consume from assigned partitions while sending
     periodic heartbeats to maintain membership.

  4. **Rebalance**: When the coordinator signals a rebalance (via heartbeat response),
     members stop consuming, commit offsets, and rejoin the group.

  ## Options

  The following options can be passed when starting a consumer group:

  - `:heartbeat_interval` - Interval between heartbeats in ms (default: 5000)
  - `:session_timeout` - Session timeout in ms (default: 30000)
  - `:session_timeout_padding` - Extra time added to request timeouts (default: 10000)
  - `:rebalance_timeout` - Time allowed for consumers to rejoin during rebalance (default: `session_timeout * 3`)
  - `:partition_assignment_callback` - Function for custom partition assignment (default: round-robin)
  - `:crash_rejoin_max_restarts` - Abnormal heartbeat-crash rejoins tolerated within
    `crash_rejoin_window_ms` before a terminal stop (default: 10; `:infinity` disables the bound)
  - `:crash_rejoin_window_ms` - Sliding window (ms) for `crash_rejoin_max_restarts` (default: 60000)

  Options can also be configured globally via application config under `:kafka_ex`.

  ## Crash-loop bound and recovery

  This bound governs only an **abnormal, uncatchable** heartbeat death — an
  `:kill` or an unstructured process crash that reaches the manager as a bare
  `{:EXIT, _, reason}` from the current heartbeat. Every *catchable* heartbeat
  failure is already classified
  by the heartbeat process itself (recoverable errors → rejoin; a code exception
  or `:fenced_instance_id` / `:group_authorization_failed` → immediate terminal
  stop), so those do not go through this counter.

  Such an abnormal death triggers an in-place rejoin (and a
  `[:kafka_ex, :consumer, :heartbeat_crash]` event). If they keep happening —
  more than `crash_rejoin_max_restarts` times within `crash_rejoin_window_ms`
  (defaults 10 / 60_000 ms; OTP `max_restarts`/`max_seconds` semantics) — the
  member stops terminally, emits `[:kafka_ex, :consumer, :member_terminated]` with
  `{:crash_loop, reason}`, and is torn down by its `one_for_all` /
  `max_restarts: 0` supervisor (whose `max_restarts: 0` is intentional and
  load-bearing for this escalation). The window is a pure sliding time-window: it
  is never reset on a successful rejoin, only aged out, so crashes are counted
  across the member's whole lifetime within the window. **Prior to this change the
  loop was unbounded; the bound is active by default.**

  To get automatic recovery from a terminal stop, run `ConsumerGroup` under your
  own supervisor (the common case); it is then restarted fresh under your restart
  strategy. **A `ConsumerGroup` started standalone will simply stop** (see
  `UPGRADING.md`). A correlated kill wave (e.g. a fleet-wide OOM or deploy) can
  trip many members at nearly the same time — per-member windowing limits each
  member, but jitter does NOT desync the trip itself, so size your own
  supervisor's `max_restarts`/`max_seconds` to absorb a simultaneous restart; it
  is the real backstop. The bound counts one crash per heartbeat failure, so with
  a short `heartbeat_interval` crashes accumulate faster within the window — raise
  `crash_rejoin_max_restarts` to give more headroom before tripping. Set
  `crash_rejoin_max_restarts: :infinity` to disable the bound.
  """
  use GenServer

  alias KafkaEx.API, as: KafkaExAPI
  alias KafkaEx.Client
  alias KafkaEx.Config
  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Consumer.ConsumerGroup.Heartbeat
  alias KafkaEx.Consumer.ConsumerGroup.PartitionAssignment
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Telemetry
  require Logger

  defmodule State do
    @moduledoc false
    defstruct [
      :supervisor_pid,
      :client,
      :heartbeat_interval,
      :session_timeout,
      :session_timeout_padding,
      :rebalance_timeout,
      :gen_consumer_module,
      :consumer_module,
      :consumer_opts,
      :partition_assignment_callback,
      :group_name,
      :topics,
      :member_id,
      :leader_id,
      :consumer_supervisor_pid,
      :members,
      :generation_id,
      :assignments,
      :heartbeat_timer,
      :sync_retry_backoff_ms,
      :crash_rejoin_max_jitter_ms,
      :crash_rejoin_max_restarts,
      :crash_rejoin_window_ms,
      :group_instance_id,
      sync_failures: 0,
      heartbeat_crash_times: []
    ]

    @type t :: %__MODULE__{
            supervisor_pid: pid(),
            client: pid(),
            heartbeat_interval: pos_integer(),
            session_timeout: pos_integer(),
            session_timeout_padding: pos_integer(),
            rebalance_timeout: pos_integer(),
            gen_consumer_module: module(),
            consumer_module: module(),
            consumer_opts: Keyword.t(),
            partition_assignment_callback: function(),
            group_name: binary(),
            topics: [binary()],
            member_id: binary() | nil,
            leader_id: binary() | nil,
            consumer_supervisor_pid: pid() | nil,
            members: [binary()] | nil,
            generation_id: integer() | nil,
            assignments: [{binary(), integer()}] | nil,
            heartbeat_timer: pid() | nil,
            sync_retry_backoff_ms: non_neg_integer() | nil,
            crash_rejoin_max_jitter_ms: non_neg_integer() | nil,
            crash_rejoin_max_restarts: non_neg_integer() | :infinity | nil,
            crash_rejoin_window_ms: non_neg_integer() | nil,
            group_instance_id: binary() | nil,
            sync_failures: non_neg_integer(),
            heartbeat_crash_times: [integer()]
          }
  end

  @heartbeat_interval 5_000
  @session_timeout 30_000
  @session_timeout_padding 10_000
  # Default rebalance_timeout multiplier (relative to session_timeout)
  # In Java client, rebalance_timeout = max.poll.interval.ms (default 5 min)
  # We use session_timeout * 3 as reasonable default
  @rebalance_timeout_multiplier 3

  @max_join_retries 6
  @max_topic_retries 5

  # Default backoff before a SyncGroup-driven rejoin (see handle_sync_error/3);
  # overridable per consumer group via the `:sync_retry_backoff_ms` opt. Defined
  # here (before init/1) because init reads it as the default.
  @sync_retry_backoff_ms 1_000

  # Max consecutive recoverable SyncGroup failures before giving up (the counter
  # resets on a successful sync). Bounds the rejoin loop the way @max_join_retries
  # bounds join — a persistent failure escalates to the supervisor rebuild.
  @max_sync_retries 3

  # Max jitter (ms) before an abnormal-crash rejoin, to desync a fleet hitting a
  # correlated heartbeat-kill. Per-group via `:crash_rejoin_max_jitter_ms`; 0 off.
  @crash_rejoin_max_jitter_ms 1_000

  # Heartbeat-crash-loop bound (OTP max_restarts/max_seconds applied to the
  # abnormal-crash rejoin path): more than @crash_rejoin_max_restarts abnormal
  # heartbeat-crash rejoins within @crash_rejoin_window_ms stops the member
  # terminally instead of looping forever. Per-group via :crash_rejoin_max_restarts
  # / :crash_rejoin_window_ms; :infinity restarts disables the bound. Raising the
  # restart count (not widening the window) is the lever to tolerate a transient
  # kill burst — a wider window accumulates more crashes and trips more eagerly.
  @crash_rejoin_max_restarts 10
  @crash_rejoin_window_ms 60_000

  # Gets a value from opts, falling back to application config, then to default
  defp get_with_default(opts, key, default) do
    Keyword.get(opts, key, Application.get_env(:kafka_ex, key, default))
  end

  @type assignments :: [{binary(), integer()}]

  # Client API

  @doc false
  @spec start_link({{module, module}, binary, [binary], Keyword.t()}) :: GenServer.on_start()
  def start_link({{gen_consumer_module, consumer_module}, group_name, topics, opts}) do
    gen_server_opts = Keyword.get(opts, :gen_server_opts, [])
    consumer_opts = Keyword.drop(opts, [:gen_server_opts])
    opts = {{gen_consumer_module, consumer_module}, group_name, topics, consumer_opts}
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  # GenServer callbacks

  # Dialyzer reports no_return because KafkaEx.Client.start_link can raise on init failure.
  # However, the function does return normally when client starts successfully.
  @dialyzer {:nowarn_function, init: 1}
  def init({{gen_consumer_module, consumer_module}, group_name, topics, opts}) do
    heartbeat_interval = get_with_default(opts, :heartbeat_interval, @heartbeat_interval)
    session_timeout = get_with_default(opts, :session_timeout, @session_timeout)
    session_timeout_padding = get_with_default(opts, :session_timeout_padding, @session_timeout_padding)
    rebalance_timeout = get_with_default(opts, :rebalance_timeout, session_timeout * @rebalance_timeout_multiplier)
    sync_retry_backoff_ms = get_with_default(opts, :sync_retry_backoff_ms, @sync_retry_backoff_ms)
    crash_rejoin_max_jitter_ms = get_with_default(opts, :crash_rejoin_max_jitter_ms, @crash_rejoin_max_jitter_ms)
    crash_rejoin_max_restarts = get_with_default(opts, :crash_rejoin_max_restarts, @crash_rejoin_max_restarts)
    crash_rejoin_window_ms = get_with_default(opts, :crash_rejoin_window_ms, @crash_rejoin_window_ms)

    group_instance_id =
      opts
      |> get_with_default(:group_instance_id, nil)
      |> Config.resolve_group_instance_id()
      |> Config.validate_group_instance_id!()

    partition_assignment_callback =
      Keyword.get(opts, :partition_assignment_callback, &PartitionAssignment.round_robin/2)

    supervisor_pid = Keyword.fetch!(opts, :supervisor_pid)

    consumer_opts =
      Keyword.drop(opts, [
        :supervisor_pid,
        :heartbeat_interval,
        :session_timeout,
        :session_timeout_padding,
        :rebalance_timeout,
        :partition_assignment_callback,
        :client,
        :sync_retry_backoff_ms,
        :crash_rejoin_max_jitter_ms,
        :crash_rejoin_max_restarts,
        :crash_rejoin_window_ms,
        :group_instance_id
      ])

    # Use Config defaults for connection options if not provided
    client_opts =
      [
        uris: Keyword.get(opts, :uris) || Config.brokers(),
        use_ssl: Keyword.get(opts, :use_ssl, Config.use_ssl()),
        ssl_options: Keyword.get(opts, :ssl_options, Config.ssl_options()),
        auth: Keyword.get(opts, :auth) || Config.auth_config(),
        consumer_group: group_name,
        initial_topics: topics
      ]

    # A pre-supplied `:client` (test-only dependency injection) bypasses starting
    # a real client; production callers never set it, so this is inert in prod.
    client_result =
      case Keyword.get(opts, :client) do
        nil -> Client.start_link(client_opts, :no_name)
        pid when is_pid(pid) -> {:ok, pid}
      end

    case client_result do
      {:ok, client} ->
        maybe_warn_static_membership_unsupported(client, group_instance_id)

        state = %State{
          supervisor_pid: supervisor_pid,
          client: client,
          heartbeat_interval: heartbeat_interval,
          session_timeout: session_timeout,
          session_timeout_padding: session_timeout_padding,
          rebalance_timeout: rebalance_timeout,
          consumer_module: consumer_module,
          gen_consumer_module: gen_consumer_module,
          partition_assignment_callback: partition_assignment_callback,
          consumer_opts: consumer_opts,
          group_name: group_name,
          topics: topics,
          member_id: nil,
          sync_retry_backoff_ms: sync_retry_backoff_ms,
          crash_rejoin_max_jitter_ms: crash_rejoin_max_jitter_ms,
          crash_rejoin_max_restarts: crash_rejoin_max_restarts,
          crash_rejoin_window_ms: crash_rejoin_window_ms,
          group_instance_id: group_instance_id
        }

        Process.flag(:trap_exit, true)

        {:ok, state, 0}

      {:error, reason} ->
        {:stop, reason}

      :ignore ->
        :ignore
    end
  end

  ######################################################################
  # handle_call clauses - mostly for ops queries
  def handle_call(:generation_id, _from, state) do
    {:reply, state.generation_id, state}
  end

  def handle_call(:member_id, _from, state) do
    {:reply, state.member_id, state}
  end

  def handle_call(:leader_id, _from, state) do
    {:reply, state.leader_id, state}
  end

  def handle_call(:am_leader, _from, state) do
    {:reply, state.leader_id && state.member_id == state.leader_id, state}
  end

  def handle_call(:assignments, _from, state) do
    {:reply, state.assignments, state}
  end

  def handle_call(:consumer_supervisor_pid, _from, state) do
    {:reply, state.consumer_supervisor_pid, state}
  end

  def handle_call(:group_name, _from, state) do
    {:reply, state.group_name, state}
  end

  # Triggered by GenConsumer when OffsetCommit returns a fatal rejoin-required
  # error (:illegal_generation, :unknown_member_id).
  # Runs the same path as a heartbeat-driven rebalance.
  #
  # The cast carries the stale generation_id the consumer saw. This lets us
  # distinguish "stale" duplicates (same generation as ours before the
  # rebalance) from "fresh" casts (consumers on a NEW generation that failed)
  # — see drain_stale_rejoin_casts/2.
  #
  # Per Java/librdkafka/brod: on :unknown_member_id the broker has explicitly
  # forgotten us, so we must reset member_id before rejoining so the broker
  # assigns a fresh one. We also reset generation_id for symmetry with Java's
  # resetGeneration() on :illegal_generation.
  #
  # Tagged with {:commit_fatal, reason} so operators can distinguish this
  # trigger from :heartbeat_timeout in the [:kafka_ex, :consumer, :rebalance]
  # telemetry stream.
  def handle_cast({:rejoin_required, reason, stale_gen}, %State{} = state) do
    Logger.warning(
      "Consumer signalled rejoin-required (#{inspect(reason)}, stale_gen=#{inspect(stale_gen)}); " <>
        "initiating rebalance."
    )

    pre_rebalance_gen = state.generation_id
    state = reset_generation(state, reason)
    {:ok, new_state} = rebalance(state, {:commit_fatal, reason})
    dropped = drain_stale_rejoin_casts(0, pre_rebalance_gen)

    if dropped > 0 do
      Logger.info("Coalesced #{dropped} stale :rejoin_required cast(s) after rebalance.")
    end

    {:noreply, new_state}
  end

  # Legacy 2-tuple form (pre-B5 external callers). Treat as stale_gen=nil,
  # which only drains casts matching the pre-rebalance generation.
  def handle_cast({:rejoin_required, reason}, %State{} = state) do
    handle_cast({:rejoin_required, reason, nil}, state)
  end

  # Per Java ConsumerCoordinator.resetGeneration(): nil out member_id and
  # generation_id on fatal member-identity errors so the next JoinGroup
  # request forces a fresh broker-assigned identity.
  defp reset_generation(%State{} = state, :unknown_member_id) do
    %State{state | member_id: nil, generation_id: nil}
  end

  defp reset_generation(%State{} = state, :illegal_generation) do
    %State{state | generation_id: nil}
  end

  defp reset_generation(%State{} = state, _reason), do: state

  # Drain casts identifiable as stale:
  #   * Tagged with any generation <= pre_rebalance_gen (stale — from this
  #     rebalance cycle OR from even-older leaked consumers that outlived a
  #     prior rebalance's stop_consumer for any reason).
  #   * Tagged with nil (sender couldn't read its generation — treat as stale
  #     rather than trigger yet another rebalance).
  #   * Legacy 2-tuple form (pre-BUG-1 senders — treat as stale).
  #
  # Casts tagged with a generation STRICTLY GREATER than pre_rebalance_gen
  # survive the drain — they come from fresh GenConsumers on the new (or
  # later) generation whose commit genuinely failed and still need handling.
  defp drain_stale_rejoin_casts(count, nil) do
    # No pre-rebalance generation to compare against — drain every rejoin
    # cast we find. This is the init-state path where we haven't joined yet
    # or for test seams passing nil.
    receive do
      {:"$gen_cast", {:rejoin_required, _reason, _stale_gen}} ->
        drain_stale_rejoin_casts(count + 1, nil)

      {:"$gen_cast", {:rejoin_required, _reason}} ->
        drain_stale_rejoin_casts(count + 1, nil)
    after
      0 -> count
    end
  end

  defp drain_stale_rejoin_casts(count, pre_rebalance_gen) when is_integer(pre_rebalance_gen) do
    receive do
      {:"$gen_cast", {:rejoin_required, _reason, stale_gen}}
      when is_integer(stale_gen) and stale_gen <= pre_rebalance_gen ->
        drain_stale_rejoin_casts(count + 1, pre_rebalance_gen)

      {:"$gen_cast", {:rejoin_required, _reason, nil}} ->
        drain_stale_rejoin_casts(count + 1, pre_rebalance_gen)

      {:"$gen_cast", {:rejoin_required, _reason}} ->
        drain_stale_rejoin_casts(count + 1, pre_rebalance_gen)
    after
      0 -> count
    end
  end

  # Test seam. Drains the calling process's mailbox of :rejoin_required
  # casts that match stale_gen (default nil, matches the legacy 2-tuple form).
  @doc false
  @spec drain_for_test(integer() | nil) :: non_neg_integer()
  def drain_for_test(stale_gen \\ nil), do: drain_stale_rejoin_casts(0, stale_gen)

  ######################################################################

  # If `member_id` and `generation_id` aren't set, we haven't yet joined the
  # group. `member_id` and `generation_id` are initialized by
  # `JoinGroupResponse`.
  def handle_info(:timeout, %State{generation_id: nil, member_id: nil} = state) do
    {:ok, new_state} = join(state)
    {:noreply, new_state}
  end

  # If the heartbeat gets an error, we need to rebalance.
  def handle_info({:EXIT, timer, {:shutdown, :rebalance}}, %State{heartbeat_timer: timer} = state) do
    {:ok, state} = rebalance(state, :heartbeat_timeout)
    {:noreply, state}
  end

  # The heartbeat hit a "rejoin now, with identity reset" signal:
  # :illegal_generation (stale generation — keep member_id) or
  # :unknown_member_id (coordinator forgot us — clear member_id). reset_generation/2
  # applies the right reset, then we rejoin in place. This is recovery, NOT a crash,
  # so the one_for_all / max_restarts: 0 consumer-group supervisor is never torn down.
  def handle_info({:EXIT, timer, {:shutdown, {:rejoin, reason}}}, %State{heartbeat_timer: timer} = state) do
    Logger.warning("Heartbeat signalled rejoin (#{inspect(reason)}), resetting identity and rejoining group")
    state = reset_generation(state, reason)
    {:ok, state} = rebalance(state, {:heartbeat_rejoin, reason})
    {:noreply, state}
  end

  # Terminal heartbeat error (fenced instance id, group-auth, or a code-defect
  # crash): rejoining would be futile, so stop cleanly (emits member_terminated).
  def handle_info({:EXIT, timer, {:shutdown, {:terminal, reason}}}, %State{heartbeat_timer: timer} = state) do
    Logger.error("Heartbeat hit terminal error #{inspect(reason)}, stopping consumer group member")
    stop_terminal(state, reason)
  end

  # If the CURRENT heartbeat gets an error, attempt to rejoin for recoverable
  # errors (coordinator changed or temporarily unavailable). Binding `timer` to
  # the current heartbeat_timer keeps a stale heartbeat's error EXIT from
  # triggering a spurious rebalance — it falls through to the stale-timer clause.
  def handle_info({:EXIT, timer, {:shutdown, {:error, reason}}}, %State{heartbeat_timer: timer} = state) do
    if recoverable_error?(reason) do
      Logger.warning("Heartbeat failed with #{inspect(reason)}, attempting to rejoin group")
      {:ok, new_state} = rebalance(state, {:heartbeat_error, reason})
      {:noreply, new_state}
    else
      Logger.error("Heartbeat failed with unrecoverable error: #{inspect(reason)}")
      {:stop, {:shutdown, {:error, reason}}, state}
    end
  end

  # If the Client dies unexpectedly (not from our own terminate/2, which
  # unlinks first), this is unrecoverable — every subsequent API call would
  # timeout. Stop loudly so the supervisor can decide what to do.
  def handle_info({:EXIT, pid, reason}, %State{client: pid} = state) do
    Logger.error("Manager's Kafka client died unexpectedly: #{inspect(reason)}")
    {:stop, {:shutdown, {:client_died, reason}}, state}
  end

  # Stale heartbeat-timer EXITs: the timer already signalled us once (and we
  # started a new one), but the underlying Erlang EXIT signal for the old
  # pid is now arriving after the swap. Match any `{:shutdown, _}` reason
  # from a non-current heartbeat pid and silently drop it. Without this,
  # the default `handle_info` logs a warning for every rebalance cycle.
  def handle_info({:EXIT, pid, {:shutdown, _}}, %State{heartbeat_timer: current} = state)
      when pid != current do
    {:noreply, state}
  end

  # Clean exit from a non-current linked child (usually a replaced heartbeat
  # timer); abnormal EXITs fall to the two clauses below.
  def handle_info({:EXIT, pid, :normal}, %State{} = state) do
    Logger.debug("Manager trapping normal :EXIT from linked pid #{inspect(pid)}")
    {:noreply, state}
  end

  # Current heartbeat died abnormally — the residual heartbeat.ex can't catch
  # (`:kill`). Rejoin keeping member_id (no protocol reason to reset it),
  # jittered to desync correlated kills — unless this member is crash-looping,
  # in which case stop terminally and let the supervisor decide.
  def handle_info({:EXIT, timer, reason}, %State{heartbeat_timer: timer} = state) do
    case register_heartbeat_crash(state) do
      {:trip, state} ->
        Logger.error(
          "Heartbeat crashed #{length(state.heartbeat_crash_times)} times within " <>
            "#{state.crash_rejoin_window_ms}ms (limit #{state.crash_rejoin_max_restarts}); " <>
            "stopping member instead of rejoining"
        )

        stop_terminal(state, {:crash_loop, reason})

      {:rejoin, state} ->
        Telemetry.emit_heartbeat_crash(
          state.group_name,
          state.member_id || "",
          reason,
          length(state.heartbeat_crash_times)
        )

        Logger.warning("Heartbeat exited abnormally (#{inspect(reason)}); rejoining group")
        maybe_crash_rejoin_jitter(state)
        {:ok, state} = rebalance(state, {:heartbeat_crash, reason})
        {:noreply, state}
    end
  end

  # Catch-all so no {:EXIT, _, _} ever FunctionClauseErrors the Manager (which
  # max_restarts: 0 would turn into a no-restart teardown). The only live source
  # is a stale heartbeat we already replaced — drop it.
  def handle_info({:EXIT, pid, reason}, %State{} = state) do
    Logger.debug("Manager dropping abnormal :EXIT from non-current linked pid #{inspect(pid)}: #{inspect(reason)}")
    {:noreply, state}
  end

  # Deferred terminal stop requested from inside the synchronous join/sync call
  # chain, which can't return {:stop, _, _} directly. handle_sync_error/3 sends
  # this on a terminal SyncGroup error (:fenced_instance_id) so the member stops
  # cleanly without rejoining.
  def handle_info({:terminal_shutdown, reason}, %State{} = state) do
    stop_terminal(state, reason)
  end

  # When terminating, inform the group coordinator that this member is leaving
  # the group so that the group can rebalance without waiting for a session
  # timeout.
  def terminate(reason, %State{generation_id: nil, member_id: nil} = state) do
    emit_termination_telemetry(reason, state)
    Process.unlink(state.client)
    GenServer.stop(state.client, :normal)
  end

  def terminate(reason, %State{} = state) do
    emit_termination_telemetry(reason, state)

    if is_nil(state.group_instance_id) do
      {:ok, _state} = leave(state)
    else
      Logger.debug(
        "Static member (group_instance_id=#{inspect(state.group_instance_id)}); skipping LeaveGroup " <>
          "so the broker retains the assignment until session timeout"
      )
    end

    Process.unlink(state.client)
    GenServer.stop(state.client, :normal)

    # should be at end because of race condition (stop heartbeat while it is shutting down)
    # if race condition happens, client will be abandoned
    stop_heartbeat_timer(state)
  end

  # Single choke point for member_terminated: every permanent, non-graceful
  # death (terminal error, unrecoverable heartbeat error, client death, or a
  # crash/give-up raise) flows through terminate/2; graceful :normal/:shutdown
  # classify to nil and stay silent.
  defp emit_termination_telemetry(reason, %State{} = state) do
    case termination_reason(reason) do
      nil -> :ok
      classified -> Telemetry.emit_member_terminated(state.group_name, state.member_id || "", classified)
    end
  end

  defp termination_reason({:shutdown, {:terminal, reason}}), do: reason
  defp termination_reason({:shutdown, {:error, reason}}), do: {:error, reason}
  defp termination_reason({:shutdown, {:client_died, reason}}), do: {:client_died, reason}
  defp termination_reason({exception, stacktrace}) when is_list(stacktrace), do: {:crashed, crash_module(exception)}
  defp termination_reason(_), do: nil

  defp crash_module(%{__struct__: module}), do: module
  defp crash_module(_), do: :unknown

  ### Helpers

  # `JoinGroupRequest` is used to set the active members of a group. The
  # response blocks until the broker has decided that it has a full list of
  # group members. This requires that all active members send a
  # `JoinGroupRequest`. For active members, this is triggered by the broker
  # responding to a heartbeat with a `:rebalance_in_progress` error code. If
  # any group members fail to send a `JoinGroupRequest` before the session
  # timeout expires, then those group members are removed from the group and
  # synchronization continues without them.
  #
  # `JoinGroupResponse` tells each member its unique member ID as well as the
  # group's current generation ID. The broker will pick one group member to be
  # the leader, which is responsible for assigning partitions to all of the
  # group members. Once a `JoinGroupResponse` is received, all group members
  # must send a `SyncGroupRequest` (see sync/2).
  defp join(state), do: join(state, 1)

  defp join(
         %State{
           client: client,
           session_timeout: session_timeout,
           session_timeout_padding: session_timeout_padding,
           rebalance_timeout: rebalance_timeout,
           group_name: group_name,
           topics: topics,
           member_id: member_id,
           group_instance_id: group_instance_id
         } = state,
         attempt_number
       ) do
    opts = [
      topics: topics,
      session_timeout: session_timeout,
      rebalance_timeout: rebalance_timeout,
      timeout: session_timeout + session_timeout_padding,
      group_instance_id: group_instance_id
    ]

    join_response = KafkaExAPI.join_group(client, group_name, member_id || "", opts)

    # Handle response - retry for recoverable errors, crash for unrecoverable
    case join_response do
      {:ok, %KafkaEx.Messages.JoinGroup{} = response} ->
        on_successful_join(state, response)

      {:error, reason} ->
        handle_join_error(state, group_name, reason, attempt_number)
    end
  end

  # :unknown_member_id — the broker forgot us; reset member_id and rejoin fresh
  # (brod should_reset_member_id / Java resetStateAndGeneration) rather than
  # crash. Other recoverable errors retry; the rest raise.
  defp handle_join_error(state, group_name, reason, attempt_number) do
    cond do
      reason == :unknown_member_id ->
        handle_recoverable_join_error(reset_generation(state, :unknown_member_id), group_name, reason, attempt_number)

      recoverable_error?(reason) ->
        handle_recoverable_join_error(state, group_name, reason, attempt_number)

      true ->
        raise KafkaEx.JoinGroupError, group_name: group_name, reason: reason
    end
  end

  defp handle_recoverable_join_error(state, _group_name, reason, attempt_number)
       when attempt_number >= @max_join_retries do
    raise KafkaEx.JoinGroupRetriesExhaustedError,
      group_name: state.group_name,
      last_error: reason,
      attempts: @max_join_retries
  end

  defp handle_recoverable_join_error(state, group_name, reason, attempt_number) do
    sleep_time = calculate_join_backoff(attempt_number)

    Logger.warning(
      "Unable to join consumer group #{inspect(group_name)}: #{inspect(reason)}. " <>
        "Will retry in #{div(sleep_time, 1000)}s (attempt #{attempt_number}/#{@max_join_retries})"
    )

    :timer.sleep(sleep_time)
    join(state, attempt_number + 1)
  end

  # Exponential backoff for join retries: 1s, 2s, 4s... capped at 10s
  @join_retry_base_delay_ms 1000
  @join_retry_max_delay_ms 10_000

  defp calculate_join_backoff(attempt_number) do
    delay = @join_retry_base_delay_ms * round(:math.pow(2, attempt_number - 1))
    min(delay, @join_retry_max_delay_ms)
  end

  defp on_successful_join(%State{} = state, join_response) do
    Logger.debug(
      "Joined consumer group #{state.group_name} generation:#{join_response.generation_id}-#{join_response.member_id}"
    )

    new_state = %State{
      state
      | leader_id: join_response.leader_id,
        member_id: join_response.member_id,
        generation_id: join_response.generation_id
    }

    is_leader = join_response.leader_id == join_response.member_id

    assignments =
      if is_leader do
        partitions = assignable_partitions(new_state)
        member_ids = Enum.map(join_response.members, & &1.member_id)
        assign_partitions(new_state, member_ids, partitions)
      else
        []
      end

    sync(new_state, assignments)
  end

  # `SyncGroupRequest` is used to distribute partition assignments to all group
  # members. All group members must send this request after receiving a
  # response to a `JoinGroupRequest`. The request blocks until assignments are
  # provided by the leader. The leader sends partition assignments (given by
  # the `assignments` parameter) as part of its `SyncGroupRequest`. For all
  # other members, `assignments` must be empty.
  #
  # `SyncGroupResponse` contains the individual member's partition assignments.
  # Upon receiving a successful `SyncGroupResponse`, a group member is free to
  # start consuming from its assigned partitions, but must send periodic
  # heartbeats to the coordinating broker.
  defp sync(
         %State{
           client: client,
           group_name: group_name,
           member_id: member_id,
           generation_id: generation_id,
           group_instance_id: group_instance_id,
           session_timeout: session_timeout,
           session_timeout_padding: session_timeout_padding
         } = state,
         assignments
       ) do
    # Convert assignments to protocol-agnostic format (protocol layer handles Kayrock conversion)
    group_assignment = format_assignments_for_sync_group(assignments)

    opts = [
      group_assignment: group_assignment,
      timeout: session_timeout + session_timeout_padding,
      group_instance_id: group_instance_id
    ]

    case KafkaExAPI.sync_group(client, group_name, generation_id, member_id, opts) do
      {:ok, sync_response} ->
        # A successful sync clears the consecutive-recoverable-failure counter.
        state = %State{state | sync_failures: 0}
        {:ok, state} = start_heartbeat_timer(state)
        {:ok, state} = stop_consumer(state)
        consumer_assignments = extract_consumer_assignments(sync_response.partition_assignments)
        start_consumer(state, consumer_assignments)

      {:error, :rebalance_in_progress} ->
        rebalance(state, :rebalance_in_progress)

      {:error, reason} ->
        handle_sync_error(state, group_name, reason)
    end
  end

  defp handle_sync_error(%State{} = state, group_name, reason)
       when reason in [:fenced_instance_id, :group_authorization_failed] do
    Logger.error("SyncGroup for #{inspect(group_name)} returned terminal #{inspect(reason)}; stopping without rejoin")

    send(self(), {:terminal_shutdown, reason})
    {:ok, state}
  end

  defp handle_sync_error(%State{} = state, group_name, reason) do
    cond do
      not sync_rejoinable?(reason) ->
        raise KafkaEx.SyncGroupError, group_name: group_name, reason: reason

      state.sync_failures >= @max_sync_retries ->
        raise KafkaEx.SyncGroupRetriesExhaustedError,
          group_name: group_name,
          last_error: reason,
          attempts: @max_sync_retries

      true ->
        attempt = state.sync_failures + 1

        Logger.warning(
          "Unable to sync consumer group #{inspect(group_name)}: #{inspect(reason)}. " <>
            "Rejoining in #{div(state.sync_retry_backoff_ms, 1000)}s (attempt #{attempt}/#{@max_sync_retries})."
        )

        :timer.sleep(state.sync_retry_backoff_ms)
        state = reset_generation(state, reason)
        rebalance(%State{state | sync_failures: attempt}, {:sync_error, reason})
    end
  end

  defp sync_rejoinable?(:illegal_generation), do: true
  defp sync_rejoinable?(:unknown_member_id), do: true
  defp sync_rejoinable?(reason), do: recoverable_error?(reason)

  # Convert assignments from Manager format to protocol-agnostic format for sync_group request
  # Input: [{member_id, [{topic, [partition_ids]}]}]
  # Output: [%{member_id: ..., topic_partitions: [{topic, [partitions]}]}]
  # The protocol layer handles conversion to Kayrock structs
  defp format_assignments_for_sync_group(assignments) do
    Enum.map(assignments, fn {member_id, topic_partitions} ->
      %{
        member_id: member_id,
        topic_partitions: topic_partitions
      }
    end)
  end

  # Convert partition_assignments from SyncGroup response to consumer format
  # Input: [%PartitionAssignment{topic: ..., partitions: [...]}]
  # Output: [{topic, partition}]
  defp extract_consumer_assignments(partition_assignments) do
    Enum.flat_map(partition_assignments, fn assignment ->
      Enum.map(assignment.partitions, fn partition ->
        {assignment.topic, partition}
      end)
    end)
  end

  # `LeaveGroupRequest` is used to voluntarily leave a group. This tells the
  # broker that the member is leaving the group without having to wait for the
  # session timeout to expire. Leaving a group triggers a rebalance for the
  # remaining group members.
  defp leave(%State{client: client, group_name: group_name, member_id: member_id} = state) do
    case KafkaExAPI.leave_group(client, group_name, member_id) do
      {:ok, %KafkaEx.Messages.LeaveGroup{}} ->
        Logger.debug("Left consumer group #{group_name}")

      {:error, reason} ->
        Logger.warning("Received error #{inspect(reason)}, consumer group manager will exit regardless.")
    end

    {:ok, state}
  end

  # When instructed that a rebalance is in progress, a group member must rejoin
  # the group with `JoinGroupRequest` (see join/1). To keep the state
  # synchronized during the join/sync phase, each member pauses its consumers
  # and commits its offsets before rejoining the group.
  defp rebalance(%State{} = state, reason) do
    Telemetry.emit_rebalance(
      state.group_name,
      state.member_id || "",
      state.generation_id,
      reason
    )

    {:ok, state} = stop_heartbeat_timer(state)
    {:ok, state} = stop_consumer(state)
    join(state)
  end

  defp stop_terminal(%State{} = state, reason) do
    {:stop, {:shutdown, {:terminal, reason}}, state}
  end

  defp maybe_crash_rejoin_jitter(%State{crash_rejoin_max_jitter_ms: max})
       when is_integer(max) and max > 0 do
    :timer.sleep(:rand.uniform(max))
  end

  defp maybe_crash_rejoin_jitter(%State{}), do: :ok

  # Sliding-window restart intensity: keep crash timestamps within the window,
  # trip terminal once they exceed the budget (current crash included). The
  # window is never cleared on a successful rejoin — it only ages out — so the
  # bound counts crashes across the member's whole lifetime within the window.
  defp register_heartbeat_crash(%State{crash_rejoin_max_restarts: max, crash_rejoin_window_ms: window} = state)
       when is_integer(max) and max >= 0 and is_integer(window) do
    now = System.monotonic_time(:millisecond)
    recent = Enum.filter([now | state.heartbeat_crash_times], &(now - &1 <= window))
    state = %State{state | heartbeat_crash_times: recent}

    if length(recent) > max, do: {:trip, state}, else: {:rejoin, state}
  end

  # :infinity (or an unset/invalid budget or window) means never bound — loop
  # forever (old behaviour).
  defp register_heartbeat_crash(%State{} = state), do: {:rejoin, state}

  ### Timer Management

  # Starts a heartbeat process to send heartbeats in the background to keep the
  # consumers active even if it takes a long time to process a batch of messages.
  defp start_heartbeat_timer(%State{} = state) do
    {:ok, timer} = Heartbeat.start_link(state)
    {:ok, %State{state | heartbeat_timer: timer}}
  end

  defp stop_heartbeat_timer(%State{heartbeat_timer: nil} = state), do: {:ok, state}

  defp stop_heartbeat_timer(%State{heartbeat_timer: heartbeat_timer} = state) do
    # :gen_server.stop/1 can exit :noproc if the timer dies between our
    # Process.alive? check and the stop call (classic TOCTOU). It can also
    # exit :timeout. Either way, we only care that the process ends up dead
    # — which it will regardless. Absorb the exit so the Manager doesn't
    # crash under the one_for_all/max_restarts: 0 supervisor.
    if Process.alive?(heartbeat_timer) do
      try do
        :gen_server.stop(heartbeat_timer)
      catch
        :exit, _ -> :ok
      end
    end

    new_state = %State{state | heartbeat_timer: nil}
    {:ok, new_state}
  end

  ### Consumer Management

  # Starts consuming from the member's assigned partitions.
  defp start_consumer(
         %State{
           consumer_module: consumer_module,
           gen_consumer_module: gen_consumer_module,
           consumer_opts: consumer_opts,
           group_name: group_name,
           member_id: member_id,
           generation_id: generation_id,
           supervisor_pid: pid
         } = state,
         assignments
       ) do
    # add member_id, generation_id, and group_manager_pid (for rejoin signalling) to the consumer opts
    consumer_opts =
      Keyword.merge(consumer_opts,
        generation_id: generation_id,
        member_id: member_id,
        group_manager_pid: self()
      )

    consumer_modules = {gen_consumer_module, consumer_module}
    {:ok, supervisor_pid} = ConsumerGroup.start_consumer(pid, consumer_modules, group_name, assignments, consumer_opts)
    state = %{state | assignments: assignments, consumer_supervisor_pid: supervisor_pid}
    {:ok, state}
  end

  # Stops consuming from the member's assigned partitions and commits offsets.
  defp stop_consumer(%State{supervisor_pid: pid} = state) do
    :ok = ConsumerGroup.stop_consumer(pid)

    {:ok, state}
  end

  ### Partition Assignment

  # Queries the Kafka brokers for a list of partitions for the topics of
  # interest to this consumer group. This function returns a list of
  # topic/partition tuples that can be passed to a GenConsumer's
  # `assign_partitions` method.
  #
  # If any topics are missing from metadata (UNKNOWN_TOPIC_OR_PARTITION),
  # retries with exponential backoff following the Java client pattern.
  defp assignable_partitions(state), do: assignable_partitions(state, 1)

  defp assignable_partitions(%State{client: client, topics: topics, group_name: group_name} = state, attempt) do
    {:ok, cluster_metadata} = KafkaExAPI.metadata(client)
    {found_partitions, missing_topics} = partition_topics_by_availability(cluster_metadata, topics)

    handle_topic_availability(state, found_partitions, missing_topics, group_name, attempt)
  end

  defp partition_topics_by_availability(cluster_metadata, topics) do
    Enum.reduce(topics, {[], []}, fn topic, {found_acc, missing_acc} ->
      case get_partitions_for_topic(cluster_metadata, topic) do
        [] -> {found_acc, [topic | missing_acc]}
        partitions -> {found_acc ++ Enum.map(partitions, &{topic, &1}), missing_acc}
      end
    end)
  end

  defp handle_topic_availability(_state, found_partitions, [], _group_name, _attempt), do: found_partitions

  defp handle_topic_availability(_state, found_partitions, missing_topics, group_name, attempt)
       when attempt >= @max_topic_retries do
    topics = Enum.join(missing_topics, ", ")

    Logger.warning(
      "Consumer group #{group_name} could not find topics #{topics} " <>
        "after #{@max_topic_retries} attempts (UNKNOWN_TOPIC_OR_PARTITION)"
    )

    found_partitions
  end

  defp handle_topic_availability(state, _found_partitions, missing_topics, group_name, attempt) do
    sleep_time = calculate_topic_backoff(attempt)
    topics = Enum.join(missing_topics, ", ")

    Logger.info(
      "Consumer group #{group_name}: topics #{topics} not found. " <>
        "Retrying in #{sleep_time}ms (attempt #{attempt}/#{@max_topic_retries})"
    )

    :timer.sleep(sleep_time)
    assignable_partitions(state, attempt + 1)
  end

  @topic_retry_base_delay_ms 100
  @topic_retry_max_delay_ms 5000
  defp calculate_topic_backoff(attempt) do
    delay = @topic_retry_base_delay_ms * round(:math.pow(2, attempt - 1))
    min(delay, @topic_retry_max_delay_ms)
  end

  # Extract partition IDs for a topic from cluster metadata
  defp get_partitions_for_topic(cluster_metadata, topic_name) do
    case Map.get(cluster_metadata.topics, topic_name) do
      nil -> []
      topic -> Enum.map(topic.partitions, & &1.partition_id)
    end
  end

  # This function is used by the group leader to determine partition
  # assignments during the join/sync phase. `members` is provided to the leader
  # by the coordinating broker in `JoinGroupResponse`. `partitions` is a list
  # of topic/partition tuples, obtained from `assignable_partitions/1`. The
  # return value is a complete list of member assignments in the format needed
  # by `SyncGroupResponse`.
  defp assign_partitions(%State{partition_assignment_callback: partition_assignment_callback}, members, partitions) do
    # Delegate partition assignment to GenConsumer module.
    assignments = Map.new(partition_assignment_callback.(members, partitions))
    Enum.map(members, &{&1, pack_assignments(Map.get(assignments, &1, []))})
  end

  defp pack_assignments(assignments) do
    assignments
    |> Enum.reduce(%{}, fn {topic, partition}, assignments ->
      Map.update(assignments, topic, [partition], &(&1 ++ [partition]))
    end)
    |> Map.to_list()
  end

  ### Error Classification

  # Determines if an error is recoverable via retry/rejoin.
  # Following Java client pattern (KAFKA-6829): UNKNOWN_TOPIC_OR_PARTITION is
  # treated like COORDINATOR_LOAD_IN_PROGRESS - both trigger retry behavior.
  defp recoverable_error?(:coordinator_not_available), do: true
  defp recoverable_error?(:not_coordinator), do: true
  defp recoverable_error?(:coordinator_load_in_progress), do: true
  defp recoverable_error?(:unknown_topic_or_partition), do: true
  defp recoverable_error?(:no_broker), do: true
  defp recoverable_error?(:timeout), do: true
  defp recoverable_error?(:closed), do: true
  defp recoverable_error?(:not_connected), do: true
  defp recoverable_error?(:unknown), do: true
  defp recoverable_error?(_), do: false

  defp maybe_warn_static_membership_unsupported(_client, nil), do: :ok

  defp maybe_warn_static_membership_unsupported(client, _group_instance_id) do
    case KafkaExAPI.api_versions(client) do
      {:ok, %ApiVersions{} = versions} ->
        case ApiVersions.max_version_for_api(versions, 11) do
          {:ok, max_version} when max_version >= 5 ->
            :ok

          _ ->
            Logger.warning(
              "group_instance_id is set but the broker does not support JoinGroup v5+ " <>
                "(requires Kafka 2.3+); static membership (KIP-345) will not take effect and " <>
                "the consumer will join as a dynamic member"
            )
        end

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end
end
