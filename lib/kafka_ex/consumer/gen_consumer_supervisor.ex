defmodule KafkaEx.Consumer.GenConsumer.Supervisor do
  @moduledoc """
  A supervisor for managing `GenConsumer` processes that are part of a consumer
  group.

  The supervisor will launch individual `GenConsumer` processes for each
  partition given by the `partitions` argument to `start_link/4`. When
  terminated, each of the supervisor's child processes will commit its latest
  offset before terminating.

  This module manages a static list of consumer processes. For dynamically
  distributing consumers in a consumer group across a cluster of nodes, see
  `KafkaEx.ConsumerGroup`.

  ## Per-partition control

  Besides the static list given to `start_link/1` (the classic consumer
  group: start every assigned partition, stop the whole supervisor on a
  rebalance), single partitions can be started and stopped on a running
  supervisor with `start_partition/3`, `stop_partition/3` and
  `stop_partitions/3`, and looked up with `lookup_partition/2` and
  `partition_pids/1`. This is what an incremental (KIP-848) assignment
  needs: only the partitions that changed are touched.

  ### The caller owns the consumers it starts

  A consumer started with `start_partition/3` is a `restart: :temporary`
  child: **the supervisor never restarts it**. Whoever calls
  `start_partition/3` (the owner, e.g. a KIP-848 group manager) is the only
  one that decides what happens to the partition, so there is exactly one
  controller per consumer:

    * The owner holds the partition's membership (`member_id`,
      `member_epoch`) and passes the current one to `start_partition/3`;
      later changes go to the running consumer with
      `KafkaEx.Consumer.GenConsumer.update_membership/3`. Nothing starts a
      consumer behind the owner's back, so an update can never be lost to a
      restart and a consumer never comes back on an old epoch.
    * The owner should `Process.monitor/1` the returned pid right away. On a
      `:DOWN` it decides: start the partition again (with the *current*
      membership), give the partition up, or rejoin the group. A consumer
      that died before the monitor was set up reports `:noproc` as the
      `:DOWN` reason; treat it as an abnormal exit.
    * A KafkaEx `GenConsumer` that can no longer consume as this member
      (`:fenced_member_epoch` / `:unknown_member_id` on a commit or on its
      startup OffsetFetch) casts `{:rejoin_required, reason, member_epoch}`
      to its `:group_manager_pid` and exits with
      `{:shutdown, {:rejoin_required, reason}}`, a clean exit the supervisor
      does not log; see `KafkaEx.Consumer.GenConsumer`.
    * The owner serializes its own operations on a partition (one
      `start_partition/3` / `stop_partition/3` at a time per partition).

  Partitions given to `start_link/1` keep the classic behaviour exactly
  (`restart: :transient`, the `:shutdown` option or 5 s): they are not
  visible to the per-partition functions, and `start_partition/3` refuses
  them (`{:error, :static_partition}`).
  """

  use DynamicSupervisor

  @default_worker_shutdown 5_000
  @config_key {__MODULE__, :config}
  @index_key {__MODULE__, :index}
  @stop_result {__MODULE__, :stop_result}

  @type topic_partition :: {topic :: binary, partition :: non_neg_integer}

  @typedoc "Options of `stop_partition/3` and `stop_partitions/3`."
  @type stop_options :: [{:lost, boolean}]

  # Partitions are int32 on the wire.
  @max_partition 2_147_483_647
  defguardp is_topic_partition(topic, partition)
            when is_binary(topic) and is_integer(partition) and partition >= 0 and partition <= @max_partition

  if Version.match?(System.version(), ">= 1.7.0") do
    @doc since: "0.13.0"
  end

  @doc """
  Starts a `GenConsumer.Supervisor` process linked to the current process.

  `gen_consumer_module` is a module that implements the `GenServer` behaviour
  which consumes events from Kafka.

  `consumer_module` is a module that implements the `GenConsumer` behaviour.

  `group_name` is the name of a consumer group.

  `assignments` is a list of partitions for the `GenConsumer`s to consume.

  `opts` accepts the same options as `KafkaEx.GenConsumer.start_link/5`.

  ### Return Values

  This function has the same return values as `Supervisor.start_link/3`.

  If the supervisor and its consumers are successfully created, this function
  returns `{:ok, pid}`, where `pid` is the PID of the supervisor.
  """
  @spec start_link(%{
          gen_consumer_module: module,
          consumer_module: module,
          group_name: binary,
          assignments: [
            {topic_name :: binary, partition_id :: non_neg_integer}
          ],
          opts: KafkaEx.Consumer.GenConsumer.options()
        }) :: Supervisor.on_start()
  def start_link(%{
        gen_consumer_module: gen_consumer_module,
        consumer_module: consumer_module,
        group_name: group_name,
        assignments: assignments,
        opts: opts
      }) do
    start_link_result =
      DynamicSupervisor.start_link(
        __MODULE__,
        {
          {gen_consumer_module, consumer_module},
          group_name,
          assignments,
          opts
        }
      )

    # The classic children: unchanged except for the id (DynamicSupervisor
    # ignores child ids; each now names its partition instead of all sharing
    # the module name).
    child_spec_builder = fn topic, partition ->
      %{
        id: {topic, partition},
        start: {gen_consumer_module, :start_link, [consumer_module, group_name, topic, partition, opts]},
        shutdown: Keyword.get(opts, :shutdown, @default_worker_shutdown),
        restart: :transient
      }
    end

    case start_link_result do
      {:ok, pid} ->
        :ok = start_workers(pid, child_spec_builder, assignments)
        {:ok, pid}

      error ->
        error
    end
  end

  @doc """
  Returns a list of child pids

  Intended to be used for operational and testing purposes
  """
  @spec child_pids(pid | atom) :: [pid]
  def child_pids(supervisor_pid) do
    supervisor_pid
    |> DynamicSupervisor.which_children()
    |> Enum.map(fn {_, pid, _, _} -> pid end)
  end

  @doc """
  Returns true if any child pids are alive
  """
  @spec active?(Supervisor.supervisor()) :: boolean
  def active?(supervisor_pid) do
    supervisor_pid
    |> child_pids
    |> Enum.any?(&Process.alive?/1)
  end

  @doc """
  Starts a consumer for one `{topic, partition}` under a running supervisor.

  The consumer is started as `gen_consumer_module.start_link(consumer_module,
  group_name, topic, partition, opts)` with the modules and group the
  supervisor was started with, and `opts` merged over the supervisor's own
  options. Pass the partition's current membership here, e.g.
  `member_id: id, member_epoch: epoch, group_manager_pid: self()`. Nothing is
  added to the options, so a custom `gen_consumer_module` receives exactly
  the merged list.

  The child is `restart: :temporary`: it is never restarted by the
  supervisor. See "The caller owns the consumers it starts" in the moduledoc
  for what the caller is expected to do when it exits.

  Its `:shutdown` (how long `stop_partition/3` and a supervisor shutdown wait
  for it before killing it) is `opts[:shutdown]`, else the supervisor's own
  `:shutdown` option, else the fetch `:max_wait_time` (from the merged
  `:fetch_options`, 10 s by default) + 10 s: a consumer stopped inside a fetch
  long-poll only handles the stop once the fetch returns, and then still
  commits in `terminate/2`.

  Returns:

    * `{:ok, pid}`
    * `{:error, {:already_started, pid}}` when the partition already has a
      live consumer started by this function (nothing is started). A consumer
      that is still running its `terminate/2` counts as live; wait for its
      `:DOWN`.
    * `{:error, :static_partition}` when the partition was given to
      `start_link/1` (the classic path owns it)
    * `{:error, :noproc}` when the supervisor is not running (or is on
      another node)
    * `{:error, :invalid_topic_partition}` when the partition is not a
      `{binary, non-negative int32}` tuple, `{:error, :invalid_options}` when
      `opts` is not a keyword list or the merged `:fetch_options` is not one
      (nothing is started)
    * `{:error, reason}` when the consumer fails to start, e.g.
      `{:error, :invalid_membership}` for a malformed `:member_epoch`
  """
  @spec start_partition(Supervisor.supervisor(), topic_partition, KafkaEx.Consumer.GenConsumer.options()) ::
          {:ok, pid}
          | {:error,
             {:already_started, pid} | :static_partition | :noproc | :invalid_topic_partition | :invalid_options | term}
  def start_partition(supervisor, topic_partition, opts \\ [])

  def start_partition(supervisor, {topic, partition}, opts)
      when is_topic_partition(topic, partition) and is_list(opts) do
    case supervisor_config(supervisor) do
      {:ok, {_gen_consumer_module, _consumer_module, _group_name, base_opts, _static} = config} ->
        if valid_partition_opts?(base_opts, opts),
          do: start_valid_partition(supervisor, config, {topic, partition}, opts),
          else: {:error, :invalid_options}

      :error ->
        {:error, :noproc}
    end
  end

  def start_partition(_supervisor, {topic, partition}, _opts) when is_topic_partition(topic, partition),
    do: {:error, :invalid_options}

  def start_partition(_supervisor, _topic_partition, _opts), do: {:error, :invalid_topic_partition}

  defp start_valid_partition(supervisor, config, {topic, partition}, opts) do
    {gen_consumer_module, _consumer_module, _group_name, base_opts, _static} = config
    shutdown = Keyword.get_lazy(opts, :shutdown, fn -> default_partition_shutdown(base_opts, opts) end)

    child_spec = %{
      id: {topic, partition},
      start: {__MODULE__, :start_partition_child, [topic, partition, opts, shutdown]},
      shutdown: shutdown,
      restart: :temporary,
      # The process runs gen_consumer_module, not the start MFA's module
      # (which_children/1, release upgrades).
      modules: [gen_consumer_module]
    }

    do_start_partition(supervisor, child_spec)
  end

  # Checked in the caller before anything else uses the opts: the merge and
  # the default shutdown below would otherwise raise in the caller (the group
  # manager), not return an error.
  defp valid_partition_opts?(base_opts, opts) do
    Keyword.keyword?(opts) and Keyword.keyword?(base_opts) and
      Keyword.keyword?(Keyword.get(Keyword.merge(base_opts, opts), :fetch_options, []))
  end

  defp do_start_partition(supervisor, child_spec) do
    case DynamicSupervisor.start_child(supervisor, child_spec) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      :ignore -> {:error, :ignore}
      {:error, _} = error -> error
    end
  catch
    # The supervisor died between the lookup and the call.
    :exit, _ -> {:error, :noproc}
  end

  @doc """
  Stops the consumer of one `{topic, partition}` started with
  `start_partition/3`.

  The consumer is asked to stop (as a supervisor shutdown does), so its
  `terminate/2` commits the acked offset (single attempt) with its current
  membership before it exits with `:shutdown`. A consumer inside a fetch
  long-poll handles the stop only once the fetch returns (up to
  `:max_wait_time`). If it has not stopped within its `:shutdown` (see
  `start_partition/3`) it is killed (no commit). Either way it is not
  restarted: the child is `:temporary`.

  The wait happens in the caller, not in the supervisor, so the supervisor
  keeps serving other starts and stops meanwhile; use `stop_partitions/3` to
  stop several partitions concurrently.

  Returns `:ok` once the consumer is dead (also when it exited on its own
  during the stop): when it returns, `lookup_partition/2` no longer finds the
  consumer and `start_partition/3` can start the partition again at once.
  Returns `{:error, :not_found}` when the partition has no live consumer
  started by `start_partition/3`, and `{:error, :invalid_topic_partition}`
  for a malformed partition.

  ## Options

    * `:lost` - `true` when the partition is *lost* rather than revoked (the
      member was fenced or hit a fatal error, Java's `onPartitionsLost`): the
      consumer is stopped with the reason `{:shutdown, :partition_lost}`
      instead of `:shutdown`, and a KafkaEx `GenConsumer` then skips its final
      commit (the member's epoch is no longer valid, so the broker would
      reject it). A custom `gen_consumer_module` sees the same reason in its
      `terminate/2`. Default `false`.
  """
  @spec stop_partition(Supervisor.supervisor(), topic_partition, stop_options) ::
          :ok | {:error, :not_found | :invalid_topic_partition}
  def stop_partition(supervisor, topic_partition, opts \\ [])

  def stop_partition(supervisor, {topic, partition} = topic_partition, opts) when is_topic_partition(topic, partition) do
    case indexed_entry(supervisor, topic_partition) do
      {pid, shutdown} -> if Process.alive?(pid), do: stop_child(pid, shutdown, stop_reason(opts)), else: {:error, :not_found}
      nil -> {:error, :not_found}
    end
  end

  def stop_partition(_supervisor, _topic_partition, _opts), do: {:error, :invalid_topic_partition}

  # The reason the consumer's terminate/2 sees. `lost: true` is
  # {:shutdown, :partition_lost}: a KafkaEx GenConsumer skips its final commit
  # for it (see stop_partition/3).
  defp stop_reason(opts) when is_list(opts) do
    if Keyword.get(opts, :lost, false) == true, do: {:shutdown, :partition_lost}, else: :shutdown
  end

  defp stop_reason(_opts), do: :shutdown

  @doc """
  Stops the consumers of several partitions concurrently (see
  `stop_partition/3`), so revoking N partitions whose consumers are idle in a
  fetch long-poll takes about one long-poll, not N.

  Each stop runs in its own process, which is monitored, not linked: a caller
  that traps exits gets no `{:EXIT, ...}` messages from it, and a stop that
  crashes cannot take the caller down (it is reported as
  `{:error, {:stop_failed, reason}}` for that partition). If the caller dies,
  the stops already under way still finish.

  Returns a map of `{topic, partition} => :ok | {:error, reason}` with the
  `stop_partition/3` results (a malformed entry maps to
  `{:error, :invalid_topic_partition}`), or
  `{:error, :invalid_topic_partition}` when `topic_partitions` is not a list.
  `opts` are those of `stop_partition/3` (e.g. `lost: true`).
  """
  @spec stop_partitions(Supervisor.supervisor(), [topic_partition], stop_options) ::
          %{topic_partition => :ok | {:error, :not_found | :invalid_topic_partition | {:stop_failed, term}}}
          | {:error, :invalid_topic_partition}
  def stop_partitions(supervisor, topic_partitions, opts \\ [])

  def stop_partitions(supervisor, topic_partitions, opts) when is_list(topic_partitions) do
    topic_partitions
    |> Enum.uniq()
    |> Enum.map(fn tp ->
      caller = self()
      {_pid, ref} = spawn_monitor(fn -> send(caller, {@stop_result, self(), stop_partition(supervisor, tp, opts)}) end)
      {tp, ref}
    end)
    |> Map.new(fn {tp, ref} -> {tp, await_stop(ref)} end)
  end

  def stop_partitions(_supervisor, _topic_partitions, _opts), do: {:error, :invalid_topic_partition}

  # The result is sent before the stop process exits, so it is in the mailbox
  # by the time its :DOWN arrives (signals between two processes are ordered).
  defp await_stop(ref) do
    receive do
      {:DOWN, ^ref, :process, pid, reason} ->
        receive do
          {@stop_result, ^pid, result} -> result
        after
          0 -> {:error, {:stop_failed, reason}}
        end
    end
  end

  # Same steps as DynamicSupervisor.terminate_child/2 (a shutdown request, then
  # a kill once :shutdown has passed), run in the caller. The consumer traps
  # exits, so an exit signal from the caller would reach it as a plain
  # message; a system terminate is what a GenServer handles as a stop.
  defp stop_child(pid, :brutal_kill, _reason), do: kill_child(pid)

  defp stop_child(pid, shutdown, reason) do
    GenServer.stop(pid, reason, shutdown)
  catch
    # Past :shutdown (still alive: kill it), gone already, exited some other
    # way while stopping, or any other failure of the stop request itself:
    # kill_child/1 makes sure it is dead either way before we return (killing
    # a dead pid is a no-op and its monitor reports :noproc at once). An
    # invalid timeout exits on OTP 25 but raises FunctionClauseError in
    # :gen.call/4 from OTP 26, so errors are caught too.
    :exit, _ -> kill_child(pid)
    :error, _ -> kill_child(pid)
  end

  # A :temporary child is never restarted, so the supervisor only drops it
  # (and logs the :killed exit, as it would for a kill in terminate_child).
  # Returns only once the pid is dead: callers rely on that to start the
  # partition again right away.
  defp kill_child(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _} -> :ok
    end
  end

  @doc """
  Returns `{:ok, pid}` for the live consumer of `{topic, partition}` started
  with `start_partition/3`, or `:error` when there is none (or the supervisor
  is not running, or is on another node).

  Non-blocking: neither the supervisor nor the consumer is called.
  """
  @spec lookup_partition(Supervisor.supervisor(), topic_partition) :: {:ok, pid} | :error
  def lookup_partition(supervisor, {topic, partition} = topic_partition) when is_topic_partition(topic, partition) do
    case indexed_entry(supervisor, topic_partition) do
      {pid, _shutdown} -> if Process.alive?(pid), do: {:ok, pid}, else: :error
      nil -> :error
    end
  end

  # A malformed partition is never indexed.
  def lookup_partition(_supervisor, _topic_partition), do: :error

  @doc """
  Returns a map of `{topic, partition} => pid` for every live consumer
  started with `start_partition/3` (empty when the supervisor is not
  running).

  Non-blocking: neither the supervisor nor the consumers are called.
  """
  @spec partition_pids(Supervisor.supervisor()) :: %{topic_partition => pid}
  def partition_pids(supervisor) do
    for {topic_partition, pid, _shutdown} <- read_index(supervisor, &:ets.tab2list/1),
        Process.alive?(pid),
        into: %{} do
      {topic_partition, pid}
    end
  end

  # The fetch builder's :max_wait_time default (KafkaEx.API @default_max_wait_time).
  @default_max_wait_time 10_000
  # Fetch network buffer (KafkaEx.API @network_timeout_buffer) plus the
  # terminate/2 commit.
  @long_poll_shutdown_margin 10_000

  defp default_partition_shutdown(base_opts, opts) do
    case Keyword.fetch(base_opts, :shutdown) do
      {:ok, shutdown} ->
        shutdown

      :error ->
        max_wait_time =
          case base_opts |> Keyword.merge(opts) |> Keyword.get(:fetch_options, []) |> Keyword.get(:max_wait_time) do
            wait when is_integer(wait) and wait >= 0 -> wait
            _ -> @default_max_wait_time
          end

        max_wait_time + @long_poll_shutdown_margin
    end
  end

  defp indexed_entry(supervisor, topic_partition) do
    case read_index(supervisor, &:ets.lookup(&1, topic_partition)) do
      [{_topic_partition, pid, shutdown}] -> {pid, shutdown}
      [] -> nil
    end
  end

  # The table dies with the supervisor, possibly between finding it and
  # reading it.
  defp read_index(supervisor, read) do
    case List.keyfind(supervisor_dictionary(supervisor), @index_key, 0) do
      {@index_key, table} -> read.(table)
      nil -> []
    end
  rescue
    ArgumentError -> []
  end

  defp supervisor_config(supervisor) do
    case List.keyfind(supervisor_dictionary(supervisor), @config_key, 0) do
      {@config_key, config} -> {:ok, config}
      nil -> :error
    end
  end

  # The supervisor's process dictionary (config + index table), or [] when it
  # is not a live local process. Reading it never blocks on the supervisor.
  defp supervisor_dictionary(supervisor) do
    with pid when is_pid(pid) and node(pid) == node() <- GenServer.whereis(supervisor),
         {:dictionary, dictionary} <- Process.info(pid, :dictionary) do
      dictionary
    else
      _ -> []
    end
  end

  # Runs inside the supervisor (DynamicSupervisor calls a child's start MFA
  # itself), so the duplicate check and the index write are serialized with
  # every other start. :temporary children are never restarted, so a dead pid
  # in the index is simply free: nothing will bring it back.
  @doc false
  def start_partition_child(topic, partition, opts, shutdown) do
    {gen_consumer_module, consumer_module, group_name, base_opts, static} = Process.get(@config_key)
    topic_partition = {topic, partition}

    cond do
      MapSet.member?(static, topic_partition) ->
        {:error, :static_partition}

      pid = live_pid(topic_partition) ->
        {:error, {:already_started, pid}}

      true ->
        merged_opts = Keyword.merge(base_opts, opts)

        case gen_consumer_module.start_link(consumer_module, group_name, topic, partition, merged_opts) do
          {:ok, pid} = ok ->
            index(topic_partition, pid, shutdown)
            ok

          {:ok, pid, _info} = ok ->
            index(topic_partition, pid, shutdown)
            ok

          other ->
            other
        end
    end
  end

  defp live_pid(topic_partition) do
    case :ets.lookup(Process.get(@index_key), topic_partition) do
      [{_topic_partition, pid, _shutdown}] -> if Process.alive?(pid), do: pid
      [] -> nil
    end
  end

  # Drops the entries of dead consumers, so the index stays as large as the
  # set of live partitions.
  defp index(topic_partition, pid, shutdown) do
    table = Process.get(@index_key)
    for {key, old_pid, _} <- :ets.tab2list(table), not Process.alive?(old_pid), do: :ets.delete(table, key)
    :ets.insert(table, {topic_partition, pid, shutdown})
  end

  @impl true
  def init(init_args) do
    case init_args do
      {{gen_consumer_module, consumer_module}, group_name, assignments, opts} ->
        static = MapSet.new(assignments)
        Process.put(@config_key, {gen_consumer_module, consumer_module, group_name, opts, static})
        Process.put(@index_key, :ets.new(__MODULE__, [:set, :protected, read_concurrency: true]))

      _ ->
        :ok
    end

    DynamicSupervisor.init(strategy: :one_for_one)
  end

  defp start_workers(pid, child_spec_builder, assignments) do
    Enum.each(assignments, fn {topic, partition} ->
      child_spec = child_spec_builder.(topic, partition)

      case start_child(pid, child_spec) do
        {:ok, _child} -> nil
        {:ok, _child, _info} -> nil
      end
    end)

    :ok
  end

  defp start_child(pid, child_spec) do
    DynamicSupervisor.start_child(pid, child_spec)
  end
end
