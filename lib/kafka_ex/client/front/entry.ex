defmodule KafkaEx.Client.Front.Entry do
  @moduledoc false

  alias KafkaEx.Client.Front.Backoff
  alias KafkaEx.Client.Front.Decision
  alias KafkaEx.Client.Front.Policy
  alias KafkaEx.Client.NodeSelector

  @type id :: term()
  @type conn :: {binary(), non_neg_integer(), term()}
  @type ref :: {id(), pos_integer()}
  @type phase ::
          :new
          | :resolving
          | :in_flight
          | :awaiting_metadata
          | :awaiting_coordinator
          | :awaiting_connection
          | :backoff
          | :ended
  @type route ::
          {:node, conn(), %{node: integer(), replica?: boolean(), epoch: non_neg_integer()}}
          | {:candidates, [conn()], non_neg_integer()}
          | {:miss, non_neg_integer()}
          | {:error, atom()}
  @type result :: {:ok, term()} | {:error, term()}
  @type body ::
          :start
          | {:routed, route()}
          | {:done, pos_integer(), result()}
          | {:dispatch_done, pos_integer()}
          | {:encode_failed, pos_integer()}
          | {:conn_down, conn(), boolean()}
          | {:connected, conn()}
          | {:swapped, %{Policy.key() => non_neg_integer()}}
          | :store_restarted
          | {:tick, non_neg_integer()}
          | :deadline
          | :caller_down
  @type event :: {integer(), float(), body()}
  @type dispatch :: %{
          ref: ref(),
          id: id(),
          conn: conn(),
          attempt: pos_integer(),
          deadline: integer(),
          gated?: boolean(),
          reply?: boolean(),
          wait?: boolean(),
          woken?: boolean()
        }
  @type outcome :: {:reply, result()} | :caller_down
  @type effect ::
          {:resolve, id(), %{selector: NodeSelector.t(), leader_only?: boolean()}}
          | {:dispatch, dispatch()}
          | {:timer, {:backoff, id(), pos_integer()}, integer()}
          | {:store, KafkaEx.Cluster.ClusterMetadata.action()}
          | {:connect, conn()}
          | {:clear_hint, {binary(), non_neg_integer()}, integer()}
          | {:classified, id(), atom(), Decision.branch()}
          | {:end, id(), outcome()}

  @enforce_keys [:id, :policy, :deadline]
  defstruct [
    :id,
    :policy,
    :deadline,
    phase: :new,
    spent: 0,
    seq: 0,
    current: nil,
    wire: nil,
    parked_on: [],
    seen_epoch: -1,
    trigger: [],
    walk: nil,
    leader_only?: false,
    token: 0,
    woken?: false,
    last_error: nil
  ]

  @type t :: %__MODULE__{
          id: id(),
          policy: Policy.t(),
          deadline: integer(),
          phase: phase(),
          spent: non_neg_integer(),
          seq: non_neg_integer(),
          current: map() | nil,
          wire: {pos_integer(), conn(), boolean()} | nil,
          parked_on: [conn()],
          seen_epoch: integer(),
          trigger: [KafkaEx.Cluster.ClusterMetadata.action()],
          walk: map() | nil,
          leader_only?: boolean(),
          token: non_neg_integer(),
          woken?: boolean(),
          last_error: term()
        }

  @parked [:awaiting_metadata, :awaiting_coordinator, :awaiting_connection]

  @spec new(id(), Policy.t(), integer()) :: t()
  def new(id, %Policy{} = policy, deadline), do: %__MODULE__{id: id, policy: policy, deadline: deadline}

  @spec transition(t(), event()) :: {t(), [effect()]}
  def transition(%__MODULE__{phase: :ended} = entry, _event), do: {entry, []}
  def transition(entry, {_now, _u, :caller_down}), do: finish(entry, :caller_down)
  def transition(entry, {now, u, {:done, seq, result}}), do: done(entry, now, u, seq, result)
  def transition(entry, {_now, _u, {:dispatch_done, seq}}), do: {wire_resolved(entry, seq), []}
  def transition(entry, {_now, _u, {:encode_failed, seq}}), do: encode_failed(entry, seq)

  def transition(%__MODULE__{deadline: deadline} = entry, {now, _u, _body}) when now >= deadline,
    do: expire(entry)

  def transition(entry, {now, u, body}), do: step(entry, now, u, body)

  defp step(%{phase: :new} = entry, _now, _u, :start), do: resolve(entry, false)
  defp step(%{phase: :resolving} = entry, now, _u, {:routed, route}), do: routed(entry, now, route)
  defp step(%{phase: phase} = entry, now, _u, {:routed, route}) when phase in @parked, do: rechecked(entry, now, route)
  defp step(%{phase: :backoff, token: token} = entry, _now, _u, {:tick, token}), do: resolve(entry, false)
  defp step(%{phase: :in_flight} = entry, now, u, {:conn_down, conn, written?}), do: lost(entry, now, u, conn, written?)

  defp step(%{phase: :awaiting_connection} = entry, now, u, {:conn_down, conn, _written?}) do
    if conn in entry.parked_on, do: backoff(%{entry | parked_on: []}, now, u, [], nil), else: {entry, []}
  end

  defp step(%{phase: :awaiting_connection} = entry, _now, _u, {:connected, conn}) do
    if conn in entry.parked_on, do: resolve(%{entry | parked_on: []}, true), else: {entry, []}
  end

  defp step(%{phase: phase} = entry, _now, _u, {:swapped, epochs}) when phase in @parked do
    if Map.get(epochs, Policy.key(entry.policy), -1) > entry.seen_epoch,
      do: resolve(%{entry | parked_on: []}, true),
      else: {entry, []}
  end

  defp step(%{phase: phase, trigger: trigger} = entry, _now, _u, :store_restarted)
       when phase in [:awaiting_metadata, :awaiting_coordinator],
       do: {entry, Enum.map(trigger, &{:store, &1})}

  defp step(entry, _now, _u, :deadline), do: expire(entry)
  defp step(entry, _now, _u, _body), do: {entry, []}

  defp resolve(entry, woken?) do
    {%{entry | phase: :resolving, woken?: woken?}, [resolve_effect(entry)]}
  end

  # A replica is never the last attempt, so a fallback to the leader always fits the budget.
  defp resolve_effect(entry) do
    leader_only? = entry.leader_only? or entry.spent + 1 >= entry.policy.max_attempts
    {:resolve, entry.id, %{selector: entry.policy.selector, leader_only?: leader_only?}}
  end

  defp routed(entry, _now, {:error, reason}), do: finish(entry, {:reply, {:error, reason}})
  defp routed(entry, now, {:node, conn, info}), do: dispatch(%{entry | seen_epoch: info.epoch}, now, conn, info)

  defp routed(entry, _now, {:miss, epoch}),
    do: park(%{entry | seen_epoch: epoch}, miss_phase(entry), Decision.miss_actions(entry.policy))

  defp routed(entry, _now, {:candidates, [], epoch}), do: park(%{entry | seen_epoch: epoch}, :awaiting_metadata, [])

  defp routed(entry, now, {:candidates, [conn | rest] = all, epoch}) do
    entry = %{entry | seen_epoch: epoch, walk: %{all: all, left: rest, written?: false}}
    dispatch(entry, now, conn, %{node: nil, replica?: false})
  end

  # The re-check after parking closes the lost-wakeup gap; only a newer epoch may dispatch.
  defp rechecked(entry, now, route) do
    case route do
      {:node, _conn, %{epoch: epoch}} when epoch > entry.seen_epoch -> routed(entry, now, route)
      {:candidates, [_ | _], epoch} when epoch > entry.seen_epoch -> routed(entry, now, route)
      _other -> {entry, []}
    end
  end

  defp miss_phase(entry), do: if(Policy.coordinator?(entry.policy), do: :awaiting_coordinator, else: :awaiting_metadata)

  defp park(entry, phase, actions) do
    actions = if phase == :awaiting_coordinator, do: with_lookup(entry, actions), else: actions
    entry = %{entry | phase: phase, trigger: actions, walk: nil}
    {entry, Enum.map(actions, &{:store, &1}) ++ [resolve_effect(entry)]}
  end

  # Only a finished lookup wakes a coordinator park, so one is always asked for.
  defp with_lookup(%{policy: %Policy{selector: %NodeSelector{consumer_group_name: group}}}, actions) do
    if Enum.any?(actions, &match?({:rediscover_coordinator, _}, &1)),
      do: actions,
      else: actions ++ [{:rediscover_coordinator, group}]
  end

  defp dispatch(entry, now, _conn, _info) when now >= entry.deadline, do: expire(entry)

  defp dispatch(entry, _now, conn, info) do
    seq = entry.seq + 1
    gated? = entry.policy.gate != nil

    effect =
      {:dispatch,
       %{
         ref: {entry.id, seq},
         id: entry.id,
         conn: conn,
         attempt: entry.spent + 1,
         deadline: entry.deadline,
         gated?: gated?,
         reply?: entry.policy.mode != :acks0,
         wait?: entry.walk == nil,
         woken?: entry.woken?
       }}

    current = %{seq: seq, conn: conn, node: info.node, replica?: info.replica?}
    wire = if gated?, do: {seq, conn, false}, else: nil
    {%{entry | phase: :in_flight, seq: seq, current: current, wire: wire, woken?: false}, [effect]}
  end

  defp done(%{phase: :in_flight, current: %{seq: seq}} = entry, now, u, seq, result) do
    entry = %{entry | wire: answered_wire(entry.wire, result)}
    answered(entry, now, u, result)
  end

  defp done(entry, _now, _u, _seq, _result), do: {entry, []}

  defp answered_wire({seq, conn, _timed_out?}, result), do: if(timeout?(result), do: {seq, conn, true}, else: nil)
  defp answered_wire(nil, _result), do: nil

  defp wire_resolved(%{wire: {seq, _conn, _timed_out?}} = entry, seq), do: %{entry | wire: nil}
  defp wire_resolved(entry, _seq), do: entry

  # Nothing reached the wire, so nothing is retried or acted on, and the caller gets :timeout.
  defp encode_failed(%{phase: :in_flight, current: %{seq: seq}} = entry, seq) do
    {entry, effects} = finish(%{entry | current: nil, wire: nil}, {:reply, {:error, :timeout}})
    {entry, [{:classified, entry.id, :timeout, :not_retryable} | effects]}
  end

  defp encode_failed(entry, seq), do: {wire_resolved(entry, seq), []}

  defp timeout?({:error, reason}), do: Decision.error_atom(reason) == :timeout
  defp timeout?(_result), do: false

  defp answered(entry, _now, _u, {:ok, _} = result), do: finish(%{entry | current: nil}, {:reply, result})

  defp answered(entry, now, u, {:error, reason} = result) do
    case Decision.error_atom(reason) do
      :not_connected -> refused(entry, now)
      error -> failed(remember(entry, reason), now, u, error, result)
    end
  end

  defp lost(%{current: %{conn: conn}} = entry, now, u, conn, true),
    do: failed(remember(%{entry | wire: nil}, :timeout), now, u, :timeout, {:error, :timeout})

  defp lost(%{current: %{conn: conn}, walk: %{}} = entry, now, _u, conn, false),
    do: next_candidate(%{entry | current: nil, wire: nil}, now)

  defp lost(%{current: %{conn: conn}} = entry, now, u, conn, false),
    do: backoff(%{entry | current: nil, wire: nil}, now, u, [], nil)

  defp lost(entry, _now, _u, _conn, _written?), do: {entry, []}

  defp refused(entry, now) when now >= entry.deadline,
    do: finish(entry, {:reply, {:error, refused_at_deadline(entry)}})

  defp refused(entry, now) do
    current = entry.current
    entry = %{entry | current: nil}

    case Decision.refused(entry.policy, current.replica?) do
      {:reply, _} ->
        finish(entry, {:reply, {:error, :no_broker}})

      {:next_candidate, _} ->
        next_candidate(entry, now)

      {:leader, _} ->
        {entry, effects} = resolve(%{entry | leader_only?: true}, false)
        {entry, [clear_hint(entry, current) | effects]}

      {:park_coordinator, actions} ->
        park(entry, :awaiting_coordinator, actions)

      {:park_connection, _} ->
        park_on(entry, [current.conn])
    end
  end

  defp next_candidate(%{walk: %{left: [conn | rest]} = walk} = entry, now),
    do: dispatch(%{entry | walk: %{walk | left: rest}}, now, conn, %{node: nil, replica?: false})

  defp next_candidate(%{walk: %{written?: true}} = entry, _now),
    do: finish(entry, {:reply, {:error, :broker_failed}})

  defp next_candidate(%{walk: %{all: all}} = entry, _now), do: park_on(%{entry | walk: nil}, all)

  # A refusal leaves the Connection without a waiter, so the front states the demand that makes it reconnect.
  defp park_on(entry, conns),
    do: {%{entry | phase: :awaiting_connection, parked_on: conns}, Enum.map(conns, &{:connect, &1})}

  defp failed(%{walk: %{} = walk} = entry, now, u, error, {:error, reason} = result) do
    if Decision.transport_failure?(reason),
      do: next_candidate(%{entry | current: nil, walk: %{walk | written?: true}}, now),
      else: classify(%{entry | walk: nil}, now, u, error, result)
  end

  defp failed(entry, now, u, error, result), do: classify(entry, now, u, error, result)

  defp classify(entry, now, u, error, result) do
    current = entry.current
    entry = %{entry | spent: entry.spent + 1, current: nil}
    {next, branch, actions} = Decision.written(entry.policy, entry.spent, error)

    {entry, next, branch, hint, result} =
      if current.replica?,
        do: from_replica(entry, current, next, branch),
        else: {entry, next, branch, [], result}

    classified = {:classified, entry.id, error, branch}
    {entry, effects} = act(entry, now, u, next, actions, result)
    {entry, [classified | hint] ++ effects}
  end

  # A replica may lag the leader, so its error is never the reply; the leader answers while budget remains.
  defp from_replica(entry, current, next, branch) do
    entry = %{entry | leader_only?: true}
    hint = [clear_hint(entry, current)]

    if next == :park_metadata or entry.spent >= entry.policy.max_attempts,
      do: {entry, next, branch, hint, nil},
      else: {entry, :backoff, :replica, hint, nil}
  end

  defp act(entry, now, _u, _next, actions, result) when now >= entry.deadline,
    do: finish_with(entry, actions, reply(entry, result))

  defp act(entry, _now, _u, :reply, actions, result), do: finish_with(entry, actions, reply(entry, result))
  defp act(entry, _now, _u, :park_metadata, actions, _result), do: park(entry, :awaiting_metadata, actions)
  defp act(entry, _now, _u, :park_coordinator, actions, _result), do: park(entry, :awaiting_coordinator, actions)
  defp act(entry, now, u, :backoff, actions, result), do: backoff(entry, now, u, actions, result)

  defp backoff(entry, now, u, actions, result) do
    case Backoff.wake_at(now, entry.spent, u, entry.deadline, Backoff.default()) do
      {:ok, at} ->
        token = entry.token + 1
        timer = {:timer, {:backoff, entry.id, token}, at}
        {%{entry | phase: :backoff, token: token}, Enum.map(actions, &{:store, &1}) ++ [timer]}

      :no_time ->
        finish_with(entry, actions, reply(entry, result))
    end
  end

  defp clear_hint(entry, current) do
    %NodeSelector{topic: topic, partition: partition} = entry.policy.selector
    {:clear_hint, {topic, partition}, current.node}
  end

  defp reply(entry, nil), do: {:reply, {:error, deadline_error(entry)}}
  defp reply(_entry, result), do: {:reply, result}

  defp expire(entry), do: finish(entry, {:reply, {:error, deadline_error(entry)}})

  defp finish_with(entry, actions, outcome) do
    {entry, effects} = finish(entry, outcome)
    {entry, Enum.map(actions, &{:store, &1}) ++ effects}
  end

  defp finish(entry, outcome), do: {%{entry | phase: :ended}, [{:end, entry.id, outcome}]}

  defp remember(%{current: %{replica?: true}} = entry, _reason), do: entry
  defp remember(entry, reason), do: %{entry | last_error: reason}

  defp refused_at_deadline(%{walk: %{left: [], written?: true}}), do: :broker_failed
  defp refused_at_deadline(entry), do: entry.last_error || :no_broker

  # With no attempt outstanding, the deadline reports the real last written error, as librdkafka does.
  defp deadline_error(%{current: %{}}), do: :timeout
  defp deadline_error(%{last_error: reason}) when reason != nil, do: reason
  defp deadline_error(%{phase: phase}) when phase in @parked, do: :no_broker
  defp deadline_error(_entry), do: :timeout
end
