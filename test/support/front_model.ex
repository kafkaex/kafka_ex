defmodule KafkaEx.TestSupport.FrontModel do
  @moduledoc false

  import StreamData

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.Front.Core
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.TestSupport.FrontPolicies

  import FrontPolicies, only: [conn: 2, policy: 2]

  @liveness 1_500
  @topic "t"
  @group "g"
  @nodes [1, 2, 3]
  @roles [:data, :coordinator, :metadata, {:fetch, "t", 0}, {:fetch, "t", 1}]

  @outcomes [
    :ok,
    :not_connected,
    :timeout,
    :closed,
    :econnreset,
    :parse_error,
    :not_leader_for_partition,
    :not_coordinator,
    :coordinator_not_available,
    :rebalance_in_progress,
    :unknown_topic_id,
    :offset_out_of_range,
    :member_id_required,
    :illegal_generation,
    {:tls_alert, {:bad_record_mac, ~c"bad record mac"}},
    %Error{error: :not_coordinator},
    {"g", :rebalance_in_progress}
  ]

  # Keyword lists: on OTP 26+ a map with atom keys iterates in atom-table order, which the seed does not pin.
  def snapshot do
    fixed_map(
      leaders: fixed_map(%{0 => member_of([nil | @nodes]), 1 => member_of([nil | @nodes])}),
      hints: fixed_map(%{0 => member_of([nil | @nodes]), 1 => member_of([nil | @nodes])}),
      coordinator: member_of([nil | @nodes]),
      brokers: member_of([[], [1], [1, 2], [1, 2, 3], [3, 1]])
    )
  end

  def liveness, do: @liveness

  def command do
    frequency([
      {6, tuple({constant(:admit), member_of(FrontPolicies.classes()), integer(0..1), integer(100..4_000)})},
      {2, tuple({constant(:admit), member_of([:produce, :acks0]), integer(0..1), integer(100..300)})},
      {6, tuple({constant(:advance), integer(1..800)})},
      {3, tuple({constant(:advance), integer(1..40)})},
      {8, tuple({constant(:answer), non_negative_integer(), member_of(@outcomes)})},
      {2, tuple({constant(:dispatch_done), non_negative_integer()})},
      {1, tuple({constant(:stale), non_negative_integer(), member_of(@outcomes)})},
      {2, tuple({constant(:crash), non_negative_integer(), boolean()})},
      {3, tuple({constant(:connected), member_of(@nodes), member_of(@roles)})},
      {3, tuple({constant(:swap), swap()})},
      {1, tuple({constant(:caller_down), non_negative_integer()})},
      {3, tuple({constant(:caller_dies), non_negative_integer(), integer(0..600)})},
      {2, tuple({constant(:encode_failed), non_negative_integer()})},
      {1, constant(:store_restarted)}
    ])
  end

  defp swap do
    one_of([
      tuple({constant(:leader), integer(0..1), member_of([nil | @nodes])}),
      tuple({constant(:hint), integer(0..1), member_of([nil | @nodes])}),
      tuple({constant(:coordinator), member_of([nil | @nodes])}),
      tuple({constant(:brokers), member_of([[], [1], [1, 2], [1, 2, 3]])})
    ])
  end

  def scenario(opts \\ []) do
    fixed_map(
      seed: tuple({positive_integer(), positive_integer(), positive_integer()}),
      snapshot: snapshot(),
      gate_limit: Keyword.get(opts, :gate_limit, integer(1..3)),
      lag: integer(0..50),
      commands: list_of(command(), max_length: Keyword.get(opts, :max_commands, 60))
    )
  end

  @spec run(map(), keyword()) :: map()
  def run(scenario, opts \\ []) do
    model = %{
      core:
        Core.new(
          seed: scenario.seed,
          gate_limit: scenario.gate_limit,
          concurrency: Keyword.get(opts, :concurrency, :multiplexed)
        ),
      now: 0,
      timer_seq: 0,
      timers: [],
      out: %{},
      answered: [],
      live: [],
      dead: MapSet.new(),
      next_id: 1,
      info: %{},
      snap: Map.put(scenario.snapshot, :epochs, %{@topic => 0, {:coordinator, @group} => 0, :brokers => 0}),
      n: 0,
      lag: scenario.lag,
      gate_limit: scenario.gate_limit,
      routes: %{},
      cause: nil,
      by: nil,
      trace: [],
      checks: Keyword.get(opts, :checks, [])
    }

    model = Enum.reduce(scenario.commands, model, &command(&2, &1))
    model = drain(model, 0)
    %{model | trace: Enum.reverse(model.trace)}
  end

  defp drain(%{timers: []} = model, _n), do: model
  defp drain(_model, n) when n > 10_000, do: raise("drain did not converge")

  defp drain(%{timers: [{at, _, _} | _]} = model, n), do: model |> advance(at) |> drain(n + 1)

  defp command(model, {:admit, class, p, budget}) do
    id = model.next_id
    policy = policy(class, p)
    deadline = model.now + budget
    info = %{class: class, p: p, deadline: deadline, policy: policy, admitted: model.now}
    model = %{model | next_id: id + 1, live: model.live ++ [id], info: Map.put(model.info, id, info)}
    feed(%{model | cause: {:admit, id}}, {:admit, id, policy, deadline})
  end

  defp command(model, {:advance, dt}), do: advance(model, model.now + dt)

  defp command(model, {:answer, k, outcome}) do
    case pick(open_refs(model), k) do
      nil -> model
      ref -> answer(%{model | cause: {:answer, ref, outcome}}, ref, outcome)
    end
  end

  defp command(model, {:dispatch_done, k}) do
    case pick(Enum.sort(for({ref, %{answered: :timeout}} <- model.out, do: ref)), k) do
      nil ->
        model

      ref ->
        %{model | out: Map.delete(model.out, ref)}
        |> Map.put(:cause, {:dispatch_done, ref})
        |> feed({:dispatch_done, ref})
    end
  end

  defp command(model, {:stale, k, outcome}) do
    case pick(model.answered, k) do
      nil -> model
      ref -> feed(%{model | cause: {:stale, ref}}, {:done, ref, result(outcome, true)})
    end
  end

  defp command(model, {:crash, k, written?}) do
    conns = model.out |> Map.values() |> Enum.map(& &1.d.conn) |> Enum.uniq() |> Enum.sort()

    case pick(conns, k) do
      nil ->
        model

      conn ->
        out = Map.reject(model.out, fn {_ref, o} -> o.d.conn == conn end)
        feed(%{model | out: out, cause: {:crash, conn, written?}}, {:conn_down, conn, written?})
    end
  end

  defp command(model, {:connected, node, role}) do
    feed(%{model | cause: {:connected, conn(node, role)}}, {:connected, conn(node, role)})
  end

  defp command(model, {:swap, change}) do
    snap = apply_swap(model.snap, change)
    keys = swapped_keys(change)
    bumped = if keys == :all, do: Map.keys(snap.epochs), else: keys
    snap = %{snap | epochs: Enum.reduce(bumped, snap.epochs, fn key, acc -> Map.update!(acc, key, &(&1 + 1)) end)}
    feed(%{model | snap: snap, cause: {:swap, change}}, {:swapped, Map.take(snap.epochs, bumped)})
  end

  defp command(model, {:caller_down, k}) do
    case pick(model.live, k) do
      nil -> model
      id -> feed(%{model | cause: {:caller_down, id}}, {:caller_down, id})
    end
  end

  defp command(model, {:caller_dies, k, delay}) do
    case pick(Enum.reject(model.live, &MapSet.member?(model.dead, &1)), k) do
      nil -> model
      id -> schedule(%{model | dead: MapSet.put(model.dead, id)}, model.now + delay, {:monitor, id})
    end
  end

  defp command(model, {:encode_failed, k}) do
    case pick(open_refs(model), k) do
      nil -> model
      ref -> feed(%{model | out: Map.delete(model.out, ref), cause: {:encode_failed, ref}}, {:encode_failed, ref})
    end
  end

  defp command(model, :store_restarted), do: feed(%{model | cause: :store_restarted}, :store_restarted)

  defp swapped_keys({:coordinator, _node}), do: [{:coordinator, @group}]
  defp swapped_keys({:brokers, _brokers}), do: :all
  defp swapped_keys(_change), do: [@topic]

  defp apply_swap(snap, {:leader, p, node}), do: put_in(snap, [:leaders, p], node)
  defp apply_swap(snap, {:hint, p, node}), do: put_in(snap, [:hints, p], node)
  defp apply_swap(snap, {:coordinator, node}), do: %{snap | coordinator: node}
  defp apply_swap(snap, {:brokers, brokers}), do: %{snap | brokers: brokers}

  defp advance(model, target) do
    case model.timers do
      [{at, _, tag} | rest] when at <= target ->
        %{model | timers: rest, now: max(model.now, at)} |> fire(tag) |> advance(target)

      _ ->
        %{model | now: max(model.now, target)}
    end
  end

  defp fire(model, {:core, tag}), do: feed(%{model | cause: {:timer, tag}}, {:tick, tag})

  defp fire(model, {:monitor, id}) do
    if(id in model.live, do: feed(%{model | cause: {:monitor, id}}, {:caller_down, id}), else: model)
  end

  defp fire(model, {:liveness, ref}) do
    case Map.fetch(model.out, ref) do
      :error ->
        model

      {:ok, %{answered: nil} = o} ->
        model = answer(%{model | cause: {:liveness, ref}}, ref, :timeout)
        if o.d.gated?, do: liveness_close(model, ref), else: model

      {:ok, %{answered: :timeout}} ->
        liveness_close(%{model | cause: {:liveness, ref}}, ref)
    end
  end

  defp liveness_close(model, ref), do: %{model | out: Map.delete(model.out, ref)} |> feed({:dispatch_done, ref})

  defp answer(model, ref, outcome) do
    o = Map.fetch!(model.out, ref)
    outcome = if o.d.reply? or outcome == :not_connected or transport?(outcome), do: outcome, else: :ok

    out =
      if outcome == :timeout and o.d.gated? do
        Map.put(model.out, ref, %{o | answered: :timeout})
      else
        Map.delete(model.out, ref)
      end

    model = %{model | out: out, answered: [ref | model.answered]}
    feed(model, {:done, ref, result(outcome, o.d.reply?)})
  end

  defp transport?(outcome), do: outcome in [:timeout, :closed, :econnreset] or match?({:tls_alert, _}, outcome)

  defp result(:ok, true), do: {:ok, :answer}
  defp result(:ok, false), do: {:ok, :no_response}
  defp result(error, _reply?), do: {:error, error}

  defp open_refs(model), do: for({ref, %{answered: nil}} <- model.out, do: ref) |> Enum.sort()

  defp pick([], _k), do: nil
  defp pick(list, k), do: Enum.at(list, rem(k, length(list)))

  defp feed(model, body) do
    {outer, event} = {model.by, model.n}
    model = %{record(model, :event, body) | by: event}
    {core, effects} = Core.handle(model.core, {model.now, body})
    model = %{model | core: core}
    model = %{Enum.reduce(effects, model, &effect(&2, &1)) | by: outer}
    Enum.reduce(model.checks, model, fn check, acc -> check.(acc) end)
  end

  defp effect(model, {:timer, tag, at} = e), do: model |> record(:effect, e) |> schedule(at + model.lag, {:core, tag})

  defp effect(model, {:resolve, id, info} = e) do
    route = resolve(model.snap, info)
    model = %{record(model, :effect, e) | routes: Map.put(model.routes, id, route)}
    feed(model, {:routed, id, route})
  end

  defp effect(model, {:dispatch, d}) do
    route = Map.get(model.routes, d.id)
    replica? = match?({:node, _, %{replica?: true}}, route)
    d = Map.merge(d, %{replica?: replica?, epoch: route_epoch(route)})
    o = %{d: d, at: model.now, answered: nil, replica?: replica?}
    model = record(model, :effect, {:dispatch, d})

    if d.woken? and MapSet.member?(model.dead, d.id) do
      skip(model, o)
    else
      schedule(%{model | out: Map.put(model.out, d.ref, o)}, model.now + @liveness, {:liveness, d.ref})
    end
  end

  defp effect(model, {:end, id, _outcome} = e), do: %{record(model, :effect, e) | live: List.delete(model.live, id)}
  defp effect(model, e), do: record(model, :effect, e)

  # The front checks the caller before a woken send; a dead one gets no {:run}, and :dispatch_done frees the slot.
  defp skip(model, %{d: d} = o) do
    model = feed(%{model | out: Map.put(model.out, d.ref, %{o | answered: :unsent})}, {:caller_down, d.id})
    feed(%{model | out: Map.delete(model.out, d.ref)}, {:dispatch_done, d.ref})
  end

  defp route_epoch({:node, _conn, %{epoch: epoch}}), do: epoch
  defp route_epoch({:candidates, _conns, epoch}), do: epoch
  defp route_epoch(_route), do: -1

  defp epoch(snap, key), do: Map.fetch!(snap.epochs, key)

  defp schedule(model, at, tag) do
    timers = Enum.sort([{at, model.timer_seq, tag} | model.timers])
    %{model | timers: timers, timer_seq: model.timer_seq + 1}
  end

  defp record(model, kind, value) do
    %{
      model
      | n: model.n + 1,
        trace: [%{i: model.n, t: model.now, cause: model.cause, by: model.by, kind: kind, v: value} | model.trace]
    }
  end

  @spec resolve(map(), map()) :: KafkaEx.Client.Front.Entry.route()
  def resolve(snap, %{selector: %NodeSelector{strategy: strategy, partition: p}, leader_only?: leader_only?})
      when strategy in [:topic_partition, :topic_partition_replica] do
    leader = snap.leaders[p]
    hint = snap.hints[p]
    role = if strategy == :topic_partition_replica, do: {:fetch, @topic, p}, else: :data

    cond do
      leader == nil ->
        {:miss, epoch(snap, @topic)}

      strategy == :topic_partition_replica and not leader_only? and hint not in [nil, leader] and hint in snap.brokers ->
        {:node, conn(hint, role), %{node: hint, replica?: true, epoch: epoch(snap, @topic)}}

      true ->
        {:node, conn(leader, role), %{node: leader, replica?: false, epoch: epoch(snap, @topic)}}
    end
  end

  def resolve(snap, %{selector: %NodeSelector{strategy: :consumer_group}}) do
    case snap.coordinator do
      nil ->
        {:miss, epoch(snap, {:coordinator, @group})}

      node ->
        {:node, conn(node, :coordinator), %{node: node, replica?: false, epoch: epoch(snap, {:coordinator, @group})}}
    end
  end

  def resolve(snap, %{selector: %NodeSelector{strategy: :node_id, node_id: node}}) do
    if node in snap.brokers do
      {:node, conn(node, :data), %{node: node, replica?: false, epoch: epoch(snap, :brokers)}}
    else
      {:error, :no_such_node}
    end
  end

  def resolve(snap, %{selector: %NodeSelector{strategy: :first_available}}) do
    {:candidates, Enum.map(snap.brokers, &conn(&1, :metadata)), epoch(snap, :brokers)}
  end
end
