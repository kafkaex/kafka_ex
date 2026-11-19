defmodule KafkaEx.TestSupport.FrontTrace do
  @moduledoc false

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.Front.Core
  alias KafkaEx.TestSupport.FrontModel

  @transport [:timeout, :closed, :econnreset]
  @gated [:produce, :acks0]
  @gate_keys [{"t", 0}, {"t", 1}]

  @budget %{
    produce: 3,
    fetch: 3,
    heartbeat: 3,
    commit: 3,
    offset_fetch: 3,
    kip848_member: 3,
    kip848_join: 3,
    find_any: 3,
    join: 1,
    sync: 1,
    acks0: 1,
    send_node: 1,
    send_any: 1
  }

  @no_resend %{
    send_node: :any,
    send_any: :broker,
    acks0: :any,
    produce: [:timeout],
    join: [:timeout],
    sync: [:timeout],
    heartbeat: [:timeout],
    commit: [:timeout],
    kip848_join: [:timeout],
    offset_fetch: [:timeout],
    kip848_member: [:timeout, :closed, :econnreset, :transport_error, :parse_error]
  }

  # The oracle reads a failure's shape itself rather than through Decision, so a misreading there shows up here.
  def failure(%Error{error: error}), do: {:broker, error}
  def failure({group, error}) when is_binary(group) and is_atom(error), do: {:broker, error}
  def failure(error) when error in @transport, do: {:transport, error}
  def failure(error) when is_atom(error), do: {:broker, error}
  def failure(_reason), do: {:transport, :transport_error}

  def effects(trace), do: for(%{kind: :effect} = r <- trace, do: r)
  def events(trace), do: for(%{kind: :event} = r <- trace, do: r)

  def dispatches(trace, id) do
    trace
    |> Enum.reduce({nil, []}, fn
      %{kind: :event, v: {:routed, ^id, route}} = r, {_walk, acc} ->
        {walk_of(route, r.i), acc}

      %{kind: :effect, v: {:dispatch, %{id: ^id} = d}} = r, {walk, acc} ->
        {walk, [Map.merge(d, %{t: r.t, i: r.i, cause: r.cause, walk: walk}) | acc]}

      _r, state ->
        state
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  defp walk_of({:candidates, conns, _epoch}, i), do: %{routed: i, conns: conns}
  defp walk_of(_route, _i), do: nil

  def ends(trace, id), do: for(%{v: {:end, ^id, outcome}} = r <- effects(trace), do: {r.t, outcome})

  def outcome_before(trace, prev, i1) do
    case resolution(trace, prev, i1) do
      nil -> nil
      r -> outcome(r)
    end
  end

  defp resolution(trace, %{ref: ref, conn: conn, i: i0}, i1) do
    trace
    |> events()
    |> Enum.filter(&(&1.i > i0 and &1.i < i1))
    |> Enum.find(fn
      %{v: {:done, ^ref, _result}} -> true
      %{v: {:encode_failed, ^ref}} -> true
      %{v: {:conn_down, ^conn, _written?}} -> true
      _ -> false
    end)
  end

  defp outcome(%{v: {:done, _ref, {:error, :not_connected}}}), do: :not_connected
  defp outcome(%{v: {:done, _ref, {:error, reason}}}), do: failure(reason)
  defp outcome(%{v: {:done, _ref, {:ok, _}}}), do: :ok
  defp outcome(%{v: {:encode_failed, _ref}}), do: :encode_failed
  defp outcome(%{v: {:conn_down, _conn, true}}), do: {:transport, :timeout}
  defp outcome(%{v: {:conn_down, _conn, false}}), do: :not_connected

  def termination(model) do
    trace = model.trace

    Enum.flat_map(model.info, fn {id, info} ->
      ends = ends(trace, id)
      late = for d <- dispatches(trace, id), d.t >= info.deadline, do: {:dispatched_at_or_after_deadline, id, d.t}

      late =
        late ++
          for %{t: t, v: {:resolve, ^id, _}} <- effects(trace),
              t >= info.deadline do
            {:resolved_after_deadline, id, t}
          end

      cond do
        length(ends) != 1 -> [{:ends, id, ends}]
        elem(hd(ends), 0) > info.deadline + model.lag -> [{:ended_late, id, hd(ends), info.deadline}]
        match?([{_, {:reply, {:error, :not_connected}}}], ends) -> [{:not_connected_leaked, id}]
        true -> []
      end ++ late ++ effects_after_end(trace, id)
    end) ++ if(Core.idle?(model.core), do: [], else: [{:not_idle, model.core}])
  end

  defp effects_after_end(trace, id) do
    case Enum.find(trace, &match?(%{kind: :effect, v: {:end, ^id, _}}, &1)) do
      nil ->
        []

      %{i: i_end} ->
        for %{kind: :effect, i: i, v: v} <- trace, i > i_end, mentions?(v, id), do: {:effect_after_end, id, v}
    end
  end

  defp mentions?({:dispatch, %{id: id}}, id), do: true
  defp mentions?({:resolve, id, _}, id), do: true
  defp mentions?({:timer, {:backoff, id, _}, _}, id), do: true
  defp mentions?({:classified, id, _, _}, id), do: true
  defp mentions?({:end, id, _}, id), do: true
  defp mentions?(_v, _id), do: false

  def attempts(model) do
    Enum.flat_map(model.info, fn {id, info} ->
      max = Map.fetch!(@budget, info.class)

      model.trace
      |> dispatches(id)
      |> Enum.reduce({0, nil, []}, fn d, {spent, prev, bad} ->
        spent = spent + spent_by(model.trace, prev, d)
        bad = if d.attempt != spent + 1 or d.attempt > max, do: [{:attempt, id, d.attempt, spent, max} | bad], else: bad
        {spent, d, bad}
      end)
      |> elem(2)
    end)
  end

  defp spent_by(_trace, nil, _d), do: 0

  defp spent_by(trace, prev, d) do
    case outcome_before(trace, prev, d.i) do
      {:transport, _error} -> if prev.walk == nil, do: 1, else: 0
      {:broker, _error} -> 1
      _other -> 0
    end
  end

  def no_resend(model) do
    Enum.flat_map(model.info, fn {id, info} ->
      case Map.fetch(@no_resend, info.class) do
        :error -> []
        {:ok, set} -> resent(model.trace, id, set)
      end
    end)
  end

  defp resent(trace, id, set) do
    trace
    |> dispatches(id)
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.flat_map(fn [prev, next] ->
      case outcome_before(trace, prev, next.i) do
        {kind, error} -> if in_set?(set, kind, error), do: [{:resent, id, error, next}], else: []
        _other -> []
      end
    end)
  end

  defp in_set?(:any, _kind, _error), do: true
  defp in_set?(:broker, kind, _error), do: kind == :broker
  defp in_set?(set, _kind, error), do: error in set

  def backoff(model) do
    Enum.flat_map(model.info, fn {id, _info} ->
      model.trace
      |> dispatches(id)
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.flat_map(fn [prev, next] ->
        floor = backoff_floor(next.attempt - 1)

        if next.t - failed_at(model.trace, prev, next) >= floor or exempt?(model.trace, prev, next) do
          []
        else
          [{:no_backoff, id, prev, next, floor}]
        end
      end)
    end)
  end

  # The wait starts when the attempt failed, not when it was sent.
  defp failed_at(trace, prev, next) do
    case resolution(trace, prev, next.i) do
      nil -> prev.t
      r -> r.t
    end
  end

  # An oracle independent of Backoff: 100 ms doubling per spent attempt, capped at 1 s, minus 20 %.
  defp backoff_floor(spent), do: trunc(min(100 * Integer.pow(2, max(spent - 1, 0)), 1_000) * 0.8)

  defp exempt?(trace, prev, next) do
    next.epoch > prev.epoch or woken_where_parked?(prev, next) or walk_step?(prev, next) or
      replica_refused?(trace, prev, next)
  end

  defp woken_where_parked?(prev, %{cause: {:connected, conn}}) do
    conn == prev.conn or (prev.walk != nil and conn in prev.walk.conns)
  end

  defp woken_where_parked?(_prev, _next), do: false

  defp walk_step?(%{walk: %{routed: routed}} = prev, %{walk: %{routed: routed}} = next), do: prev.conn != next.conn
  defp walk_step?(_prev, _next), do: false

  defp replica_refused?(trace, prev, next) do
    prev.replica? and outcome_before(trace, prev, next.i) == :not_connected
  end

  def dead_callers(model) do
    for %{t: t, v: {:caller_down, id}} <- events(model.trace),
        d <- dispatches(model.trace, id),
        d.t > t or (d.t == t and dispatched_after_down?(model.trace, id)) do
      {:dispatched_after_down, id, d}
    end
  end

  defp dispatched_after_down?(trace, id) do
    trace
    |> Enum.drop_while(&(&1.v != {:caller_down, id}))
    |> Enum.any?(&match?(%{kind: :effect, v: {:dispatch, %{id: ^id}}}, &1))
  end

  def first_available(model) do
    Enum.flat_map(model.info, fn {id, info} ->
      if info.class in [:send_any, :find_any], do: walk_violations(model.trace, id), else: []
    end)
  end

  defp walk_violations(trace, id) do
    walks =
      trace
      |> Enum.filter(
        &(match?(%{kind: :effect, v: {:resolve, ^id, _}}, &1) or
            match?(%{kind: :effect, v: {:dispatch, %{id: ^id}}}, &1))
      )
      |> Enum.chunk_while([], &walk_chunk/2, &{:cont, Enum.reverse(&1), []})
      |> Enum.reject(&(&1 == []))

    dup =
      for w <- walks,
          conns = Enum.map(w, & &1.conn),
          length(conns) != length(Enum.uniq(conns)) do
        {:candidate_twice, id, conns}
      end

    bad_end =
      case ends(trace, id) do
        [{_, {:reply, {:error, reason}}}] when reason in [:not_connected, :no_connected_broker] ->
          [{:walk_end, id, reason}]

        _ ->
          []
      end

    dup ++ bad_end
  end

  defp walk_chunk(%{v: {:resolve, _, _}}, acc), do: {:cont, Enum.reverse(acc), []}
  defp walk_chunk(%{v: {:dispatch, d}}, acc), do: {:cont, [d | acc]}

  # Not only at the deadline: an exhausted walk replies :broker_failed after a write, and otherwise waits.
  def walk_end(model) do
    for {id, info} <- model.info,
        info.class in [:send_any, :find_any],
        {end_rec, reason} <- error_ends(model.trace, id),
        marks = marks(model.trace, id, info.deadline, end_rec.by),
        bad = walk_end_violation(model.trace, marks, end_rec, reason, info.deadline),
        bad != nil do
      {bad, id, reason}
    end
  end

  defp walk_end_violation(trace, marks, end_rec, reason, deadline) do
    exhausted = exhausted_walk(marks)

    cond do
      exhausted == :written and reason != :broker_failed ->
        :exhausted_walk_not_broker_failed

      exhausted != :written and reason == :broker_failed ->
        :broker_failed_without_exhausted_walk

      exhausted == :unwritten and end_rec.t < deadline and not parked_conn_down?(trace, marks, end_rec) ->
        :unwritten_walk_ended

      true ->
        nil
    end
  end

  # A parked entry whose connection goes down backs off, and replies at once if no backoff fits.
  defp parked_conn_down?(trace, marks, end_rec) do
    {%{walk: %{conns: conns}}, _mark} = List.last(marks)

    case Enum.at(trace, end_rec.by) do
      %{v: {:conn_down, conn, _written?}} -> conn in conns
      _other -> false
    end
  end

  def encode_failure(model) do
    trace = model.trace

    for %{kind: :event, i: i, v: {:encode_failed, {id, _seq} = ref}} <- trace,
        not Enum.any?(trace, &match?(%{kind: :effect, i: j, v: {:end, ^id, _}} when j < i, &1)),
        bad = encode_violations(trace, id, i),
        bad != [] do
      {:encode_failed, ref, bad}
    end
  end

  defp encode_violations(trace, id, i) do
    caused = for %{kind: :effect, by: ^i, v: v} <- trace, do: v
    later = for %{kind: :effect, i: j, by: by, v: v} <- trace, j > i, by != i, mentions?(v, id), do: v
    stores = for {:store, _action} = v <- caused, do: v
    reply = if {:end, id, {:reply, {:error, :timeout}}} in caused, do: [], else: [{:no_timeout_reply, caused}]
    reply ++ stores ++ later
  end

  def gate_check(model) do
    on_wire =
      model.out
      |> Map.values()
      |> Enum.flat_map(&List.wrap(gate_of(model, &1.d.id)))
      |> Enum.frequencies()

    over = for {key, n} <- on_wire, n > model.gate_limit, do: {:gate_over, key, n}
    gates = for key <- @gate_keys, do: {key, Core.gate(model.core, key)}

    fifo =
      for {key, %{holders: holders, waiters: waiters}} <- gates,
          live = for(h <- holders, not match?({:wire, _}, h), do: h),
          waiters != Enum.sort(waiters) or (live != [] and waiters != [] and Enum.max(live) > Enum.min(waiters)) do
        {:gate_not_fifo, key, live, waiters}
      end

    wires = for {key, gate} <- gates, {:wire, ref} <- gate.holders, do: {key, ref, Map.get(model.out, ref)}
    stale = for {key, ref, nil} <- wires, do: {:slot_not_released, key, ref}

    held_too_long =
      for {key, ref, %{at: at}} <- wires, model.now - at > FrontModel.liveness(), do: {:slot_held_too_long, key, ref}

    case over ++ fifo ++ stale ++ held_too_long do
      [] -> model
      bad -> raise ExUnit.AssertionError, message: "gate violation: #{inspect(bad)}"
    end
  end

  def gate_order(model) do
    starts = starts(model.trace)

    for key <- @gate_keys,
        ids = Enum.filter(starts, &(gate_of(model, &1) == key)),
        ids != Enum.sort(ids) do
      {:gate_not_fifo, key, ids}
    end
  end

  defp gate_of(model, id) do
    %{class: class, p: p} = Map.fetch!(model.info, id)
    if class in @gated, do: {"t", p}
  end

  def serial_exclusive(model) do
    model.trace
    |> Enum.reduce({nil, MapSet.new(), []}, fn
      %{kind: :effect, v: {:end, id, _}}, {id, started, bad} ->
        {nil, started, bad}

      %{kind: :effect, v: {:resolve, id, _}}, {active, started, bad} ->
        cond do
          MapSet.member?(started, id) -> {active, started, bad}
          active == nil -> {id, MapSet.put(started, id), bad}
          true -> {id, MapSet.put(started, id), [{:serial_admitted, active, id} | bad]}
        end

      _r, state ->
        state
    end)
    |> elem(2)
  end

  def serial_order(model) do
    starts = starts(model.trace)
    if starts == Enum.sort(starts), do: [], else: [{:serial_order, starts}]
  end

  defp starts(trace) do
    for %{v: {:resolve, id, _}} <- effects(trace), reduce: [] do
      acc -> if id in acc, do: acc, else: acc ++ [id]
    end
  end

  # Restated from the trace alone, so this oracle for the deadline reply shares no code with Entry.
  def deadline_reply(model) do
    for {id, info} <- model.info,
        {end_rec, reason} <- error_ends(model.trace, id),
        end_rec.t >= info.deadline,
        expected = expected_at_deadline(model.trace, id, info.deadline, end_rec),
        reason != expected do
      {:deadline_reply, id, reason, expected}
    end
  end

  def replica_reply(model) do
    for {id, info} <- model.info,
        {_end_rec, reason} <- error_ends(model.trace, id),
        marks = marks(model.trace, id, info.deadline, length(model.trace)),
        reason in written(marks, true),
        reason not in [:timeout, :no_broker | written(marks, false)] do
      {:replica_error_replied, id, reason}
    end
  end

  def classification(model) do
    for %{kind: :event, i: i, v: {:done, {id, _seq}, {:error, reason}}} <- model.trace,
        reason != :not_connected,
        {_kind, expected} = failure(reason),
        %{by: ^i, v: {:classified, ^id, error, _branch}} <- effects(model.trace),
        error != expected do
      {:misclassified, id, reason, error, expected}
    end
  end

  defp error_ends(trace, id) do
    for(%{v: {:end, ^id, {:reply, {:error, reason}}}} = r <- effects(trace), do: {r, reason})
  end

  defp expected_at_deadline(trace, id, deadline, end_rec) do
    marks = marks(trace, id, deadline, end_rec.by)
    last = List.last(marks)

    cond do
      match?(%{v: {:encode_failed, {^id, _seq}}}, Enum.at(trace, end_rec.by)) -> :timeout
      match?({_d, :outstanding}, last) -> :timeout
      exhausted_walk(marks) == :written -> :broker_failed
      written(marks, false) != [] -> List.last(written(marks, false))
      parked?(trace, id, end_rec, last) -> :no_broker
      true -> :timeout
    end
  end

  defp marks(trace, id, deadline, upto) do
    for d <- dispatches(trace, id), d.i < upto, do: {d, mark(trace, d, deadline, upto)}
  end

  # A :conn_down at or after the deadline meets the deadline guard before it reaches the attempt.
  defp mark(trace, d, deadline, upto) do
    trace
    |> Enum.filter(&(&1.kind == :event and &1.i > d.i and &1.i <= upto))
    |> Enum.find_value(:outstanding, fn
      %{v: {:done, ref, {:ok, _}}} when ref == d.ref -> :ok
      %{v: {:done, ref, {:error, :not_connected}}} when ref == d.ref -> :refused
      %{v: {:done, ref, {:error, reason}}} when ref == d.ref -> {:written, reason}
      %{v: {:encode_failed, ref}} when ref == d.ref -> :encode_failed
      %{t: t, v: {:conn_down, conn, true}} when conn == d.conn and t < deadline -> {:written, :timeout}
      %{t: t, v: {:conn_down, conn, false}} when conn == d.conn and t < deadline -> :unwritten
      _ -> nil
    end)
  end

  defp written(marks, replica?), do: for({%{replica?: ^replica?}, {:written, reason}} <- marks, do: reason)

  defp exhausted_walk(marks) do
    with {%{walk: %{routed: routed, conns: conns}}, last} <- List.last(marks),
         true <- last in [:refused, :unwritten] or transport_mark?(last) do
      walk = for {%{walk: %{routed: ^routed}}, m} <- marks, do: m

      cond do
        length(walk) != length(conns) -> nil
        Enum.any?(walk, &transport_mark?/1) -> :written
        true -> :unwritten
      end
    else
      _ -> nil
    end
  end

  defp transport_mark?({:written, reason}), do: match?({:transport, _}, failure(reason))
  defp transport_mark?(_mark), do: false

  defp parked?(trace, id, end_rec, last) do
    parking =
      trace
      |> Enum.filter(&(&1.i < end_rec.i))
      |> effects()
      |> Enum.filter(&(match?(%{v: {:resolve, ^id, _}}, &1) or match?(%{v: {:timer, {:backoff, ^id, _}, _}}, &1)))
      |> List.last()

    cond do
      parking != nil and (last == nil or elem(last, 0).i < parking.i) -> match?(%{v: {:resolve, _, _}}, parking)
      last == nil -> false
      true -> match?({_d, :refused}, last) or match?({%{walk: %{}}, :unwritten}, last)
    end
  end
end
