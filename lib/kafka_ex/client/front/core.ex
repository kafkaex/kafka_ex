defmodule KafkaEx.Client.Front.Core do
  @moduledoc false

  alias KafkaEx.Client.Front.Decision
  alias KafkaEx.Client.Front.Entry
  alias KafkaEx.Client.Front.Policy

  @type concurrency :: :multiplexed | :serial
  @type gate_key :: {binary(), non_neg_integer()}
  @type holder :: Entry.id() | {:wire, Entry.ref()}
  @type body ::
          {:admit, Entry.id(), Policy.t(), integer()}
          | {:routed, Entry.id(), Entry.route()}
          | {:done, Entry.ref(), Entry.result()}
          | {:dispatch_done, Entry.ref()}
          | {:encode_failed, Entry.ref()}
          | {:conn_down, Entry.conn(), boolean()}
          | {:connected, Entry.conn()}
          | {:swapped, %{Policy.key() => non_neg_integer()}}
          | :store_restarted
          | {:tick, {:deadline, Entry.id()} | {:backoff, Entry.id(), pos_integer()}}
          | {:caller_down, Entry.id()}
  @type event :: {integer(), body()}
  @type effect :: Entry.effect() | {:timer, {:deadline, Entry.id()}, integer()}
  @type gate :: %{holders: MapSet.t(holder()), waiters: :queue.queue(Entry.id())}

  defstruct entries: %{},
            arrival: %{},
            next_arrival: 0,
            gates: %{},
            wires: %{},
            active: nil,
            queue: :queue.new(),
            concurrency: :multiplexed,
            gate_limit: 1,
            rand: nil

  @type t :: %__MODULE__{
          entries: %{Entry.id() => Entry.t()},
          arrival: %{Entry.id() => non_neg_integer()},
          next_arrival: non_neg_integer(),
          gates: %{gate_key() => gate()},
          wires: %{Entry.ref() => {gate_key(), Entry.conn(), boolean()}},
          active: Entry.id() | nil,
          queue: :queue.queue(Entry.id()),
          concurrency: concurrency(),
          gate_limit: pos_integer(),
          rand: :rand.state()
        }

  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      concurrency: Keyword.get(opts, :concurrency, :multiplexed),
      gate_limit: Keyword.get(opts, :gate_limit, 1),
      rand: :rand.seed_s(:exsss, Keyword.fetch!(opts, :seed))
    }
  end

  @spec handle(t(), event()) :: {t(), [effect()]}
  def handle(%__MODULE__{} = core, {now, body}) do
    {core, effects} = apply_event(core, now, body)
    {core, more} = grant_all(core, now)
    {core, effects ++ more}
  end

  @spec idle?(t()) :: boolean()
  def idle?(%__MODULE__{} = core) do
    core.entries == %{} and core.wires == %{} and core.active == nil and :queue.is_empty(core.queue) and
      Enum.all?(core.gates, fn {_key, gate} -> MapSet.size(gate.holders) == 0 and :queue.is_empty(gate.waiters) end)
  end

  @spec gate(t(), gate_key()) :: %{holders: [holder()], waiters: [Entry.id()]}
  def gate(%__MODULE__{} = core, key) do
    gate = Map.get(core.gates, key, empty_gate())
    %{holders: MapSet.to_list(gate.holders), waiters: :queue.to_list(gate.waiters)}
  end

  @spec started(t()) :: [Entry.id()]
  def started(%__MODULE__{} = core), do: for({id, %Entry{phase: phase}} <- core.entries, phase != :new, do: id)

  defp apply_event(core, now, {:admit, id, policy, deadline}) do
    core = %{
      core
      | entries: Map.put(core.entries, id, Entry.new(id, policy, deadline)),
        arrival: Map.put(core.arrival, id, core.next_arrival),
        next_arrival: core.next_arrival + 1
    }

    {core, effects} = admit(core, now, id)
    {core, [{:timer, {:deadline, id}, deadline} | effects]}
  end

  defp apply_event(core, now, {:routed, id, route}), do: deliver(core, now, id, {:routed, route})
  defp apply_event(core, now, {:tick, {:backoff, id, token}}), do: deliver(core, now, id, {:tick, token})
  defp apply_event(core, now, {:tick, {:deadline, id}}), do: expire(core, now, id, :deadline)
  defp apply_event(core, now, {:caller_down, id}), do: expire(core, now, id, :caller_down)

  defp apply_event(core, now, {:done, {id, seq} = ref, result}) do
    core = wire_answered(core, ref, result)
    deliver(core, now, id, {:done, seq, result})
  end

  defp apply_event(core, now, {:dispatch_done, {id, seq} = ref}) do
    core |> release_wire(ref) |> deliver(now, id, {:dispatch_done, seq})
  end

  # Nothing was written and no :dispatch_done follows, so a wire slot left by an ended entry is freed now.
  defp apply_event(core, now, {:encode_failed, {id, seq} = ref}) do
    core |> release_wire(ref) |> deliver(now, id, {:encode_failed, seq})
  end

  # An entry this event ends may leave a wire on conn, so wires are freed only after delivery.
  defp apply_event(core, now, {:conn_down, conn, _written?} = body) do
    {core, effects} = broadcast(core, now, body)
    {release_wires_on(core, conn), effects}
  end

  defp apply_event(core, now, body), do: broadcast(core, now, body)

  defp admit(%{concurrency: :serial, active: active} = core, _now, id) when active != nil do
    {%{core | queue: :queue.in(id, core.queue)}, []}
  end

  defp admit(%{concurrency: :serial} = core, now, id), do: gate_or_start(%{core | active: id}, now, id)
  defp admit(core, now, id), do: gate_or_start(core, now, id)

  defp gate_or_start(core, now, id) do
    case Map.fetch!(core.entries, id).policy.gate do
      nil ->
        deliver(core, now, id, :start)

      key ->
        gate = Map.get(core.gates, key, empty_gate())

        core
        |> put_gate(key, %{gate | waiters: :queue.in(id, gate.waiters)})
        |> grant(now, key)
    end
  end

  defp deliver(core, now, id, body) do
    case Map.fetch(core.entries, id) do
      {:ok, %Entry{phase: :new}} when body != :start -> {core, []}
      {:ok, entry} -> run(core, now, entry, body)
      :error -> {core, []}
    end
  end

  defp run(core, now, entry, body) do
    {u, rand} = :rand.uniform_s(core.rand)
    {entry, effects} = Entry.transition(entry, {now, u, body})
    core = %{core | rand: rand, entries: Map.put(core.entries, entry.id, entry)}
    settle(core, now, entry, effects)
  end

  defp settle(core, now, %Entry{phase: :ended, id: id} = entry, effects) do
    core = %{core | entries: Map.delete(core.entries, id), arrival: Map.delete(core.arrival, id)}
    {core, more} = core |> release_gate(entry) |> next_serial(now, id)
    {core, effects ++ more}
  end

  defp settle(core, _now, _entry, effects), do: {core, effects}

  defp expire(core, now, id, how) do
    case Map.fetch(core.entries, id) do
      {:ok, %Entry{phase: :new} = entry} -> drop_waiting(core, now, entry, how)
      {:ok, entry} -> run(core, now, entry, how)
      :error -> {core, []}
    end
  end

  defp drop_waiting(core, now, entry, how) do
    core = %{core | queue: :queue.delete(entry.id, core.queue)}

    core =
      case entry.policy.gate do
        nil -> core
        key -> update_gate(core, key, &%{&1 | waiters: :queue.delete(entry.id, &1.waiters)})
      end

    outcome = if how == :caller_down, do: :caller_down, else: {:reply, {:error, :timeout}}
    settle(core, now, %{entry | phase: :ended}, [{:end, entry.id, outcome}])
  end

  defp release_gate(core, %Entry{policy: %Policy{gate: nil}}), do: core

  defp release_gate(core, %Entry{policy: %Policy{gate: key}, id: id, wire: wire}) do
    core = update_gate(core, key, &%{&1 | holders: MapSet.delete(&1.holders, id)})

    case wire do
      nil ->
        core

      {seq, conn, timed_out?} ->
        ref = {id, seq}
        core = update_gate(core, key, &%{&1 | holders: MapSet.put(&1.holders, {:wire, ref})})
        %{core | wires: Map.put(core.wires, ref, {key, conn, timed_out?})}
    end
  end

  defp release_wire(core, ref) do
    case Map.pop(core.wires, ref) do
      {nil, _wires} ->
        core

      {{key, _conn, _timed_out?}, wires} ->
        update_gate(%{core | wires: wires}, key, &%{&1 | holders: MapSet.delete(&1.holders, {:wire, ref})})
    end
  end

  # After a :timeout the bytes may still be on the wire, so only :dispatch_done or the socket closing frees the slot.
  defp wire_answered(core, ref, result) do
    case Map.fetch(core.wires, ref) do
      {:ok, {key, conn, false}} ->
        if timeout?(result) do
          %{core | wires: Map.put(core.wires, ref, {key, conn, true})}
        else
          release_wire(core, ref)
        end

      _other ->
        core
    end
  end

  defp release_wires_on(core, conn) do
    core.wires
    |> Enum.filter(fn {_ref, {_key, wire_conn, _timed_out?}} -> wire_conn == conn end)
    |> Enum.reduce(core, fn {ref, _}, acc -> release_wire(acc, ref) end)
  end

  defp next_serial(%{concurrency: :serial, active: id} = core, now, id) do
    case :queue.out(core.queue) do
      {{:value, next}, queue} -> gate_or_start(%{core | queue: queue, active: next}, now, next)
      {:empty, _queue} -> {%{core | active: nil}, []}
    end
  end

  defp next_serial(core, _now, _id), do: {core, []}

  defp grant_all(core, now) do
    Enum.reduce(Map.keys(core.gates), {core, []}, fn key, {acc, effects} ->
      {acc, more} = grant(acc, now, key)
      {acc, effects ++ more}
    end)
  end

  defp grant(core, now, key) do
    gate = Map.fetch!(core.gates, key)

    with true <- MapSet.size(gate.holders) < core.gate_limit,
         {{:value, id}, waiters} <- :queue.out(gate.waiters) do
      core = put_gate(core, key, %{gate | waiters: waiters, holders: MapSet.put(gate.holders, id)})
      {core, effects} = deliver(core, now, id, :start)
      {core, more} = grant(core, now, key)
      {core, effects ++ more}
    else
      _ -> {core, []}
    end
  end

  defp broadcast(core, now, body) do
    core.entries
    |> Map.keys()
    |> Enum.sort_by(&Map.fetch!(core.arrival, &1))
    |> Enum.reduce({core, []}, fn id, {acc, effects} ->
      {acc, more} = deliver(acc, now, id, body)
      {acc, effects ++ more}
    end)
  end

  defp empty_gate, do: %{holders: MapSet.new(), waiters: :queue.new()}
  defp put_gate(core, key, gate), do: %{core | gates: Map.put(core.gates, key, gate)}

  defp update_gate(core, key, fun) do
    %{core | gates: Map.put(core.gates, key, fun.(Map.get(core.gates, key, empty_gate())))}
  end

  defp timeout?({:error, reason}), do: Decision.error_atom(reason) == :timeout
  defp timeout?(_result), do: false
end
