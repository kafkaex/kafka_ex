defmodule KafkaEx.Client.ConnectionPropertiesTest do
  use KafkaEx.TestSupport.ConnectionCase, async: false
  use ExUnitProperties

  @moduletag capture_log: true

  @window 100
  @reply_timeout 40
  @max_backoff 500
  @slack 400
  @grace 50
  @errors [:not_connected, :timeout, :closed, :econnreset, :epipe]
  @drop_reasons %{peer_close: [:remote_closed, :recv_error], tcp_error: [:recv_error]}

  property "any mix of calls, replies, drops and timers answers every caller once and keeps the state invariants" do
    check all(open? <- boolean(), commands <- list_of(command(), max_length: 20), max_runs: 100) do
      commands |> Enum.reduce(start(open?), &step/2) |> finish()
    end
  end

  defp command do
    frequency([
      {6, tuple({constant(:request), member_of([:reply, :late, :never, :acks0]), boolean()})},
      {2, constant(:release)},
      {2, constant(:peer_close)},
      {1, constant(:tcp_error)},
      {3, constant(:stale)},
      {2, constant(:reply_timeout)},
      {2, constant(:window)},
      {2, constant(:allow)},
      {1, constant(:refuse)},
      {2, constant(:connect)}
    ])
  end

  defp start(open?) do
    %{fake: fake, host: host} = start_fake()
    watch_close(host)
    FakeTransport.respond(fake, :produce, &echo/1)
    FakeTransport.respond(fake, :heartbeat, &echo/1)
    FakeTransport.hold(fake, :heartbeat)
    FakeTransport.respond(fake, :offset_commit, :no_reply)
    gate = PinGate.new()
    if not open?, do: PinGate.close(gate)

    opts = [gate: gate, reconnect_window: @window, liveness_slack: @reply_timeout, stable_period: 0]
    %{conn: conn} = start_connection(fake, host, port(), :data, opts)
    :sys.install(conn, {&forward_call/3, self()})

    %{
      conn: conn,
      monitor: Process.monitor(conn),
      fake: fake,
      host: host,
      gate: gate,
      open?: open?,
      down_closed?: false,
      state: :disconnected,
      socket: nil,
      old_sockets: [],
      pending: %{},
      calls: %{}
    }
  end

  defp echo(request), do: {:frame, [<<request.correlation_id::32-signed>>, request.client_id]}

  defp forward_call(test, {:in, {{:call, from}, _request}, _state}, _process) do
    send(test, {:call_in, self(), from})
    test
  end

  defp forward_call(test, {:out, _reply, from}, _process) do
    send(test, {:call_out, self(), from})
    test
  end

  defp forward_call(test, _event, _process), do: test

  defp step({:request, kind, wait?}, model) do
    conn = model.conn
    {request, overrides} = request_for(kind)
    overrides = [wait?: wait?, timeout: @reply_timeout] ++ overrides
    model |> ask(kind, &dispatch(conn, %{request | client_id: &1}, overrides)) |> checkpoint(:request)
  end

  defp step(:connect, model) do
    conn = model.conn
    model |> ask(:connect, fn _tag -> ConnectionStateMachine.connect(conn, 5_000) end) |> checkpoint(:connect)
  end

  defp step(:release, model) do
    FakeTransport.release(model.fake, :heartbeat)
    FakeTransport.hold(model.fake, :heartbeat)
    checkpoint(model, :release)
  end

  defp step(:peer_close, model) do
    up? = connection_state(model.conn) == :connected
    FakeTransport.close(model.fake, {model.host, port()})
    if up?, do: await_down(model.conn)
    checkpoint(model, :peer_close)
  end

  defp step(:tcp_error, model) do
    case :sys.get_state(model.conn) do
      {:connected, %{socket: %{socket: raw}}} ->
        send(model.conn, {:tcp_error, raw, :econnreset})
        model = checkpoint(model, :tcp_error)
        assert model.state != :connected, "a tcp_error left the connection up"
        model

      _down ->
        checkpoint(model, :tcp_error)
    end
  end

  defp step(:stale, model) do
    {_state, data} = :sys.get_state(model.conn)
    ids = for {id, _entry} <- Map.to_list(data.in_flight), do: id
    stale = for socket <- [model.socket | model.old_sockets], socket && socket != data.socket, do: socket

    for socket <- stale do
      for id <- [0 | ids], do: FakeTransport.inject(%{socket: socket, owner: model.conn}, {:data, <<id::32-signed>>})
      FakeTransport.inject(%{socket: socket, owner: model.conn}, :closed)
    end

    checkpoint(model, :stale)
  end

  defp step(:reply_timeout, model) do
    {_state, data} = :sys.get_state(model.conn)
    due = for {_id, %{from: {pid, _tag}} = entry} <- Map.to_list(data.in_flight), do: {entry.reply_by, pid}
    model |> await_first(due) |> checkpoint(:reply_timeout)
  end

  defp step(:window, model) do
    {_state, data} = :sys.get_state(model.conn)
    due = for %{from: {pid, _tag}} = waiter <- data.waiting, do: {waiter.enqueued_at + @window, pid}
    model |> await_first(due) |> checkpoint(:window)
  end

  defp step(:allow, model) do
    PinGate.open(model.gate)
    pending = Map.new(model.pending, fn {tag, caller} -> {tag, %{caller | closed_down?: false}} end)
    checkpoint(%{model | open?: true, pending: pending}, :allow)
  end

  defp step(:refuse, model) do
    PinGate.close(model.gate)
    checkpoint(%{model | open?: false}, :refuse)
  end

  defp request_for(:reply), do: {request(:produce), []}
  defp request_for(:late), do: {request(:heartbeat), []}
  defp request_for(:never), do: {request(:offset_commit), []}
  defp request_for(:acks0), do: {request(:produce, acks: 0), [reply?: false]}

  defp ask(model, kind, call) do
    tag = "#{kind}-#{System.unique_integer([:positive])}"
    test = self()
    asked_at = now()
    pid = spawn(fn -> send(test, {:answer, tag, call.(tag), now()}) end)
    caller = %{tag: tag, kind: kind, pid: pid, asked_at: asked_at, closed_down?: false}
    settle(%{model | pending: Map.put(model.pending, tag, caller)}, tag)
  end

  # Settled once the Connection holds the call or answered it, so the next command meets a known state.
  defp settle(model, tag, deadline \\ now() + 2_000) do
    %{pid: pid} = model.pending[tag]
    {_state, data} = :sys.get_state(model.conn)

    cond do
      not Process.alive?(pid) -> model
      pid in queued(data) -> put_in(model.pending[tag].closed_down?, not model.open?)
      pid in in_flight(data) -> model
      now() >= deadline -> flunk("#{tag} never reached the Connection")
      true -> settle(model, tag, deadline)
    end
  end

  defp queued(data) do
    waiters = for %{from: {pid, _tag}} <- data.waiting, do: pid
    waiters ++ for {pid, _tag} <- data.connectors, do: pid
  end

  defp in_flight(data), do: for({_id, %{from: {pid, _tag}}} <- Map.to_list(data.in_flight), do: pid)

  defp await_down(conn, deadline \\ now() + 1_000) do
    cond do
      connection_state(conn) != :connected -> :ok
      now() >= deadline -> flunk("a peer close left the connection up")
      true -> await_down(conn, deadline)
    end
  end

  defp await_first(model, []), do: model

  defp await_first(model, due) do
    {at, pid} = Enum.min(due)
    {tag, _caller} = Enum.find(model.pending, fn {_tag, caller} -> caller.pid == pid end)

    receive do
      {:answer, ^tag, result, answered_at} -> answered(model, tag, result, answered_at)
    after
      max(at - now(), 0) + @slack -> flunk("#{tag} was not answered by its timer")
    end
  end

  defp await_answer(tag, model) do
    caller = model.pending[tag]

    receive do
      {:answer, ^tag, result, answered_at} -> answered(model, tag, result, answered_at)
    after
      max(caller.asked_at + bound(caller.kind) - now(), 0) -> flunk("#{tag} was never answered")
    end
  end

  defp answered(model, tag, result, answered_at) do
    {caller, pending} = Map.pop!(model.pending, tag)
    took = answered_at - caller.asked_at

    assert took <= bound(caller.kind), "#{tag} took #{took} ms"
    assert valid?(caller, result), "#{inspect(caller)} got #{inspect(result)}"
    %{model | pending: pending}
  end

  defp bound(:connect), do: @max_backoff + @slack
  defp bound(_request), do: @window + @reply_timeout + @slack

  defp valid?(%{kind: :connect, closed_down?: true}, result), do: result == {:error, {:connect, :econnrefused}}
  defp valid?(%{kind: :connect}, {:ok, versions}), do: is_map(versions)
  defp valid?(%{kind: :connect}, {:error, _reason}), do: true
  defp valid?(%{closed_down?: true}, result), do: match?({:error, :not_connected, _meta}, result)
  defp valid?(_caller, {:error, reason, %{bytes_received: 0}}), do: reason in @errors
  defp valid?(%{kind: :acks0}, {:ok, :no_response, %{bytes_sent: sent, bytes_received: 0}}), do: sent > 0

  defp valid?(%{kind: kind, tag: tag}, {:ok, <<_id::32-signed, echoed::binary>> = frame, meta}) do
    kind in [:reply, :late] and echoed == tag and meta.bytes_received == byte_size(frame)
  end

  defp valid?(_caller, _result), do: false

  defp checkpoint(model, command) do
    at = now()
    {state, data, timers} = status(model.conn)
    model = drain(model, command)

    assert_invariants(state, data, timers, at)
    refute model.down_closed? and state == :connected, "connected through a closed gate"
    track_socket(%{model | state: state, down_closed?: not model.open? and state != :connected}, data.socket)
  end

  defp status(conn) do
    {:status, ^conn, _module, [_dictionary, _running, _parent, _debug, items]} = :sys.get_status(conn)
    [general, [{~c"State", {state, data}}]] = Keyword.get_values(items, :data)
    {~c"Time-outs", {_count, timeouts}} = List.keyfind(general, ~c"Time-outs", 0)
    {state, data, for({type, _message} <- timeouts, do: type)}
  end

  # A run that failed, then shrank, leaves its Connection and callers behind: their messages are dropped here.
  defp drain(model, command) do
    %{conn: conn, monitor: monitor, host: host, pending: pending} = model

    receive do
      {:answer, tag, result, answered_at} when is_map_key(pending, tag) ->
        model |> answered(tag, result, answered_at) |> drain(command)

      {:closed, %{host: ^host, reason: reason}} ->
        assert reason in [:liveness, :send_error | Map.get(@drop_reasons, command, [])],
               "closed #{reason} on #{command}"

        drain(model, command)

      {:call_in, ^conn, from} ->
        refute Map.has_key?(model.calls, from)
        drain(put_in(model.calls[from], :open), command)

      {:call_out, ^conn, from} ->
        assert model.calls[from] == :open, "a second answer to #{inspect(from)}"
        drain(put_in(model.calls[from], :answered), command)

      {:DOWN, ^monitor, :process, _pid, reason} ->
        flunk("the Connection stopped: #{inspect(reason)}")

      {:closed, _other_run} ->
        drain(model, command)

      {call, _other_run, _from} when call in [:call_in, :call_out] ->
        drain(model, command)

      {:answer, _other_run, _result, _answered_at} ->
        drain(model, command)
    after
      0 -> model
    end
  end

  defp assert_invariants(:connected, data, timers, at) do
    assert data.socket
    assert data.waiting == [] and data.connectors == []
    entries = Map.to_list(data.in_flight)
    if entries != [], do: assert({:timeout, :liveness} in timers)

    for {id, entry} <- entries do
      assert entry.live_until >= at - @grace, "id #{id} outlived its liveness bound"
      if entry.from, do: assert({:timeout, {:reply, id}} in timers and entry.reply_by >= at - @grace)
    end
  end

  defp assert_invariants(state, data, timers, at) when state in [:disconnected, :reconnecting] do
    assert data.socket == nil and data.in_flight == %{}

    for waiter <- data.waiting do
      assert {:timeout, {:window, waiter.ref}} in timers
      assert waiter.enqueued_at + @window >= at - @grace, "a waiter outlived its window"
    end

    if state == :disconnected, do: assert(data.waiting == [] and data.connectors == [])
    if state == :reconnecting, do: assert(:state_timeout in timers)
  end

  defp assert_invariants(_setup_state, _data, _timers, _at), do: :ok

  defp track_socket(%{socket: socket} = model, socket), do: model
  defp track_socket(model, nil), do: model

  defp track_socket(model, socket) do
    %{model | socket: socket, old_sockets: Enum.reject([model.socket | model.old_sockets], &is_nil/1)}
  end

  defp finish(model) do
    FakeTransport.release(model.fake, :heartbeat)
    model = model.pending |> Map.keys() |> Enum.reduce(model, &await_answer/2)
    model = model |> await_drained() |> checkpoint(:finish)

    assert Enum.all?(model.calls, fn {_from, answer} -> answer == :answered end)
    %{monitor: monitor, host: host} = model
    ConnectionStateMachine.stop(model.conn, :done)
    assert_receive {:DOWN, ^monitor, :process, _pid, :normal}, 1_000
    if model.state == :connected, do: assert_received({:closed, %{host: ^host, reason: :done}})
  end

  defp await_drained(model, deadline \\ now() + @slack) do
    {_state, data} = :sys.get_state(model.conn)

    cond do
      data.in_flight == %{} -> model
      now() >= deadline -> flunk("in-flight ids never drained: #{inspect(Map.to_list(data.in_flight))}")
      true -> await_drained(model, deadline)
    end
  end
end
