defmodule KafkaEx.Client.ConnectionStateMachine do
  @moduledoc """
  One process per TCP/TLS connection to one broker in one role (`:data`, `:coordinator`, `:metadata`
  or `{:fetch, topic, partition}`), started under the client's supervisor.

  It owns the connection's whole life: connect, ApiVersions, optional SASL, then requests and their
  replies, liveness, and reconnecting with backoff. Its states are `:disconnected`, `:reconnecting`,
  `:connecting`, `:negotiating`, `:authenticating` and `:connected`.

  `await/3` queues the caller for up to `reconnect_window` while not connected, `write/3` sends one
  request on a connected socket, and `connect/2` connects and returns the negotiated API versions.
  Its timers are liveness (each reply's deadline plus `liveness_slack`), idle (the fetch role
  only) and stable, which resets the reconnect backoff.

  Every connection to one broker address shares one reconnect gate in the client's table
  (`reconnect_backoff_table/0`): a connect opens it, a failed attempt closes it, and a closed gate
  grants one attempt per interval (100 ms doubling with jitter to about 500 ms), as Kafka's
  `reconnect.backoff.ms` spaces only attempts after a failure.
  """
  @behaviour :gen_statem

  require Logger

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Auth.SASL
  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Client.ConnectionRegistry
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Support.Retry
  alias KafkaEx.Telemetry

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)
  @idle_jitter 0.2
  @max_correlation_id 2_147_483_647
  @reconnect_base_ms 100
  @reconnect_cap_ms 500

  @meta0 %{bytes_sent: 0, bytes_received: 0}
  @setup_states [:connecting, :negotiating, :authenticating]

  @type role :: :data | :coordinator | :metadata | {:fetch, topic :: String.t(), partition :: non_neg_integer()}
  @type dispatch :: %{
          optional(:as_is?) => boolean(),
          node_id: non_neg_integer() | nil,
          request: struct(),
          info: {atom(), non_neg_integer()},
          reply?: boolean(),
          timeout: pos_integer(),
          long_poll: non_neg_integer() | nil,
          wait?: boolean(),
          deadline: integer()
        }
  @type meta :: %{
          required(:bytes_sent) => non_neg_integer(),
          required(:bytes_received) => non_neg_integer(),
          optional(:correlation_id) => integer()
        }

  @spec child_spec(map()) :: Supervisor.child_spec()
  def child_spec(args) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [args]}, restart: :temporary, shutdown: 2_000}
  end

  @spec start_link(map()) :: :gen_statem.start_ret()
  def start_link(args), do: :gen_statem.start_link(__MODULE__, args, [])

  @spec await(pid(), dispatch(), timeout()) :: :ok | {:error, term(), meta()}
  def await(pid, dispatch, timeout), do: :gen_statem.call(pid, {:await, dispatch}, timeout)

  @spec write(pid(), dispatch(), timeout()) :: {:ok, binary() | :no_response, meta()} | {:error, term(), meta()}
  def write(pid, dispatch, timeout), do: :gen_statem.call(pid, {:write, dispatch}, timeout)

  @spec connect(pid(), timeout()) ::
          {:ok, %{non_neg_integer() => {non_neg_integer(), non_neg_integer()}}} | {:error, term()}
  def connect(pid, timeout), do: :gen_statem.call(pid, :connect, timeout)

  @spec stop(pid(), atom()) :: :ok
  def stop(pid, reason), do: :gen_statem.cast(pid, {:stop, reason})

  @spec reconnect_backoff_table() :: :ets.tid()
  def reconnect_backoff_table do
    :ets.new(:reconnect_backoff, [:set, :public, read_concurrency: true, write_concurrency: true])
  end

  @impl :gen_statem
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl :gen_statem
  def init(args) do
    Process.flag(:trap_exit, true)
    opts = options()

    data =
      args
      |> Map.take([:host, :port, :role, :use_ssl, :secrets, :reconnect_backoff, :registry])
      |> Map.merge(%{
        opts: opts,
        owner_ref: Process.monitor(args.client),
        socket: nil,
        node_id: nil,
        api_versions: nil,
        in_flight: %{},
        next_id: 0,
        waiting: [],
        connectors: [],
        setup_deadline: nil,
        outage: nil,
        attempted?: false,
        spans: %{},
        # The armed liveness deadline, kept so a request costs O(1) however many others are in flight.
        liveness_at: nil
      })

    {:ok, :disconnected, data}
  end

  @doc false
  @spec options() :: map()
  def options do
    Map.merge(KafkaEx.Config.connection(), %{
      connect_timeout: KafkaEx.Config.connect_timeout(),
      client_id: KafkaEx.Config.client_id()
    })
  end

  @impl :gen_statem
  def handle_event(:enter, _old, :connected, data), do: {:keep_state_and_data, idle_when_empty(data)}

  def handle_event(:enter, _old, :disconnected, data), do: {:keep_state_and_data, idle_without_demand(data)}

  def handle_event(:enter, _old, _state, _data), do: :keep_state_and_data

  def handle_event({:call, from}, {:await, dispatch}, :connected, data) do
    {:keep_state, named(data, dispatch), [{:reply, from, readiness(dispatch, data)}]}
  end

  def handle_event({:call, from}, {:await, dispatch}, state, data) do
    enqueue(from, dispatch, state, named(data, dispatch))
  end

  # A caller that died while its write waited, such as a consumer closing mid-join, must not have it sent (KIP-848).
  def handle_event({:call, {caller, _tag} = from}, {:write, dispatch}, :connected, data) do
    if Process.alive?(caller), do: send_request(from, dispatch, data), else: :keep_state_and_data
  end

  # The caller saw this connection up and committed to the write: a drop since then is a dead socket's send.
  def handle_event({:call, from}, {:write, _dispatch}, _state, _data) do
    {:keep_state_and_data, [{:reply, from, {:error, :closed, @meta0}}]}
  end

  def handle_event({:call, from}, :connect, :connected, data) do
    {:keep_state_and_data, [{:reply, from, {:ok, data.api_versions}}]}
  end

  def handle_event({:call, from}, :connect, state, data) do
    data = %{data | connectors: data.connectors ++ [from]}
    {:keep_state, data, [cancel(:idle) | try_connect_when(state)]}
  end

  def handle_event({:timeout, {:window, ref}}, nil, _state, data), do: window_expired(ref, data)

  def handle_event(event, :try_connect, state, data)
      when state in [:disconnected, :reconnecting] and event in [:internal, :state_timeout] do
    try_connect(data)
  end

  def handle_event(:info, :reconnect_slot, :reconnecting, %{attempted?: false} = data), do: try_connect(data)

  def handle_event(:info, :reconnect_slot, _state, _data), do: :keep_state_and_data

  def handle_event(:internal, :run, state, data) when state in @setup_states do
    guarded(data, fn -> run(state, data) end)
  end

  def handle_event(:internal, :flush, :connected, data), do: flush(data)

  def handle_event(:info, {tag, raw, frame}, :connected, %{socket: %{socket: raw}} = data) when tag in [:tcp, :ssl] do
    received(frame, data)
  end

  def handle_event(:info, {tag, raw}, :connected, %{socket: %{socket: raw}} = data)
      when tag in [:tcp_closed, :ssl_closed] do
    peer_closed(data)
  end

  def handle_event(:info, {tag, raw, reason}, :connected, %{socket: %{socket: raw}} = data)
      when tag in [:tcp_error, :ssl_error] do
    recv_error(reason, data)
  end

  def handle_event({:timeout, {:reply, id}}, nil, :connected, data), do: reply_timed_out(id, data)

  def handle_event({:timeout, :liveness}, nil, :connected, data), do: liveness(data)

  # Unregistered first, so a call left in the mailbox exits :normal and its caller reconnects through the registry.
  def handle_event({:timeout, :idle}, nil, _state, data) do
    :ok = ConnectionRegistry.unregister(data.registry, {data.host, data.port, data.role})
    {:stop, :normal, close_socket(data, :idle)}
  end

  def handle_event({:timeout, :stable}, nil, :connected, data) do
    :ets.update_element(data.reconnect_backoff, key(data), {3, 0})
    :keep_state_and_data
  end

  def handle_event(:cast, {:stop, reason}, _state, data), do: stopped(reason, data)

  # A gen_tcp port that dies without a close message leaves no :tcp_closed behind.
  def handle_event(:info, {:EXIT, raw, _reason}, :connected, %{socket: %{socket: raw}} = data) do
    recv_error(:closed, data)
  end

  def handle_event(:info, {:DOWN, ref, :process, _pid, reason}, _state, %{owner_ref: ref} = data) do
    stopped(if(reason == {:shutdown, :init_error}, do: :init_error, else: :shutdown), data)
  end

  def handle_event(:info, {:EXIT, _pid, _reason}, _state, _data), do: :keep_state_and_data

  def handle_event(type, content, _state, _data) do
    Logger.debug("#{inspect(__MODULE__)} ignoring #{inspect(type)} #{inspect(content, limit: 5)}")
    :keep_state_and_data
  end

  @impl :gen_statem
  def terminate(_reason, _state, data) do
    close_socket(data, :shutdown)
    :ok
  end

  @impl :gen_statem
  def format_status(%{data: data} = status), do: %{status | data: redact(data)}

  defp redact(%{secrets: _, waiting: waiting} = data) do
    %{data | secrets: :redacted, waiting: Enum.map(waiting, &redact_waiter/1)}
  end

  defp redact_waiter(%{dispatch: %{request: %module{}} = dispatch} = waiter) do
    %{waiter | dispatch: %{dispatch | request: module}}
  end

  defp enqueue(from, dispatch, state, data) do
    if expired?(dispatch) do
      {:keep_state_and_data, [{:reply, from, {:error, :timeout, @meta0}}]}
    else
      ref = make_ref()
      waiter = %{ref: ref, from: from, dispatch: dispatch, enqueued_at: now()}
      data = %{data | waiting: data.waiting ++ [waiter]}
      window = {{:timeout, {:window, ref}}, data.opts.reconnect_window, nil}
      {:keep_state, data, [window, cancel(:idle) | try_connect_when(state)]}
    end
  end

  # An outage is spent when an attempt fails after a window has run out in it; only a connect resets it.
  defp outage_after_failure(%{outage: :window_expired}), do: :spent
  defp outage_after_failure(data), do: data.outage

  defp try_connect_when(:disconnected), do: [{:next_event, :internal, :try_connect}]
  defp try_connect_when(_state), do: []

  defp window_expired(ref, data) do
    case Enum.split_with(data.waiting, &(&1.ref == ref)) do
      {[], _} ->
        :keep_state_and_data

      {[waiter], waiting} ->
        {:keep_state, window_ran_out(%{data | waiting: waiting}), [{:reply, waiter.from, not_connected()}]}
    end
  end

  defp window_ran_out(%{outage: :spent} = data), do: data
  defp window_ran_out(data), do: %{data | outage: :window_expired}

  # Back-to-back attempts run as inserted events, ahead of any window timeout already in the mailbox.
  defp answer_overdue(data) do
    now = now()

    case answer_waiters(data, &(now - &1.enqueued_at >= data.opts.reconnect_window)) do
      {[], _data} -> {[], data}
      {answers, data} -> {answers, window_ran_out(data)}
    end
  end

  defp try_connect(data) do
    {overdue, data} = answer_overdue(data)

    with true <- demand?(data),
         :ok <- claim_slot(data.reconnect_backoff, key(data), now()) do
      unless data.attempted?, do: :ets.delete(data.reconnect_backoff, {:first_connect, key(data), self()})
      {:next_state, :connecting, %{data | attempted?: true}, overdue ++ [run()]}
    else
      false -> {:next_state, :disconnected, data, overdue ++ idle_without_demand(data)}
      {:wait, ms} -> wait_for_slot(ms, data, overdue)
    end
  end

  defp wait_for_slot(ms, data, overdue) do
    {answers, data} = if data.outage == :spent, do: answer_waiters(data, fn _waiter -> true end), else: {[], data}

    # Idle is armed here too: staying :disconnected runs no state enter, and the enqueue cancelled it.
    if demand?(data) do
      # A connection that has not attempted yet waits on another's reservation only until that one connects.
      unless data.attempted?, do: :ets.insert(data.reconnect_backoff, {{:first_connect, key(data), self()}})
      {:next_state, :reconnecting, data, overdue ++ answers ++ [{:state_timeout, ms, :try_connect}]}
    else
      {:next_state, :disconnected, data, overdue ++ answers ++ idle_without_demand(data)}
    end
  end

  defp answer_waiters(data, answer?) do
    {answered, waiting} = Enum.split_with(data.waiting, answer?)
    actions = Enum.flat_map(answered, &[{:reply, &1.from, not_connected()}, cancel({:window, &1.ref})])
    {actions, %{data | waiting: waiting}}
  end

  defp demand?(data), do: data.waiting != [] or data.connectors != []

  defp run(:connecting, data) do
    data = %{data | setup_deadline: now() + data.opts.connect_timeout}
    data = start_span(data, :connection, Telemetry.connection_metadata(data.host, data.port, data.use_ssl))
    guarded(data, fn -> connect(data) end)
  end

  defp run(:negotiating, data) do
    case data.secrets.() do
      %{auth: nil} -> negotiate(data)
      %{auth: auth} -> authenticate_config(auth, data)
    end
  end

  defp run(:authenticating, data) do
    %{auth: auth} = data.secrets.()
    sasl_opts = [api_versions: data.api_versions, deadline: data.setup_deadline]

    case SASL.authenticate(data.socket, enrich(auth, data.host, data.port), sasl_opts) do
      :ok -> data |> stop_span(:auth, %{}) |> activate()
      {:error, :timeout} -> setup_timed_out(data)
      {:error, reason} -> auth_failed(reason, data)
    end
  end

  defp connect(data) do
    %{ssl_options: ssl_options} = data.secrets.()
    address = %{host: data.host, port: data.port, role: data.role}

    case NetworkClient.connect(address, base_options(data) ++ ssl_options, data.use_ssl, data.opts.connect_timeout) do
      {:ok, socket} ->
        {:next_state, :negotiating, %{data | socket: socket}, [run()]}

      {:error, reason} ->
        Logger.error("Could not connect to broker #{peer(data)} because of error #{inspect(reason)}")
        if reason == :timeout, do: setup_timed_out(data), else: setup_failed({:connect, reason}, data)
    end
  end

  defp run, do: {:next_event, :internal, :run}

  defp base_options(data) do
    [:binary, {:packet, 4}, {:active, false}, {:send_timeout, data.opts.send_timeout}, {:send_timeout_close, true}]
  end

  defp remaining(data) do
    case data.setup_deadline - now() do
      ms when ms > 0 -> {:ok, ms}
      _ -> {:error, :timeout}
    end
  end

  # The auth span opens before ApiVersions, as SASL's own exchange did, so a refused config writes nothing.
  defp authenticate_config(auth, data) do
    data = start_span(data, :auth, Telemetry.auth_metadata(data.host, data.port, AuthConfig.mechanism_name(auth)))

    guarded(data, fn ->
      case VersionSupport.validate_config(auth, data.socket) do
        :ok -> negotiate(data)
        {:error, reason} -> auth_failed(reason, data)
      end
    end)
  end

  defp negotiate(data) do
    with {:ok, remaining} <- remaining(data),
         {:ok, api_versions} <- exchange_api_versions(data, remaining) do
      data = %{data | api_versions: api_versions}
      if data.spans[:auth], do: {:next_state, :authenticating, data, [run()]}, else: activate(data)
    else
      {:error, :timeout} -> setup_timed_out(data)
      {:error, reason} -> setup_failed({:api_versions, reason}, data)
    end
  end

  defp exchange_api_versions(data, remaining) do
    {id, _data} = next_id(data)
    request = %{@protocol.build_request(:api_versions, 0, []) | correlation_id: id, client_id: data.opts.client_id}

    with :ok <- NetworkClient.write(data.socket, @protocol.serialize_request(request)),
         {:ok, frame} <- NetworkClient.read(data.socket, remaining) do
      parse_api_versions(frame, id, request)
    end
  end

  defp parse_api_versions(<<id::32-signed, _::binary>> = frame, id, request) do
    {response, _rest} = @protocol.response_deserializer(request).(frame)

    case @protocol.parse_response(:api_versions, response) do
      {:ok, api_versions} -> {:ok, State.api_version_ranges(api_versions)}
      {:error, error} -> {:error, error}
    end
  rescue
    _malformed -> {:error, :parse_error}
  end

  defp parse_api_versions(_frame, _id, _request), do: {:error, :parse_error}

  defp activate(data) do
    case NetworkClient.activate(data.socket) do
      :ok ->
        open_gate(data.reconnect_backoff, key(data))
        data = stop_span(data, :connection, %{success: true})
        replies = for from <- data.connectors, do: {:reply, from, {:ok, data.api_versions}}
        stable = {{:timeout, :stable}, data.opts.stable_period, nil}
        data = %{data | connectors: [], outage: nil}
        {:next_state, :connected, data, replies ++ [stable, {:next_event, :internal, :flush}]}

      {:error, reason} ->
        setup_failed({:connect, reason}, data)
    end
  end

  defp auth_failed(reason, data) do
    data = stop_span(data, :auth, %{result: :error, error: reason})
    data = close_socket(data, :auth_error)
    Logger.error("SASL authentication failed for broker #{peer(data)}: #{inspect(reason)}")
    setup_failed({:auth, reason}, data)
  end

  defp setup_timed_out(data) do
    if data.socket, do: Telemetry.emit_connection_close(data.host, data.port, :setup_timeout)
    setup_failed(:setup_timeout, data)
  end

  defp setup_failed(reason, data) do
    close_gate(data.reconnect_backoff, key(data), now())
    data = data |> close_socket(nil) |> stop_span(:auth, %{result: :error, error: unwrap(reason)})
    data = stop_span(data, :connection, %{success: false, error: unwrap(reason)})
    data = %{data | outage: outage_after_failure(data)}
    spent = data.outage == :spent
    {answers, data} = answer_waiters(data, &(spent or gives_up?(&1)))
    connectors = for from <- data.connectors, do: {:reply, from, {:error, reason}}
    data = %{data | connectors: []}
    {:next_state, :disconnected, data, answers ++ connectors ++ try_connect_if_waiting(data)}
  end

  defp unwrap({step, reason}) when step in [:connect, :api_versions, :auth], do: reason
  defp unwrap(:setup_timeout), do: :timeout

  defp gives_up?(waiter), do: not waiter.dispatch.wait? or expired?(waiter.dispatch)

  # `data` must carry every span the step opened, so the guard sits right after each start_span/3.
  defp guarded(data, step) do
    step.()
  catch
    kind, reason -> handshake_failed(kind, reason, data)
  end

  # Only the exception's module or the kind leaves the process: any term may hold a credential.
  defp handshake_failed(kind, reason, data) do
    summary = if kind == :error, do: Exception.normalize(:error, reason, []).__struct__, else: kind
    close_gate(data.reconnect_backoff, key(data), now())
    data = data |> span_exception(:auth, summary) |> span_exception(:connection, summary) |> close_socket(nil)
    {:stop, {:handshake_failed, summary}, data}
  end

  defp claim_slot(table, key, now) do
    :ets.insert_new(table, {key, now, 0})

    case :ets.lookup(table, key) do
      [{^key, :open, _failures}] -> :ok
      [{^key, next_at, _failures}] when next_at > now -> {:wait, next_at - now}
      [row] -> if reserve_next_slot(table, row, now), do: :ok, else: claim_slot(table, key, now)
    end
  end

  defp close_gate(table, key, now) do
    case :ets.lookup(table, key) do
      [{^key, :open, _failures} = row] -> reserve_next_slot(table, row, now) or close_gate(table, key, now)
      _closed -> false
    end
  end

  defp open_gate(table, key) do
    :ets.update_element(table, key, {2, :open})

    for [pid] <- :ets.match(table, {{:first_connect, key, :"$1"}}) do
      :ets.delete(table, {:first_connect, key, pid})
      send(pid, :reconnect_slot)
    end
  end

  # One winner among concurrent callers: the swap applies only to the row each of them read.
  defp reserve_next_slot(table, {key, _gate, failures} = row, now) do
    delay = Retry.backoff_delay_jittered(failures, @reconnect_base_ms, @reconnect_cap_ms)
    :ets.select_replace(table, [{row, [], [{:const, {key, now + delay, failures + 1}}]}]) == 1
  end

  defp try_connect_if_waiting(data), do: if(demand?(data), do: [{:next_event, :internal, :try_connect}], else: [])

  defp flush(data) do
    answers =
      Enum.flat_map(data.waiting, &[cancel({:window, &1.ref}), {:reply, &1.from, readiness(&1.dispatch, data)}])

    {:keep_state, %{data | waiting: []}, answers}
  end

  defp send_request(from, dispatch, data) do
    case readiness(dispatch, data) do
      :ok -> write_request(from, dispatch, data)
      refused -> {:keep_state, data, [{:reply, from, refused}]}
    end
  end

  defp readiness(dispatch, data) do
    cond do
      expired?(dispatch) ->
        {:error, :timeout, @meta0}

      version_refused?(dispatch, data.api_versions) ->
        {:error, {:unsupported_version, data.api_versions}, @meta0}

      true ->
        :ok
    end
  end

  defp version_refused?(%{as_is?: true}, _api_versions), do: false
  defp version_refused?(dispatch, api_versions), do: not negotiated?(dispatch.info, api_versions)

  defp negotiated?({operation, version}, api_versions) do
    in_range?(Map.get(api_versions, @protocol.api_key(operation)), version)
  end

  defp in_range?({min, max}, version), do: version >= min and version <= max
  defp in_range?(nil, _version), do: false

  defp write_request(from, dispatch, data) do
    {id, advanced} = next_id(data)

    case serialize(%{dispatch.request | correlation_id: id}) do
      {:ok, wire} ->
        write_wire(from, dispatch, id, wire, advanced)

      {:exception, _kind, _reason, _stacktrace} = raised ->
        {:keep_state, data, [{:reply, from, {:error, raised, @meta0}}]}
    end
  end

  # Ids wrap at INT32 and skip any still in flight: an abandoned request awaits its late reply under its id.
  defp next_id(%{next_id: id, in_flight: in_flight} = data) do
    data = %{data | next_id: if(id == @max_correlation_id, do: 0, else: id + 1)}
    if is_map_key(in_flight, id), do: next_id(data), else: {id, data}
  end

  # A request that cannot be encoded is the caller's fault, not the socket's: nothing is written or closed.
  defp serialize(request) do
    {:ok, @protocol.serialize_request(request)}
  catch
    kind, reason -> {:exception, kind, reason, __STACKTRACE__}
  end

  defp write_wire(from, dispatch, id, wire, data) do
    bytes = IO.iodata_length(wire)

    case NetworkClient.write(data.socket, wire) do
      :ok -> written(from, dispatch, id, bytes, data)
      {:error, reason} -> send_failed(from, dispatch, reason, {id, bytes}, data)
    end
  end

  defp written(from, %{reply?: false}, id, bytes, data) do
    meta = %{bytes_sent: bytes, bytes_received: 0, correlation_id: id}
    {:keep_state, data, [{:reply, from, {:ok, :no_response, meta}}]}
  end

  defp written(from, dispatch, id, bytes, data) do
    now = now()
    {operation, _version} = dispatch.info
    live_until = now + (dispatch.long_poll || 0) + dispatch.timeout + data.opts.liveness_slack

    entry = %{
      from: from,
      sent_at: now,
      reply_by: now + dispatch.timeout,
      live_until: live_until,
      bytes_sent: bytes,
      api: operation
    }

    {data, liveness} = arm_liveness(%{data | in_flight: Map.put(data.in_flight, id, entry)}, live_until)
    {:keep_state, data, [{{:timeout, {:reply, id}}, dispatch.timeout, nil}, cancel(:idle) | liveness]}
  end

  defp send_failed(from, dispatch, reason, {id, bytes}, data) do
    verb = if dispatch.reply?, do: "Sending", else: "Asynchronously sending"
    Logger.error("#{verb} data to broker #{peer(data)} failed with #{inspect(reason)}")
    data = close_socket(data, :send_error)

    # Siblings share the send's reason: a :timeout is send_timeout_close, a client-side close.
    {data, replies} = answer_outstanding(data, reason)
    meta = %{bytes_sent: bytes, bytes_received: 0, correlation_id: id}
    disconnect(data, [{:reply, from, {:error, reason, meta}} | replies])
  end

  defp received(<<id::32-signed, _::binary>> = frame, data) do
    case Map.pop(data.in_flight, id) do
      {%{from: nil}, in_flight} ->
        Logger.debug("Dropped a late reply with correlation id #{id} from broker #{peer(data)}")
        rearm(%{data | in_flight: in_flight}, [])

      {entry, in_flight} when entry != nil ->
        meta = %{bytes_sent: entry.bytes_sent, bytes_received: byte_size(frame), correlation_id: id}
        rearm(%{data | in_flight: in_flight}, [cancel({:reply, id}), {:reply, entry.from, {:ok, frame, meta}}])

      {nil, _in_flight} ->
        close_client_side(:correlation_mismatch, data)
    end
  end

  defp received(_frame, data), do: close_client_side(:correlation_mismatch, data)

  defp rearm(data, actions) do
    case NetworkClient.activate(data.socket) do
      :ok -> keep_reading(data, actions)
      {:error, _reason} -> peer_closed(data, actions)
    end
  end

  defp keep_reading(%{in_flight: in_flight} = data, actions) when in_flight == %{} do
    {:keep_state, %{data | liveness_at: nil}, actions ++ [cancel(:liveness) | idle_when_empty(data)]}
  end

  defp keep_reading(data, actions), do: {:keep_state, data, actions}

  defp arm_liveness(%{liveness_at: at} = data, live_until) when is_integer(at) and at <= live_until, do: {data, []}
  defp arm_liveness(data, live_until), do: {%{data | liveness_at: live_until}, [liveness_timer(live_until)]}

  defp liveness_timer(nil), do: cancel(:liveness)
  defp liveness_timer(at), do: {{:timeout, :liveness}, at, nil, [abs: true]}

  defp idle_without_demand(data), do: if(fetch?(data) and not demand?(data), do: [idle(data)], else: [])

  defp idle_when_empty(data), do: if(fetch?(data) and data.in_flight == %{}, do: [idle(data)], else: [])

  defp idle(data) do
    jitter = @idle_jitter * (2 * :rand.uniform() - 1)
    {{:timeout, :idle}, round(data.opts.idle_timeout * (1 + jitter)), nil}
  end

  defp fetch?(%{role: {:fetch, _topic, _partition}}), do: true
  defp fetch?(_data), do: false

  defp reply_timed_out(id, data) do
    case data.in_flight do
      %{^id => %{from: from} = entry} when from != nil ->
        Logger.error("Receiving data from broker #{peer(data)} timed out")
        in_flight = Map.put(data.in_flight, id, %{entry | from: nil})
        {:keep_state, %{data | in_flight: in_flight}, [{:reply, from, {:error, :timeout, meta(id, entry)}}]}

      _ ->
        :keep_state_and_data
    end
  end

  defp liveness(data) do
    now = now()

    if Enum.any?(data.in_flight, fn {_id, entry} -> entry.live_until <= now end) do
      close_client_side(:liveness, data)
    else
      at = data.in_flight |> Enum.map(fn {_id, entry} -> entry.live_until end) |> Enum.min(fn -> nil end)
      {:keep_state, %{data | liveness_at: at}, [liveness_timer(at)]}
    end
  end

  defp peer_closed(data, actions \\ []), do: dropped(:closed, :remote_closed, data, actions)

  defp recv_error(reason, data), do: dropped(reason, :recv_error, data, [])

  defp dropped(reason, idle_reason, data, actions) do
    log_drop(Enum.any?(data.in_flight, fn {_id, entry} -> entry.from != nil end), reason, idle_reason, data)
    {data, replies} = data |> close_socket(nil) |> answer_outstanding(reason)
    disconnect(data, actions ++ replies)
  end

  # A waiting caller meets the drop as a failed read, as a passive socket reported it.
  defp log_drop(true, reason, _idle_reason, data) do
    Logger.error("Receiving data from broker #{peer(data)} failed with #{inspect(reason)}")
    Telemetry.emit_connection_close(data.host, data.port, :recv_error)
  end

  defp log_drop(false, _reason, :remote_closed, data) do
    Logger.debug("#{describe_peer(data)} closed connection")
    Telemetry.emit_connection_close(data.host, data.port, :remote_closed)
  end

  defp log_drop(false, reason, :recv_error, data) do
    Logger.warning("#{describe_peer(data)} closed connection: #{inspect(reason)}")
    Telemetry.emit_connection_close(data.host, data.port, :recv_error)
  end

  defp close_client_side(reason, data) do
    {data, replies} = data |> close_socket(reason) |> answer_outstanding(:timeout)
    disconnect(data, replies)
  end

  defp stopped(reason, data) do
    {data, replies} = data |> close_socket(reason) |> answer_outstanding(:timeout)
    waiters = for waiter <- data.waiting, do: {:reply, waiter.from, not_connected()}
    connectors = for from <- data.connectors, do: {:reply, from, {:error, :not_connected}}
    replies = for {:reply, _from, _reply} = reply <- replies, do: reply
    {:stop_and_reply, :normal, replies ++ waiters ++ connectors, %{data | waiting: [], connectors: []}}
  end

  defp answer_outstanding(data, reason) do
    replies =
      for {id, %{from: from} = entry} <- data.in_flight, from, do: {:reply, from, {:error, reason, meta(id, entry)}}

    cancels = for {id, _entry} <- data.in_flight, do: cancel({:reply, id})
    {%{data | in_flight: %{}}, replies ++ cancels}
  end

  defp meta(id, entry), do: %{bytes_sent: entry.bytes_sent, bytes_received: 0, correlation_id: id}

  defp disconnect(data, actions) do
    data = %{data | socket: nil, liveness_at: nil}
    timers = [cancel(:liveness), cancel(:stable)]
    {:next_state, :disconnected, data, actions ++ timers ++ try_connect_if_waiting(data)}
  end

  defp close_socket(%{socket: nil} = data, _reason), do: data

  defp close_socket(data, reason) do
    if reason, do: Telemetry.emit_connection_close(data.host, data.port, reason)
    NetworkClient.close(data.socket)
    %{data | socket: nil}
  end

  # By hand, not :telemetry.span/3: the connection span covers several states.
  defp start_span(data, name, metadata) do
    context = make_ref()
    start = System.monotonic_time()
    measurements = %{monotonic_time: start, system_time: System.system_time()}
    :telemetry.execute([:kafka_ex, name, :start], measurements, Map.put(metadata, :telemetry_span_context, context))
    %{data | spans: Map.put(data.spans, name, %{context: context, start: start, metadata: metadata})}
  end

  defp stop_span(data, name, metadata), do: end_span(data, name, :stop, fn _span -> metadata end)

  defp span_exception(data, name, module) do
    failure = %{kind: :exit, reason: {:handshake_failed, module}, stacktrace: []}
    end_span(data, name, :exception, &Map.merge(&1.metadata, failure))
  end

  defp end_span(data, name, phase, metadata) do
    case Map.pop(data.spans, name) do
      {nil, _spans} ->
        data

      {span, spans} ->
        stop = System.monotonic_time()
        measurements = %{duration: stop - span.start, monotonic_time: stop}

        :telemetry.execute(
          [:kafka_ex, name, phase],
          measurements,
          Map.put(metadata.(span), :telemetry_span_context, span.context)
        )

        %{data | spans: spans}
    end
  end

  defp enrich(%AuthConfig{mechanism_opts: opts} = auth, host, port) do
    %{auth | mechanism_opts: Map.merge(opts || %{}, %{broker_host: host, broker_port: port})}
  end

  defp expired?(dispatch), do: now() >= dispatch.deadline
  defp not_connected, do: {:error, :not_connected, @meta0}
  defp cancel(name), do: {{:timeout, name}, :cancel}
  defp key(data), do: {data.host, data.port}
  defp peer(data), do: "#{inspect(data.host)}:#{inspect(data.port)}"

  defp named(data, %{node_id: nil}), do: data
  defp named(data, %{node_id: node_id}), do: %{data | node_id: node_id}

  defp describe_peer(%{node_id: nil} = data), do: "#{data.host}:#{data.port}"
  defp describe_peer(data), do: Broker.to_string(%Broker{node_id: data.node_id, host: data.host, port: data.port})
  defp now, do: System.monotonic_time(:millisecond)
end
