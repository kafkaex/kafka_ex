defmodule KafkaEx.Client.Connection do
  @moduledoc false
  @behaviour :gen_statem

  require Logger

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Auth.SASL
  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Client.Connection.InFlight
  alias KafkaEx.Client.Connection.Pacing
  alias KafkaEx.Client.ResponseParser
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Telemetry

  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  @meta0 %{bytes_sent: 0, bytes_received: 0}
  @setup_states [:connecting, :negotiating, :authenticating]

  @type role :: :data | :coordinator | :metadata | :fetch
  @type dispatch :: %{
          optional(:node_id) => non_neg_integer(),
          optional(:as_is?) => boolean(),
          request: struct(),
          info: {atom(), non_neg_integer()},
          reply?: boolean(),
          timeout: pos_integer(),
          long_poll: non_neg_integer() | nil,
          wait?: boolean(),
          deadline: integer()
        }
  @type meta :: %{bytes_sent: non_neg_integer(), bytes_received: non_neg_integer()}

  @spec child_spec(map()) :: Supervisor.child_spec()
  def child_spec(args),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [args]}, restart: :temporary, shutdown: 2_000}

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

  @impl :gen_statem
  def callback_mode, do: [:handle_event_function, :state_enter]

  @impl :gen_statem
  def init(args) do
    Process.flag(:trap_exit, true)
    opts = options(args.opts)

    data =
      args
      |> Map.take([:host, :port, :role, :use_ssl, :transport, :secrets, :pacing])
      |> Map.merge(%{
        opts: opts,
        socket: nil,
        node_id: nil,
        api_versions: nil,
        in_flight: InFlight.new(opts.initial_correlation_id),
        waiting: :queue.new(),
        connectors: [],
        setup_deadline: nil,
        outage: nil,
        attempted?: false,
        spans: %{}
      })

    {:ok, :disconnected, data}
  end

  @doc false
  @spec options(Keyword.t()) :: map()
  def options(opts) do
    %{
      connect_timeout: Keyword.get_lazy(opts, :connect_timeout, &KafkaEx.Config.connect_timeout/0),
      reconnect_window: Keyword.get(opts, :reconnect_window, 1_500),
      liveness_slack: Keyword.get(opts, :liveness_slack, 5_000),
      idle_timeout: Keyword.get(opts, :idle_timeout, 60_000),
      idle_jitter: Keyword.get(opts, :idle_jitter, 0.2),
      stable_period: Keyword.get(opts, :stable_period, 10_000),
      send_timeout: Keyword.get_lazy(opts, :send_timeout, &KafkaEx.Config.request_timeout/0),
      initial_correlation_id: Keyword.get(opts, :initial_correlation_id, 0),
      client_id: Keyword.get(opts, :client_id)
    }
  end

  @impl :gen_statem
  def handle_event(:enter, _old, :connected, data), do: {:keep_state_and_data, idle_when_empty(data)}

  def handle_event(:enter, _old, :disconnected, data), do: {:keep_state_and_data, idle_without_demand(data)}

  def handle_event(:enter, _old, _state, _data), do: :keep_state_and_data

  def handle_event({:call, from}, {:await, dispatch}, :connected, data),
    do: {:keep_state, named(data, dispatch), [{:reply, from, readiness(dispatch, data)}]}

  def handle_event({:call, from}, {:await, dispatch}, state, data),
    do: enqueue(from, dispatch, state, named(data, dispatch))

  def handle_event({:call, from}, {:write, dispatch}, :connected, data),
    do: finish(send_request(from, dispatch, data))

  # The caller saw this connection up and committed to the write: a drop since then is a dead socket's send.
  def handle_event({:call, from}, {:write, _dispatch}, _state, _data),
    do: {:keep_state_and_data, [{:reply, from, {:error, :closed, @meta0}}]}

  def handle_event({:call, from}, :connect, :connected, data),
    do: {:keep_state_and_data, [{:reply, from, {:ok, data.api_versions}}]}

  def handle_event({:call, from}, :connect, state, data) do
    data = %{data | connectors: data.connectors ++ [from]}
    {:keep_state, data, [cancel(:idle) | try_connect_when(state)]}
  end

  def handle_event({:timeout, {:window, ref}}, nil, _state, data), do: window_expired(ref, data)

  def handle_event(event, :try_connect, state, data)
      when state in [:disconnected, :reconnecting] and event in [:internal, :state_timeout],
      do: try_connect(data)

  def handle_event(:info, :pacing_slot, :reconnecting, %{attempted?: false} = data), do: try_connect(data)

  def handle_event(:info, :pacing_slot, _state, _data), do: :keep_state_and_data

  def handle_event(:internal, :run, state, data) when state in @setup_states,
    do: guarded(data, fn -> run(state, data) end)

  def handle_event(:internal, :flush, :connected, data), do: flush(data)

  def handle_event(:info, {tag, raw, frame}, :connected, %{socket: %{socket: raw}} = data) when tag in [:tcp, :ssl],
    do: received(frame, data)

  def handle_event(:info, {tag, raw}, :connected, %{socket: %{socket: raw}} = data)
      when tag in [:tcp_closed, :ssl_closed],
      do: peer_closed(data)

  def handle_event(:info, {tag, raw, reason}, :connected, %{socket: %{socket: raw}} = data)
      when tag in [:tcp_error, :ssl_error],
      do: recv_error(reason, data)

  def handle_event({:timeout, {:reply, id}}, nil, :connected, data), do: reply_timed_out(id, data)

  def handle_event({:timeout, :liveness}, nil, :connected, data), do: liveness(data)

  def handle_event({:timeout, :idle}, nil, _state, data) do
    {:stop, :normal, close_socket(data, :idle)}
  end

  def handle_event({:timeout, :stable}, nil, :connected, data) do
    Pacing.stable(data.pacing, key(data))
    :keep_state_and_data
  end

  def handle_event(:cast, {:stop, reason}, _state, data), do: stopped(reason, data)

  def handle_event(:info, {:EXIT, _pid, _reason}, _state, _data), do: :keep_state_and_data

  def handle_event({:call, from}, _request, _state, _data),
    do: {:keep_state_and_data, [{:reply, from, {:error, :unknown_call}}]}

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
  def format_status(status), do: Map.update(status, :data, nil, &redact/1)

  defp redact(%{secrets: _, waiting: waiting} = data) do
    waiting = :queue.from_list(for waiter <- :queue.to_list(waiting), do: redact_waiter(waiter))
    %{data | secrets: :redacted, waiting: waiting}
  end

  defp redact(data), do: data

  defp redact_waiter(%{dispatch: %{request: %module{}} = dispatch} = waiter),
    do: %{waiter | dispatch: %{dispatch | request: module}}

  defp enqueue(from, dispatch, state, data) do
    if expired?(dispatch) do
      {:keep_state_and_data, [{:reply, from, {:error, :timeout, @meta0}}]}
    else
      ref = make_ref()
      waiter = %{ref: ref, from: from, dispatch: dispatch, enqueued_at: now()}
      data = %{data | waiting: :queue.in(waiter, data.waiting)}
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
    case take_waiter(data.waiting, &(&1.ref == ref)) do
      {nil, _} ->
        :keep_state_and_data

      {waiter, waiting} ->
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

  defp take_waiter(queue, fun) do
    {taken, kept} = queue |> :queue.to_list() |> Enum.split_with(fun)
    {List.first(taken), :queue.from_list(kept)}
  end

  defp try_connect(data) do
    {overdue, data} = answer_overdue(data)

    with true <- demand?(data),
         :ok <- Pacing.claim(data.pacing, key(data), now()) do
      unless data.attempted?, do: Pacing.stop_awaiting_slot(data.pacing, key(data), self())
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
      unless data.attempted?, do: Pacing.await_slot(data.pacing, key(data), self())
      {:next_state, :reconnecting, data, overdue ++ answers ++ [{:state_timeout, ms, :try_connect}]}
    else
      {:next_state, :disconnected, data, overdue ++ answers ++ idle_without_demand(data)}
    end
  end

  defp answer_waiters(data, answer?) do
    {answered, waiting} = data.waiting |> :queue.to_list() |> Enum.split_with(answer?)
    actions = Enum.flat_map(answered, &[{:reply, &1.from, not_connected()}, cancel({:window, &1.ref})])
    {actions, %{data | waiting: :queue.from_list(waiting)}}
  end

  defp demand?(data), do: not :queue.is_empty(data.waiting) or data.connectors != []

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

    case data.transport.connect(address, base_options(data) ++ ssl_options, data.use_ssl, data.opts.connect_timeout) do
      {:ok, socket} ->
        {:next_state, :negotiating, %{data | socket: socket}, [run()]}

      {:error, reason} ->
        Logger.error("Could not connect to broker #{peer(data)} because of error #{inspect(reason)}")
        if reason == :timeout, do: setup_timed_out(data), else: setup_failed({:connect, reason}, data)
    end
  end

  defp run, do: {:next_event, :internal, :run}

  defp base_options(data),
    do: [:binary, {:packet, 4}, {:active, false}, {:send_timeout, data.opts.send_timeout}, {:send_timeout_close, true}]

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
    {id, _} = InFlight.next_id(data.in_flight)
    request = %{@protocol.build_request(:api_versions, 0, []) | correlation_id: id, client_id: data.opts.client_id}

    with :ok <- data.transport.write(data.socket, @protocol.serialize_request(request)),
         {:ok, frame} <- data.transport.read(data.socket, remaining) do
      parse_api_versions(frame, id, request)
    end
  end

  defp parse_api_versions(<<id::32-signed, _::binary>> = frame, id, request) do
    {response, _rest} = @protocol.response_deserializer(request).(frame)

    case ResponseParser.api_versions_response(response) do
      {:ok, api_versions} -> {:ok, State.api_version_ranges(api_versions)}
      {:error, error} -> {:error, error}
    end
  rescue
    _malformed -> {:error, :parse_error}
  end

  defp parse_api_versions(_frame, _id, _request), do: {:error, :parse_error}

  defp activate(data) do
    case data.transport.activate(data.socket) do
      :ok ->
        Pacing.connected(data.pacing, key(data), now())
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
    Logger.error("SASL authentication failed for broker #{peer(data)}}: #{inspect(reason)}")
    setup_failed({:auth, reason}, data)
  end

  defp setup_timed_out(data) do
    if data.socket, do: Telemetry.emit_connection_close(data.host, data.port, :setup_timeout)
    data = stop_span(data, :auth, %{result: :error, error: :timeout})
    setup_failed(:setup_timeout, data)
  end

  defp setup_failed(reason, data) do
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
  defp unwrap(reason), do: reason

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
    data = data |> span_exception(:auth, summary) |> span_exception(:connection, summary) |> close_socket(nil)
    {:stop, {:handshake_failed, summary}, data}
  end

  defp try_connect_if_waiting(data), do: if(demand?(data), do: [{:next_event, :internal, :try_connect}], else: [])

  defp flush(data) do
    answers =
      Enum.flat_map(
        :queue.to_list(data.waiting),
        &[cancel({:window, &1.ref}), {:reply, &1.from, readiness(&1.dispatch, data)}]
      )

    {:keep_state, %{data | waiting: :queue.new()}, answers}
  end

  defp finish({:cont, data, actions}), do: {:keep_state, data, actions}
  defp finish({:down, data, actions}), do: disconnect(data, actions)

  defp send_request(from, dispatch, data) do
    case readiness(dispatch, data) do
      :ok -> write_request(from, dispatch, data)
      refused -> {:cont, data, [{:reply, from, refused}]}
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

  defp negotiated?({:unknown, _version}, _api_versions), do: false

  defp negotiated?({operation, version}, api_versions),
    do: in_range?(Map.get(api_versions, @protocol.api_key(operation)), version)

  defp in_range?({min, max}, version), do: version >= min and version <= max
  defp in_range?(nil, _version), do: false

  defp write_request(from, dispatch, data) do
    {id, in_flight} = InFlight.next_id(data.in_flight)

    case serialize(%{dispatch.request | correlation_id: id}) do
      {:ok, wire} -> write_wire(from, dispatch, id, wire, %{data | in_flight: in_flight})
      {:exception, _kind, _reason, _stacktrace} = raised -> {:cont, data, [{:reply, from, {:error, raised, @meta0}}]}
    end
  end

  # A request that cannot be encoded is the caller's fault, not the socket's: nothing is written or closed.
  defp serialize(request) do
    {:ok, @protocol.serialize_request(request)}
  catch
    kind, reason -> {:exception, kind, reason, __STACKTRACE__}
  end

  defp write_wire(from, dispatch, id, wire, data) do
    bytes = IO.iodata_length(wire)

    case data.transport.write(data.socket, wire) do
      :ok -> written(from, dispatch, id, bytes, data)
      {:error, reason} -> send_failed(from, dispatch, reason, bytes, data)
    end
  end

  defp written(from, %{reply?: false}, _id, bytes, data),
    do: {:cont, data, [{:reply, from, {:ok, :no_response, %{bytes_sent: bytes, bytes_received: 0}}}]}

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

    data = %{data | in_flight: InFlight.put(data.in_flight, id, entry)}
    {:cont, data, [{{:timeout, {:reply, id}}, dispatch.timeout, nil}, liveness_timer(data), cancel(:idle)]}
  end

  defp send_failed(from, dispatch, reason, bytes, data) do
    verb = if dispatch.reply?, do: "Sending", else: "Asynchronously sending"
    Logger.error("#{verb} data to broker #{peer(data)} failed with #{inspect(reason)}")
    data = close_socket(data, :send_error)

    # Siblings share the send's reason: a :timeout is send_timeout_close, a client-side close.
    {data, replies} = answer_outstanding(data, reason)
    {:down, data, [{:reply, from, {:error, reason, %{bytes_sent: bytes, bytes_received: 0}}} | replies]}
  end

  defp received(<<id::32-signed, _::binary>> = frame, data) do
    case InFlight.pop(data.in_flight, id) do
      {:ok, %{from: nil}, in_flight} ->
        Logger.debug("Dropped a late reply with correlation id #{id} from broker #{peer(data)}")
        rearm(%{data | in_flight: in_flight}, [])

      {:ok, entry, in_flight} ->
        meta = %{bytes_sent: entry.bytes_sent, bytes_received: byte_size(frame)}
        rearm(%{data | in_flight: in_flight}, [cancel({:reply, id}), {:reply, entry.from, {:ok, frame, meta}}])

      :error ->
        close_client_side(:correlation_mismatch, data)
    end
  end

  defp received(_frame, data), do: close_client_side(:correlation_mismatch, data)

  defp rearm(data, actions) do
    case data.transport.activate(data.socket) do
      :ok -> {:keep_state, data, actions ++ [liveness_timer(data) | idle_when_empty(data)]}
      {:error, _reason} -> peer_closed(data, actions)
    end
  end

  defp liveness_timer(data) do
    case InFlight.earliest_live_until(data.in_flight) do
      nil -> cancel(:liveness)
      at -> {{:timeout, :liveness}, at, nil, [abs: true]}
    end
  end

  defp idle_without_demand(data), do: if(fetch?(data) and not demand?(data), do: [idle(data)], else: [])

  defp idle_when_empty(data), do: if(fetch?(data) and InFlight.empty?(data.in_flight), do: [idle(data)], else: [])

  defp idle(data) do
    jitter = data.opts.idle_jitter * (2 * :rand.uniform() - 1)
    {{:timeout, :idle}, round(data.opts.idle_timeout * (1 + jitter)), nil}
  end

  defp fetch?(%{role: :fetch}), do: true
  defp fetch?(_data), do: false

  defp reply_timed_out(id, data) do
    case List.keyfind(InFlight.entries(data.in_flight), id, 0) do
      {^id, %{from: from} = entry} when from != nil ->
        Logger.error("Receiving data from broker #{peer(data)} timed out")
        meta = %{bytes_sent: entry.bytes_sent, bytes_received: 0}

        {:keep_state, %{data | in_flight: InFlight.abandon(data.in_flight, id)},
         [{:reply, from, {:error, :timeout, meta}}]}

      _ ->
        :keep_state_and_data
    end
  end

  defp liveness(data) do
    now = now()

    if Enum.any?(InFlight.entries(data.in_flight), fn {_id, entry} -> entry.live_until <= now end),
      do: close_client_side(:liveness, data),
      else: {:keep_state_and_data, [liveness_timer(data)]}
  end

  defp peer_closed(data, actions \\ []), do: dropped(:closed, :remote_closed, data, actions)

  defp recv_error(reason, data), do: dropped(reason, :recv_error, data, [])

  defp dropped(reason, idle_reason, data, actions) do
    log_drop(InFlight.awaited?(data.in_flight), reason, idle_reason, data)
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
    waiters = for waiter <- :queue.to_list(data.waiting), do: {:reply, waiter.from, not_connected()}
    connectors = for from <- data.connectors, do: {:reply, from, {:error, :not_connected}}
    replies = for {:reply, _from, _reply} = reply <- replies, do: reply
    {:stop_and_reply, :normal, replies ++ waiters ++ connectors, %{data | waiting: :queue.new(), connectors: []}}
  end

  defp answer_outstanding(data, reason) do
    {entries, in_flight} = InFlight.drain(data.in_flight)

    replies =
      for {_id, %{from: from} = entry} <- entries, from != nil, do: {:reply, from, {:error, reason, meta(entry)}}

    cancels = for {id, _entry} <- entries, do: cancel({:reply, id})
    {%{data | in_flight: in_flight}, replies ++ cancels}
  end

  defp meta(entry), do: %{bytes_sent: entry.bytes_sent, bytes_received: 0}

  defp disconnect(data, actions) do
    data = %{data | socket: nil}
    timers = [cancel(:liveness), cancel(:stable)]
    {:next_state, :disconnected, data, actions ++ timers ++ try_connect_if_waiting(data)}
  end

  defp close_socket(%{socket: nil} = data, _reason), do: data

  defp close_socket(data, reason) do
    if reason, do: Telemetry.emit_connection_close(data.host, data.port, reason)
    data.transport.close(data.socket)
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

  defp enrich(%AuthConfig{mechanism_opts: opts} = auth, host, port),
    do: %{auth | mechanism_opts: Map.merge(opts || %{}, %{broker_host: host, broker_port: port})}

  defp expired?(dispatch), do: now() >= dispatch.deadline
  defp not_connected, do: {:error, :not_connected, @meta0}
  defp cancel(name), do: {{:timeout, name}, :cancel}
  defp key(data), do: {data.host, data.port}
  defp peer(data), do: "#{inspect(data.host)}:#{inspect(data.port)}"

  defp named(%{node_id: node_id} = data, %{node_id: node_id}), do: data
  defp named(data, %{node_id: node_id}), do: %{data | node_id: node_id}
  defp named(data, _dispatch), do: data

  defp describe_peer(%{node_id: nil} = data), do: "#{data.host}:#{data.port}"
  defp describe_peer(data), do: Broker.to_string(%Broker{node_id: data.node_id, host: data.host, port: data.port})
  defp now, do: System.monotonic_time(:millisecond)
end
