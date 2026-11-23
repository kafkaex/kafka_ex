defmodule KafkaEx.TestSupport.FakeTransport.Controller do
  @moduledoc false
  use GenServer

  require Logger

  alias KafkaEx.TestSupport.FakeTransport.Peer
  alias KafkaEx.TestSupport.FakeTransport.Wire

  @default_versions %{api_versions: 0..0, metadata: 0..0, sasl_handshake: 0..1, sasl_authenticate: 0..1}
  @mechanisms ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "AWS_MSK_IAM"]

  def start_link(opts), do: GenServer.start_link(__MODULE__, {self(), opts})
  def start(opts), do: GenServer.start(__MODULE__, {self(), opts})

  @impl true
  def init({owner, opts}) do
    Process.monitor(owner)
    versions = Map.merge(by_api_key(@default_versions), by_api_key(Keyword.get(opts, :api_versions, %{})))

    versions_by_node =
      Map.new(Keyword.get(opts, :api_versions_by_node, %{}), fn {node_id, node_versions} ->
        {node_id, Map.merge(versions, by_api_key(node_versions))}
      end)

    {:ok,
     %{
       brokers: Keyword.get(opts, :brokers),
       topics: Keyword.get(opts, :topics, %{}),
       versions: versions,
       versions_by_node: versions_by_node,
       tls: Keyword.get(opts, :tls),
       tls_silent?: false,
       rules: %{},
       holds: MapSet.new(),
       held: [],
       hangs: MapSet.new(),
       hung: [],
       send_failures: %{},
       log: [],
       connects: [],
       connections: %{},
       subscribers: [],
       next_id: 1,
       connect_delay: 0
     }}
  end

  @impl true
  def handle_call({:connect, record}, from, %{connect_delay: delay} = state) when delay > 0 do
    {:reply, reply, state} = handle_call({:connect, record}, from, %{state | connect_delay: 0})
    Process.send_after(self(), {:reply_later, from, reply}, delay)
    {:noreply, %{state | connect_delay: delay}}
  end

  def handle_call({:connect, record}, _from, state) do
    state = %{state | connects: [record | state.connects]}
    node_id = node_id(state, record)

    cond do
      record.use_ssl and is_nil(state.tls) -> {:reply, {:error, :tls_not_configured}, state}
      is_nil(node_id) -> {:reply, {:error, :econnrefused}, state}
      true -> listen(record, node_id, state)
    end
  end

  def handle_call({:delay_connects, ms}, _from, state), do: {:reply, :ok, %{state | connect_delay: ms}}

  def handle_call({:attach, id, socket}, _from, state),
    do: {:reply, :ok, update_in(state.connections[id], &%{&1 | socket: socket})}

  def handle_call({:request, _id, frame}, _from, state) when byte_size(frame) < 8 do
    Logger.error("FakeTransport could not parse a #{byte_size(frame)}-byte request")
    {:reply, :close, state}
  end

  def handle_call({:request, id, frame}, _from, state) do
    connection = state.connections[id]

    request =
      frame
      |> Wire.parse_header()
      |> Map.merge(%{conn: id, host: connection.host, port: connection.port, node_id: connection.node_id, frame: frame})

    action = decide(request, state)
    entry = Map.put(request, :action, action_name(action))
    notify(state, {:fake_transport, :request, entry})
    state = %{state | log: [entry | state.log]}
    state = if action == :hold, do: %{state | held: [{request, connection.peer} | state.held]}, else: state
    {:reply, action, state}
  end

  def handle_call({:respond, api_key, reply}, _from, state),
    do: {:reply, :ok, %{state | rules: Map.put(state.rules, api_key, reply)}}

  def handle_call(:silence_tls, _from, state), do: {:reply, :ok, %{state | tls_silent?: true}}

  def handle_call({:hold, api_key}, _from, state), do: {:reply, :ok, %{state | holds: MapSet.put(state.holds, api_key)}}

  def handle_call({:hang_send, api_key}, _from, state),
    do: {:reply, :ok, %{state | hangs: MapSet.put(state.hangs, api_key)}}

  def handle_call({:fail_send, api_key, bytes_sent, reason}, _from, state),
    do: {:reply, :ok, %{state | send_failures: Map.put(state.send_failures, api_key, {bytes_sent, reason})}}

  def handle_call({:release, api_key}, _from, state) do
    {released, held} = Enum.split_with(state.held, fn {request, _peer} -> request.api_key == api_key end)
    {unhung, hung} = Enum.split_with(state.hung, fn {key, _from} -> key == api_key end)

    state = %{
      state
      | holds: MapSet.delete(state.holds, api_key),
        hangs: MapSet.delete(state.hangs, api_key),
        send_failures: Map.delete(state.send_failures, api_key),
        held: held,
        hung: hung
    }

    Enum.each(Enum.reverse(released), fn {request, peer} -> send(peer, {:action, reply_for(request, state)}) end)
    Enum.each(Enum.reverse(unhung), fn {_key, from} -> GenServer.reply(from, :none) end)
    {:reply, :ok, state}
  end

  def handle_call({:send_fault, api_key}, from, state) do
    cond do
      MapSet.member?(state.hangs, api_key) -> {:noreply, %{state | hung: [{api_key, from} | state.hung]}}
      Map.has_key?(state.send_failures, api_key) -> {:reply, {:fail, state.send_failures[api_key]}, state}
      true -> {:reply, :none, state}
    end
  end

  def handle_call({:close_address, {host, port}}, _from, state) do
    closing = for {_id, %{host: ^host, port: ^port, open?: true} = connection} <- state.connections, do: connection
    Enum.each(closing, &send(&1.peer, :close))
    closed = Map.new(closing, &{&1.id, %{&1 | open?: false}})
    state = forget_held(%{state | connections: Map.merge(state.connections, closed)}, Map.keys(closed))
    {:reply, closing, state}
  end

  def handle_call(:requests, _from, state), do: {:reply, Enum.reverse(state.log), state}
  def handle_call(:connects, _from, state), do: {:reply, Enum.reverse(state.connects), state}

  def handle_call(:connections, _from, state) do
    connections = state.connections |> Map.values() |> Enum.sort_by(& &1.id) |> Enum.map(&Map.delete(&1, :peer))
    {:reply, connections, state}
  end

  def handle_call({:subscribe, pid}, _from, state), do: {:reply, :ok, %{state | subscribers: [pid | state.subscribers]}}

  @impl true
  def handle_info({:peer_closed, id, reason}, state) do
    notify(state, {:fake_transport, :closed, id})
    connections = Map.update!(state.connections, id, &%{&1 | open?: false, close_reason: reason})
    {:noreply, forget_held(%{state | connections: connections}, [id])}
  end

  def handle_info({:reply_later, from, reply}, state) do
    GenServer.reply(from, reply)
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, _owner, _reason}, state), do: {:stop, :normal, state}
  def handle_info(_stray, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.connections, fn {_id, %{peer: peer}} ->
      # Without the unlink the peer's exit comes back over the link and replaces the real stop reason.
      Process.unlink(peer)
      Process.exit(peer, :shutdown)
    end)
  end

  defp listen(record, node_id, state) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, ip: {127, 0, 0, 1}])
    {:ok, listen_port} = :inet.port(listener)
    id = state.next_id
    tls = if record.use_ssl, do: peer_tls(state)
    {:ok, peer} = Peer.start_link(%{listener: listener, conn_id: id, controller: self(), tls: tls})
    :ok = :gen_tcp.controlling_process(listener, peer)
    send(peer, :accept)

    connection = %{
      id: id,
      host: record.host,
      port: record.port,
      node_id: node_id,
      owner: record.owner,
      socket: nil,
      peer: peer,
      open?: true,
      close_reason: nil
    }

    {:reply, {:ok, id, listen_port},
     %{state | next_id: id + 1, connections: Map.put(state.connections, id, connection)}}
  end

  defp peer_tls(%{tls_silent?: true}), do: :silent
  defp peer_tls(state), do: state.tls

  defp by_api_key(versions), do: Map.new(versions, fn {api, range} -> {Wire.api_key(api), range} end)

  defp notify(state, message), do: Enum.each(state.subscribers, &send(&1, message))

  defp forget_held(state, ids),
    do: %{state | held: Enum.reject(state.held, fn {request, _peer} -> request.conn in ids end)}

  defp node_id(%{brokers: nil}, _record), do: 1

  defp node_id(%{brokers: brokers}, %{host: host, port: port}) do
    Enum.find_value(brokers, fn
      {node_id, ^host, ^port} -> node_id
      _ -> nil
    end)
  end

  defp decide(request, state) do
    cond do
      unsupported_version?(request, state) -> :unsupported_version
      Wire.acks0?(request.frame) -> :acks0
      MapSet.member?(state.holds, request.api_key) -> :hold
      true -> reply_for(request, state)
    end
  end

  defp unsupported_version?(%{api: :api_versions}, _state), do: false

  defp unsupported_version?(request, state) do
    case Map.get(node_versions(state, request.node_id), request.api_key) do
      nil -> false
      range -> request.api_version not in range
    end
  end

  defp reply_for(request, state) do
    case Map.get(state.rules, request.api_key) || default_rule(request, state) do
      nil -> :unscripted
      rule -> render(rule, request)
    end
  end

  defp render(rule, request) do
    normalize(rule, request)
  catch
    kind, reason ->
      Logger.error("FakeTransport could not answer #{inspect(request.api)}: #{Exception.format_banner(kind, reason)}")
      :close
  end

  defp default_rule(%{api: :api_versions} = request, state) do
    Wire.api_versions_fields(node_versions(state, request.node_id))
  end

  defp default_rule(%{api: :metadata} = request, state),
    do: Wire.metadata_fields(state.brokers || [{1, request.host, request.port}], state.topics)

  defp default_rule(%{api: :sasl_handshake}, _state), do: %{error_code: 0, mechanisms: @mechanisms}
  defp default_rule(%{api: :sasl_authenticate}, _state), do: %{error_code: 0, error_message: nil, auth_bytes: ""}

  defp default_rule(_request, _state), do: nil

  defp node_versions(state, node_id), do: Map.get(state.versions_by_node, node_id, state.versions)

  defp normalize(fun, request) when is_function(fun, 1), do: normalize(fun.(request), request)
  defp normalize(action, _request) when action in [:no_reply, :close, :stall], do: action

  defp normalize({:after, ms, rule}, request) when is_integer(ms) and ms > 0 do
    case normalize(rule, request) do
      {:reply, iodata} -> {:reply_after, ms, iodata}
      action -> action
    end
  end

  defp normalize({:frame, frame}, _request), do: {:reply, IO.iodata_to_binary(frame)}

  defp normalize(%{} = fields, request),
    do: {:reply, Wire.response(request.api_key, request.api_version, request.correlation_id, fields)}

  defp normalize(other, _request), do: raise(ArgumentError, "unsupported reply #{inspect(other)}")

  defp action_name({:reply, _iodata}), do: :reply
  defp action_name({:reply_after, _ms, _iodata}), do: :reply
  defp action_name(action), do: action
end
