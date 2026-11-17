defmodule KafkaEx.TestSupport.FakeTransport.Controller do
  @moduledoc false
  use GenServer

  alias KafkaEx.TestSupport.FakeTransport.Peer
  alias KafkaEx.TestSupport.FakeTransport.Wire

  @default_versions %{18 => 0..0, 3 => 0..0}

  def start_link(opts), do: GenServer.start_link(__MODULE__, {self(), opts})
  def start(opts), do: GenServer.start(__MODULE__, {self(), opts})

  @impl true
  def init({owner, opts}) do
    Process.monitor(owner)
    versions = Map.new(Keyword.get(opts, :api_versions, %{}), fn {api, range} -> {Wire.api_key(api), range} end)

    {:ok,
     %{
       brokers: Keyword.get(opts, :brokers),
       topics: Keyword.get(opts, :topics, %{}),
       versions: Map.merge(@default_versions, versions),
       log: [],
       connects: [],
       connections: %{},
       subscribers: [],
       next_id: 1
     }}
  end

  @impl true
  def handle_call({:connect, record}, _from, state) do
    state = %{state | connects: [record | state.connects]}

    if is_nil(node_id(state, record)),
      do: {:reply, {:error, :econnrefused}, state},
      else: listen(record, state)
  end

  def handle_call({:attach, id, socket}, _from, state),
    do: {:reply, :ok, update_in(state.connections[id], &%{&1 | socket: socket})}

  def handle_call({:request, id, frame}, _from, state) do
    connection = state.connections[id]

    request =
      frame
      |> Wire.parse_header()
      |> Map.merge(%{conn: id, host: connection.host, port: connection.port, node_id: connection.node_id, frame: frame})

    action = reply_for(request, state)
    entry = Map.put(request, :action, action_name(action))
    Enum.each(state.subscribers, &send(&1, {:fake_transport, :request, entry}))
    {:reply, action, %{state | log: [entry | state.log]}}
  end

  def handle_call(:requests, _from, state), do: {:reply, Enum.reverse(state.log), state}
  def handle_call(:connects, _from, state), do: {:reply, Enum.reverse(state.connects), state}

  def handle_call(:connections, _from, state) do
    connections = state.connections |> Map.values() |> Enum.sort_by(& &1.id) |> Enum.map(&Map.delete(&1, :peer))
    {:reply, connections, state}
  end

  def handle_call({:subscribe, pid}, _from, state), do: {:reply, :ok, %{state | subscribers: [pid | state.subscribers]}}

  @impl true
  def handle_info({:peer_closed, id}, state), do: {:noreply, update_in(state.connections[id], &%{&1 | open?: false})}
  def handle_info({:DOWN, _ref, :process, _owner, _reason}, state), do: {:stop, :normal, state}

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.connections, fn {_id, %{peer: peer}} -> Process.exit(peer, :shutdown) end)
  end

  defp listen(record, state) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, ip: {127, 0, 0, 1}])
    {:ok, listen_port} = :inet.port(listener)
    id = state.next_id
    {:ok, peer} = Peer.start_link(%{listener: listener, conn_id: id, controller: self(), tls: nil})
    :ok = :gen_tcp.controlling_process(listener, peer)
    send(peer, :accept)

    connection = %{
      id: id,
      host: record.host,
      port: record.port,
      node_id: node_id(state, record),
      owner: record.owner,
      socket: nil,
      peer: peer,
      open?: true
    }

    {:reply, {:ok, id, listen_port},
     %{state | next_id: id + 1, connections: Map.put(state.connections, id, connection)}}
  end

  defp node_id(%{brokers: nil}, _record), do: 1

  defp node_id(%{brokers: brokers}, %{host: host, port: port}) do
    Enum.find_value(brokers, fn
      {node_id, ^host, ^port} -> node_id
      _ -> nil
    end)
  end

  defp reply_for(request, state) do
    case default_rule(request, state) do
      nil -> :unscripted
      rule -> normalize(rule, request)
    end
  end

  defp default_rule(%{api_key: 18}, state), do: Wire.api_versions_fields(state.versions)

  defp default_rule(%{api_key: 3} = request, state),
    do: Wire.metadata_fields(state.brokers || [{1, request.host, request.port}], state.topics)

  defp default_rule(_request, _state), do: nil

  defp normalize(%{} = fields, request),
    do: {:reply, Wire.response(request.api_key, request.api_version, request.correlation_id, fields)}

  defp action_name({:reply, _iodata}), do: :reply
  defp action_name(action), do: action
end
