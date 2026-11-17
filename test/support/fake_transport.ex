defmodule KafkaEx.TestSupport.FakeTransport do
  @moduledoc """
  Loopback fake broker behind `KafkaEx.Network.Behaviour`, selected with the internal `:transport` client option.

  Every connection is a real socket pair: the connecting process owns the client side, a peer
  process owns the server side and answers like a broker. Start a fake per test and hand it to
  the client inside `ssl_options`, the one option the client forwards to `create_socket/5`:

      fake = start_supervised!({FakeTransport, brokers: [{1, "a", 9092}]})
      opts = [uris: [{"a", 9092}], transport: FakeTransport, ssl_options: FakeTransport.ssl_options(fake)]
      {:ok, client} = KafkaEx.Client.start_link(opts, :no_name)

  Without a fake in `ssl_options` the connecting process starts its own, which answers
  ApiVersions and a Metadata listing the connected address as node 1.

  Contract:

    * Responses are scripted by API with `respond/3`, never by order or count. A fields map is
      encoded for the request's API version and carries its correlation id; `{:frame, binary}`
      is written verbatim.
    * ApiVersions and Metadata are answered automatically on every connection unless scripted.
      Any other unscripted API closes its connection.
    * `hold/2` keeps requests of an API until `release/2`; `respond/3` with `:no_reply` never
      answers; `respond/3` with `:close` and `close/2` close from the broker side.
    * Sends on a missing or closed socket behave as in `KafkaEx.Network.NetworkClient`.
    * `requests/1` is the request log and `subscribe/1` streams it; `connects/1` records the
      options of every connect attempt.
    * tcp and ssl message families from real sockets; `use_ssl` needs the `:tls` server options.
    * Messages are tagged with the socket they came from; `inject/2` replays one for a stale socket.
    * At most one message per `active: :once` re-arm.
    * Several brokers (`:brokers`); any other address is refused like an unreachable broker.
    * No global state: scripts and the log live in the fake process. Reply functions run inside
      it and must not call it.

  `send_sync_request/3` and `send_async_request/2` follow today's seam, which carries the socket
  inside the broker map; that part of the contract is not final.
  """
  @behaviour KafkaEx.Network.Behaviour

  require Logger

  alias KafkaEx.Auth.SASL.VersionSupport
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Network.Socket
  alias KafkaEx.Telemetry
  alias KafkaEx.TestSupport.FakeTransport.Controller
  alias KafkaEx.TestSupport.FakeTransport.Wire

  @connect_timeout 5_000

  def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :temporary}
  def start_link(opts \\ []), do: Controller.start_link(opts)

  def ssl_options(fake, extra \\ []), do: [{:fake_transport, fake} | extra]

  def respond(fake, api, reply), do: GenServer.call(fake, {:respond, Wire.api_key(api), reply})
  def hold(fake, api), do: GenServer.call(fake, {:hold, Wire.api_key(api)})
  def release(fake, api), do: GenServer.call(fake, {:release, Wire.api_key(api)})
  def close(fake, {host, port}), do: GenServer.call(fake, {:close_address, {host, port}})

  def requests(fake), do: GenServer.call(fake, :requests)
  def subscribe(fake), do: GenServer.call(fake, {:subscribe, self()})
  def connects(fake), do: GenServer.call(fake, :connects)
  def connections(fake), do: GenServer.call(fake, :connections)

  def inject(%{socket: %Socket{ssl: ssl, socket: raw}, owner: owner}, event) do
    send(owner, message(ssl, raw, event))
    :ok
  end

  defp message(false, raw, :closed), do: {:tcp_closed, raw}
  defp message(true, raw, :closed), do: {:ssl_closed, raw}
  defp message(false, raw, {:data, data}), do: {:tcp, raw, data}
  defp message(true, raw, {:data, data}), do: {:ssl, raw, data}

  @impl true
  def create_socket(host, port, ssl_options \\ [], use_ssl \\ false, auth \\ nil) do
    {fake, ssl_options} = controller(ssl_options)
    record = %{host: host, port: port, ssl_options: ssl_options, use_ssl: use_ssl, auth: auth, owner: self()}
    {socket, _metadata} = connect(fake, record)
    socket
  end

  @impl true
  def close_socket(nil), do: :ok

  def close_socket(socket) do
    socket = wrap(socket)
    ensure_owner!(socket)
    forget(socket)
    Socket.close(socket)
  end

  @impl true
  def close_socket(_broker, nil, _reason), do: :ok

  def close_socket(%{host: host, port: port}, socket, reason) do
    socket = wrap(socket)
    ensure_owner!(socket)
    Telemetry.emit_connection_close(host, port, reason)
    forget(socket)
    Socket.close(socket)
    :ok
  end

  @impl true
  def send_async_request(broker, data), do: NetworkClient.send_async_request(broker, data)

  @impl true
  def send_sync_request(broker, data, timeout), do: NetworkClient.send_sync_request(broker, data, timeout)

  @impl true
  def format_host(host), do: String.to_charlist(host)

  defp controller(ssl_options) do
    case List.keytake(ssl_options, :fake_transport, 0) do
      {{:fake_transport, fake}, rest} -> {fake, rest}
      nil -> {own_controller(), ssl_options}
    end
  end

  defp own_controller do
    case Process.get({__MODULE__, :controller}) do
      pid when is_pid(pid) -> if Process.alive?(pid), do: pid, else: start_own_controller()
      nil -> start_own_controller()
    end
  end

  defp start_own_controller do
    {:ok, pid} = Controller.start([])
    Process.put({__MODULE__, :controller}, pid)
    pid
  end

  defp connect(fake, %{host: host, port: port} = record) do
    case GenServer.call(fake, {:connect, record}) do
      {:ok, id, listen_port} -> open(fake, id, listen_port, record)
      {:error, :tls_not_configured} -> raise ArgumentError, "FakeTransport needs the :tls server options for use_ssl"
      {:error, reason} -> refuse(host, port, reason)
    end
  end

  defp open(fake, id, listen_port, %{host: host, port: port} = record) do
    case dial(listen_port, record.ssl_options, record.use_ssl) do
      {:ok, socket} ->
        :ok = GenServer.call(fake, {:attach, id, socket})
        Process.put({__MODULE__, socket.socket}, %{fake: fake, id: id})
        finish(socket, record.auth, host, port)

      {:error, reason} ->
        refuse(host, port, reason)
    end
  end

  defp dial(listen_port, ssl_options, use_ssl) do
    opts = [:binary, packet: 4, active: false]

    with {:ok, tcp} <- :gen_tcp.connect({127, 0, 0, 1}, listen_port, opts, @connect_timeout) do
      upgrade(tcp, opts ++ ssl_options, use_ssl)
    end
  end

  defp upgrade(tcp, _opts, false), do: {:ok, %Socket{socket: tcp, ssl: false}}

  defp upgrade(tcp, opts, true) do
    case :ssl.connect(tcp, opts, @connect_timeout) do
      {:ok, ssl} ->
        {:ok, %Socket{socket: ssl, ssl: true}}

      {:error, _reason} = error ->
        :gen_tcp.close(tcp)
        error
    end
  end

  defp refuse(host, port, reason) do
    Logger.error("Could not connect to #{inspect_broker(host, port)} because of error #{inspect(reason)}")
    {nil, %{success: false, error: reason}}
  end

  defp finish(socket, auth, host, port) do
    case authenticate(socket, auth) do
      :ok ->
        :ok = Socket.setopts(socket, [:binary, {:packet, 4}, {:active, true}])
        {socket, %{success: true}}

      {:error, reason} ->
        close_socket(%{host: host, port: port}, socket, :auth_error)
        Logger.error("SASL authentication failed for #{inspect_broker(host, port)}}: #{inspect(reason)}")
        {nil, %{success: false, error: reason}}
    end
  end

  defp authenticate(_socket, nil), do: :ok
  defp authenticate(socket, auth), do: VersionSupport.validate_config(auth, socket)

  defp wrap(%Socket{} = socket), do: socket
  defp wrap(raw) when is_port(raw), do: %Socket{socket: raw, ssl: false}
  defp wrap(raw), do: %Socket{socket: raw, ssl: true}

  defp ensure_owner!(%Socket{socket: raw} = socket) do
    cond do
      Process.get({__MODULE__, raw}) -> :ok
      not Socket.open?(socket) -> :ok
      owner(socket) == self() -> :ok
      true -> raise ArgumentError, "FakeTransport socket closed from a non-owner process"
    end
  end

  defp owner(%Socket{ssl: false, socket: raw}) do
    case :erlang.port_info(raw, :connected) do
      {:connected, pid} -> pid
      _ -> nil
    end
  end

  defp owner(%Socket{ssl: true}), do: nil

  defp forget(%Socket{socket: raw}), do: Process.delete({__MODULE__, raw})

  defp inspect_broker(host, port), do: "broker #{inspect(host)}:#{inspect(port)}"
end
