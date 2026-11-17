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
      encoded for the request's API version and carries its correlation id; `{:frame, iodata}`
      is written verbatim.
    * ApiVersions, SaslHandshake, SaslAuthenticate and Metadata are answered automatically on
      every connection unless scripted. A request of any API with neither a script nor one of
      these defaults closes its connection.
    * A reply function that raises, returns an unsupported value or yields fields the encoder
      cannot encode is logged and closes its connection; the fake and other connections stay up.
    * Like Kafka, a connection answers its requests one at a time in arrival order. `hold/2` keeps
      requests of an API until `release/2`; a connection with a held request answers nothing
      more, and its later requests reach neither the log nor the scripts, until the release
      answers it under the scripts in force then.
    * `respond/3` with `:no_reply` never answers that request nor any later one on its connection.
    * A client closing a connection that waits on a held or unanswered request turns it
      `open?: false` in `connections/1`; from then on `release/2` runs no reply function for it.
    * `respond/3` with `:close` and `close/2` close from the broker side. `close/2` returns once
      the peers have closed and every active tcp client socket has its `tcp_closed` in the
      owner's mailbox.
    * `requests/1` is the request log and `subscribe/1` streams it; `connects/1` records the
      options of every connect attempt.
    * Several brokers (`:brokers`); any other address is refused like an unreachable broker.
    * Scripted SASL success and failure. SCRAM supports failure only: the default
      SaslAuthenticate reply cannot complete its exchange. The handshake uses passive `recv`
      before the socket turns active.
    * tcp and ssl message families from real sockets; `use_ssl` needs the `:tls` server options.
    * Messages are tagged with the socket they came from; `inject/2` replays one for a stale socket.
    * At most one message per `active: :once` re-arm.
    * `hang_send/2` blocks the sender until `release/2`, ignoring the send's timeout;
      `fail_send/4` writes less than a whole frame and fails like `KafkaEx.Network.NetworkClient`.
      `release/2` disarms both. Both apply only to sends from the process that created the
      socket; a send from any other process is never faulted.
    * Sends on a missing or closed socket behave as in `KafkaEx.Network.NetworkClient`.
    * No global state: scripts and the log live in the fake process. Reply functions run inside
      it and must not call it.

  `send_sync_request/3` and `send_async_request/2` follow today's seam, which carries the socket
  inside the broker map; that part of the contract is not final.
  """
  @behaviour KafkaEx.Network.Behaviour

  require Logger

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Auth.SASL
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
  def hang_send(fake, api), do: GenServer.call(fake, {:hang_send, Wire.api_key(api)})

  def fail_send(fake, api, bytes_sent, reason) when is_integer(bytes_sent) and bytes_sent >= 0,
    do: GenServer.call(fake, {:fail_send, Wire.api_key(api), bytes_sent, reason})

  def release(fake, api), do: GenServer.call(fake, {:release, Wire.api_key(api)})

  def close(fake, {host, port}) do
    fake
    |> GenServer.call({:close_address, {host, port}})
    |> Enum.flat_map(fn %{peer: peer, socket: socket} -> [Process.monitor(peer) | watch_client(socket)] end)
    |> Enum.each(&await_down/1)
  end

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

    case request_connect(fake, record) do
      {:error, :tls_not_configured} ->
        raise ArgumentError, "FakeTransport needs the :tls server options for use_ssl"

      attempt ->
        metadata = Telemetry.connection_metadata(host, port, use_ssl)
        Telemetry.span([:kafka_ex, :connection], metadata, fn -> connect(fake, attempt, record) end)
    end
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
  def send_async_request(broker, data) do
    case send_fault(broker, data) do
      :none -> NetworkClient.send_async_request(broker, data)
      {:fail, {bytes_sent, reason}} -> fail(broker, data, bytes_sent, reason, "Asynchronously sending data to")
    end
  end

  @impl true
  def send_sync_request(broker, data, timeout) do
    case send_fault(broker, data) do
      :none -> NetworkClient.send_sync_request(broker, data, timeout)
      {:fail, {bytes_sent, reason}} -> fail(broker, data, bytes_sent, reason, "Sending data to")
    end
  end

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

  defp request_connect(fake, record) do
    GenServer.call(fake, {:connect, record})
  catch
    :exit, _reason -> {:error, :econnrefused}
  end

  defp connect(fake, {:ok, id, listen_port}, record), do: open(fake, id, listen_port, record)
  defp connect(_fake, {:error, reason}, %{host: host, port: port}), do: refuse(host, port, reason)

  defp open(fake, id, listen_port, %{host: host, port: port} = record) do
    case dial(listen_port, record.ssl_options, record.use_ssl) do
      {:ok, socket} ->
        :ok = GenServer.call(fake, {:attach, id, socket})
        Process.put({__MODULE__, socket.socket}, %{fake: fake, id: id})
        finish(socket, enrich(record.auth, host, port), host, port)

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
    case authenticate(socket, auth, host, port) do
      :ok ->
        :ok = Socket.setopts(socket, [:binary, {:packet, 4}, {:active, true}])
        {socket, %{success: true}}

      {:error, reason} ->
        close_socket(%{host: host, port: port}, socket, :auth_error)
        Logger.error("SASL authentication failed for #{inspect_broker(host, port)}}: #{inspect(reason)}")
        {nil, %{success: false, error: reason}}
    end
  end

  defp authenticate(_socket, nil, _host, _port), do: :ok

  defp authenticate(socket, %AuthConfig{} = auth, host, port) do
    metadata = Telemetry.auth_metadata(host, port, mechanism_name(auth))

    Telemetry.span([:kafka_ex, :auth], metadata, fn ->
      result = with :ok <- VersionSupport.validate_config(auth, socket), do: SASL.authenticate(socket, auth)
      {result, %{}}
    end)
  end

  defp mechanism_name(%AuthConfig{mechanism: :plain}), do: "PLAIN"
  defp mechanism_name(%AuthConfig{mechanism: :scram, mechanism_opts: %{algo: :sha512}}), do: "SCRAM-SHA-512"
  defp mechanism_name(%AuthConfig{mechanism: :scram}), do: "SCRAM-SHA-256"
  defp mechanism_name(%AuthConfig{mechanism: :oauthbearer}), do: "OAUTHBEARER"
  defp mechanism_name(%AuthConfig{mechanism: mechanism}), do: to_string(mechanism)

  defp enrich(nil, _host, _port), do: nil

  defp enrich(%AuthConfig{mechanism_opts: opts} = auth, host, port),
    do: %{auth | mechanism_opts: Map.merge(opts || %{}, %{broker_host: host, broker_port: port})}

  defp send_fault(%{socket: %Socket{socket: raw}}, data) do
    case Process.get({__MODULE__, raw}) do
      %{fake: fake, id: id} ->
        <<api_key::16-signed, _::binary>> = IO.iodata_to_binary(data)

        try do
          GenServer.call(fake, {:send_fault, id, api_key}, :infinity)
        catch
          :exit, _reason -> :none
        end

      nil ->
        :none
    end
  end

  defp send_fault(_broker, _data), do: :none

  defp fail(%{host: host, port: port, socket: socket} = broker, data, bytes_sent, reason, verb) do
    frame = IO.iodata_to_binary(data)

    with :ok <- Socket.setopts(socket, packet: :raw) do
      Socket.send(socket, [<<byte_size(frame)::32>>, binary_part(frame, 0, min(bytes_sent, byte_size(frame) - 1))])
    end

    Logger.error("#{verb} #{inspect_broker(host, port)} failed with #{inspect(reason)}")
    close_socket(broker, socket, :send_error)
    {:error, reason}
  end

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

  # The ssl socket's port belongs to the TLS connection process, not to the caller.
  defp owner(%Socket{ssl: true}), do: nil

  defp forget(%Socket{socket: raw}), do: Process.delete({__MODULE__, raw})

  defp watch_client(%Socket{ssl: false, socket: raw}) do
    case :inet.getopts(raw, [:active]) do
      {:ok, [active: false]} -> []
      {:ok, _armed} -> [:erlang.monitor(:port, raw)]
      {:error, _closed} -> []
    end
  end

  defp watch_client(_socket), do: []

  defp await_down(ref) do
    receive do
      {:DOWN, ^ref, _kind, _object, _reason} -> :ok
    after
      @connect_timeout -> raise "FakeTransport.close/2 timed out waiting for a connection to close"
    end
  end

  defp inspect_broker(host, port), do: "broker #{inspect(host)}:#{inspect(port)}"
end
