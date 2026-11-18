defmodule KafkaEx.TestSupport.FakeTransport do
  @moduledoc """
  Loopback fake broker behind `KafkaEx.Network.Behaviour` and the low-level `KafkaEx.Network.Transport`,
  selected with the internal `:transport` client option.

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
    * `respond/3` with `:stall` answers nothing either and stops reading the socket for good, so
      the client's sends block once the socket buffers fill.
    * A request at a version outside the range its node advertised in ApiVersions closes its
      connection (`:unsupported_version`), as Kafka does. ApiVersions itself and APIs the node
      does not advertise are exempt.
    * A Produce with `acks = 0` gets no reply, whatever its script or hold, and its connection reads on (`:acks0`).
    * A client closing a connection that waits on a held or unanswered request turns it
      `open?: false` in `connections/1` and sends `{:fake_transport, :closed, id}` to subscribers;
      from then on `release/2` runs no reply function for it. `connections/1` then also has `close_reason`. A `release/2` issued before that
      notice may still run it, so wait for the notice first.
    * `respond/3` with `:close` and `close/2` close from the broker side. `close/2` returns once
      the peers have closed and every active tcp client socket has its `tcp_closed` in the
      owner's mailbox.
    * `requests/1` is the request log and `subscribe/1` streams it with every closed connection;
      `connects/1` records the options of every connect attempt, with the `:role` of a
      low-level `connect/4` (`nil` on `create_socket/5`).
    * `:api_versions` sets the ApiVersions reply; `:api_versions_by_node` overrides it per API for
      one node (`%{node_id => %{api => range}}`).
    * Several brokers (`:brokers`); any other address is refused like an unreachable broker.
    * Scripted SASL success and failure. SCRAM supports failure only: the default
      SaslAuthenticate reply cannot complete its exchange. The handshake uses passive `recv`
      before the socket turns active. `oauthbearer_refusal/1` is a SaslAuthenticate reply
      function with Kafka's answer to a refused OAUTHBEARER token.
    * tcp and ssl message families from real sockets; `use_ssl` needs the `:tls` server options.
      After `silence_tls/1`, peers accepted for `use_ssl` never complete the TLS handshake.
    * A stalled or TLS-silenced peer no longer watches its socket, so a client close leaves it
      `open?: true` without a `:closed` notice; `close/2` still closes it.
    * Messages are tagged with the socket they came from; `inject/2` replays one for a stale socket.
    * At most one message per `active: :once` re-arm.
    * `hang_send/2` blocks the sender until `release/2`, ignoring the send's timeout;
      `fail_send/4` writes less than a whole frame and fails like `KafkaEx.Network.NetworkClient`.
      `release/2` disarms both. Both apply only to sends from the process that created the
      socket; a send from any other process is never faulted.
    * Sends on a missing or closed socket behave as in `KafkaEx.Network.NetworkClient`.
    * The low-level seam (`connect/4`, `write/2`, `read/2`, `activate/1`, `close/1`) behaves like
      `NetworkClient`'s: it takes the caller's socket options and timeout and emits no telemetry,
      no log line and no SASL. The fake rides in the socket options (`ssl_options/2`). On
      `write/2`, `hang_send/2` and `fail_send/4` act as above, but a failed write only closes the
      socket and returns the reason.
    * No global state: scripts and the log live in the fake process. Reply functions run inside
      it and must not call it.

  `send_sync_request/3` and `send_async_request/2` follow today's seam, which carries the socket
  inside the broker map; that part of the contract is not final.
  """
  @behaviour KafkaEx.Network.Behaviour
  @behaviour KafkaEx.Network.Transport

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
  def silence_tls(fake), do: GenServer.call(fake, :silence_tls)

  # Measured on cp-kafka 7.0: an error challenge with error code 0, then 58 to the client's %x01.
  def oauthbearer_refusal(%{frame: frame}) do
    <<_header::binary-size(8), id_size::16, _id::binary-size(id_size), size::32, bytes::binary-size(size)>> = frame
    refusal = ~s({"status":"invalid_token"})

    if bytes == <<1>>,
      do: %{error_code: 58, error_message: refusal, auth_bytes: ""},
      else: %{error_code: 0, error_message: "", auth_bytes: refusal}
  end

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

  @impl KafkaEx.Network.Transport
  def connect(%{host: host, port: port, role: role}, socket_options, use_ssl, timeout) do
    {fake, socket_options} = controller(socket_options)

    record = %{
      host: host,
      port: port,
      ssl_options: socket_options,
      use_ssl: use_ssl,
      auth: nil,
      owner: self(),
      role: role
    }

    case request_connect(fake, record) do
      {:error, :tls_not_configured} ->
        raise ArgumentError, "FakeTransport needs the :tls server options for use_ssl"

      {:error, reason} ->
        {:error, reason}

      {:ok, id, listen_port} ->
        with {:ok, socket} <- Socket.create({127, 0, 0, 1}, listen_port, socket_options, use_ssl, timeout) do
          :ok = GenServer.call(fake, {:attach, id, socket})
          Process.put({__MODULE__, socket.socket}, fake)
          {:ok, socket}
        end
    end
  end

  @impl KafkaEx.Network.Transport
  def write(%Socket{} = socket, data) do
    case send_fault(socket, data) do
      :none -> Socket.send(socket, data)
      {:fail, {bytes_sent, reason}} -> fail_write(socket, data, bytes_sent, reason)
    end
  end

  @impl KafkaEx.Network.Transport
  def read(socket, timeout), do: Socket.recv(socket, 0, timeout)

  @impl KafkaEx.Network.Transport
  def activate(socket), do: Socket.setopts(socket, [:binary, {:packet, 4}, {:active, :once}])

  @impl KafkaEx.Network.Transport
  def close(socket) do
    close_owned(socket)
    :ok
  end

  @impl true
  def create_socket(host, port, ssl_options \\ [], use_ssl \\ false, auth \\ nil) do
    {fake, ssl_options} = controller(ssl_options)

    record = %{
      host: host,
      port: port,
      ssl_options: ssl_options,
      use_ssl: use_ssl,
      auth: auth,
      owner: self(),
      role: nil
    }

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

  def close_socket(socket), do: socket |> owned!() |> close_owned()

  @impl true
  def close_socket(_broker, nil, _reason), do: :ok

  def close_socket(%{host: host, port: port}, socket, reason) do
    socket = owned!(socket)
    Telemetry.emit_connection_close(host, port, reason)
    close_owned(socket)
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
    pid = Process.get({__MODULE__, :controller})
    if pid && Process.alive?(pid), do: pid, else: start_own_controller()
  end

  defp start_own_controller do
    {:ok, pid} = Controller.start([])
    Process.put({__MODULE__, :controller}, pid)
    pid
  end

  defp request_connect(fake, record) do
    GenServer.call(fake, {:connect, record})
  catch
    :exit, {reason, _call} when reason in [:noproc, :normal, :shutdown] -> {:error, :econnrefused}
  end

  defp connect(_fake, {:error, reason}, %{host: host, port: port}), do: refuse(host, port, reason)

  defp connect(fake, {:ok, id, listen_port}, %{host: host, port: port} = record) do
    case dial(listen_port, record.ssl_options, record.use_ssl) do
      {:ok, socket} ->
        :ok = GenServer.call(fake, {:attach, id, socket})
        Process.put({__MODULE__, socket.socket}, fake)
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

  defp send_fault(%Socket{socket: raw}, data) do
    case Process.get({__MODULE__, raw}) do
      nil -> :none
      fake -> ask_send_fault(fake, data)
    end
  end

  defp send_fault(%{socket: %Socket{} = socket}, data), do: send_fault(socket, data)
  defp send_fault(_broker, _data), do: :none

  defp ask_send_fault(fake, data) do
    <<api_key::16-signed, _::binary>> = IO.iodata_to_binary(data)
    GenServer.call(fake, {:send_fault, api_key}, :infinity)
  catch
    :exit, _reason -> :none
  end

  defp fail(%{host: host, port: port, socket: socket} = broker, data, bytes_sent, reason, verb) do
    write_partial(socket, data, bytes_sent)
    Logger.error("#{verb} #{inspect_broker(host, port)} failed with #{inspect(reason)}")
    close_socket(broker, socket, :send_error)
    {:error, reason}
  end

  defp fail_write(socket, data, bytes_sent, reason) do
    write_partial(socket, data, bytes_sent)
    close_owned(socket)
    {:error, reason}
  end

  defp write_partial(socket, data, bytes_sent) do
    frame = IO.iodata_to_binary(data)

    with :ok <- Socket.setopts(socket, packet: :raw) do
      Socket.send(socket, [<<byte_size(frame)::32>>, binary_part(frame, 0, min(bytes_sent, byte_size(frame) - 1))])
    end
  end

  defp wrap(%Socket{} = socket), do: socket
  defp wrap(raw) when is_port(raw), do: %Socket{socket: raw, ssl: false}
  defp wrap(raw), do: %Socket{socket: raw, ssl: true}

  defp owned!(socket) do
    %Socket{socket: raw} = socket = wrap(socket)

    cond do
      Process.get({__MODULE__, raw}) -> socket
      not Socket.open?(socket) -> socket
      owner(socket) == self() -> socket
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

  defp close_owned(%Socket{socket: raw} = socket) do
    Process.delete({__MODULE__, raw})
    Socket.close(socket)
  end

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
