defmodule KafkaEx.Network.TransportTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import KafkaEx.TestHelpers, only: [forward_telemetry: 2]

  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Network.Socket
  alias KafkaEx.TestSupport.TLSFixtures

  @opts [:binary, {:packet, 4}, {:active, false}]

  setup do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, packet: 4, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listener)
    on_exit(fn -> :gen_tcp.close(listener) end)
    %{listener: listener, address: %{host: "127.0.0.1", port: port, role: :metadata}}
  end

  defp connect_pair(%{listener: listener, address: address}) do
    {:ok, socket} = NetworkClient.connect(address, @opts, false, 1_000)
    {:ok, peer} = :gen_tcp.accept(listener, 1_000)
    {socket, peer}
  end

  defp attach_here(events) do
    test_pid = self()
    forward_telemetry(events, fn event, _measurements, _metadata -> if self() == test_pid, do: {:telemetry, event} end)
  end

  test "connect/4 returns a passive plain socket framed by a 4-byte length", context do
    {%Socket{ssl: false, socket: raw}, _peer} = connect_pair(context)

    assert :inet.getopts(raw, [:active, :packet]) == {:ok, [active: false, packet: 4]}
  end

  test "connect/4 with use_ssl returns a TLS socket" do
    tls = TLSFixtures.chain()
    {:ok, port, stop} = TLSFixtures.start_acceptor(tls.server)
    on_exit(stop)

    assert {:ok, %Socket{ssl: true}} =
             NetworkClient.connect(%{host: "localhost", port: port, role: :data}, @opts ++ tls.client_ok, true, 5_000)
  end

  test "connect/4 to a closed port is refused", %{listener: listener, address: address} do
    :ok = :gen_tcp.close(listener)

    assert NetworkClient.connect(address, @opts, false, 1_000) == {:error, :econnrefused}
  end

  test "write/2 and read/2 round-trip a frame", context do
    {socket, peer} = connect_pair(context)

    assert NetworkClient.write(socket, "ping") == :ok
    assert {:ok, "ping"} = :gen_tcp.recv(peer, 0, 1_000)
    :ok = :gen_tcp.send(peer, "pong")
    assert NetworkClient.read(socket, 1_000) == {:ok, "pong"}
  end

  test "read/2 from a silent peer times out", context do
    {socket, _peer} = connect_pair(context)
    started = System.monotonic_time(:millisecond)

    assert NetworkClient.read(socket, 100) == {:error, :timeout}
    assert System.monotonic_time(:millisecond) - started >= 100
  end

  test "activate/1 delivers exactly one frame", context do
    {%Socket{socket: raw} = socket, peer} = connect_pair(context)
    :ok = :gen_tcp.send(peer, "one")
    :ok = :gen_tcp.send(peer, "two")

    assert NetworkClient.activate(socket) == :ok
    assert_receive {:tcp, ^raw, "one"}, 1_000
    refute_receive {:tcp, ^raw, _}, 100
  end

  test "close/1 closes the connection for the peer", context do
    {socket, peer} = connect_pair(context)

    assert NetworkClient.close(socket) == :ok
    assert :gen_tcp.recv(peer, 0, 1_000) == {:error, :closed}
  end

  test "connect/4 emits no telemetry and logs nothing", %{listener: listener, address: address} do
    attach_here([
      [:kafka_ex, :connection, :start],
      [:kafka_ex, :connection, :stop],
      [:kafka_ex, :connection, :exception],
      [:kafka_ex, :connection, :close]
    ])

    log =
      capture_log(fn ->
        {:ok, _socket} = NetworkClient.connect(address, @opts, false, 1_000)
        :ok = :gen_tcp.close(listener)
        {:error, :econnrefused} = NetworkClient.connect(address, @opts, false, 1_000)
      end)

    assert log == ""
    refute_receive {:telemetry, _}, 100
  end
end
