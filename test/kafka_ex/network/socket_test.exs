defmodule KafkaEx.Network.Socket.Test do
  use ExUnit.Case, async: false
  import KafkaEx.TestHelpers

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ssl)
  end

  describe "without SSL socket" do
    setup do
      port = get_free_port(3040)
      KafkaEx.TestSupport.Server.start(port)
      {:ok, [port: port]}
    end

    test "create a non SSL socket", context do
      {:ok, socket} =
        KafkaEx.Network.Socket.create(
          ~c"localhost",
          context[:port],
          [:binary, {:packet, 0}],
          false
        )

      assert socket.ssl == false
      KafkaEx.Network.Socket.close(socket)
    end

    test "send and receive using a non SSL socket", context do
      {:ok, socket} =
        KafkaEx.Network.Socket.create(
          ~c"localhost",
          context[:port],
          [:binary, {:packet, 0}, {:active, false}],
          false
        )

      KafkaEx.Network.Socket.send(socket, ~c"ping")
      assert {:ok, "ping"} == KafkaEx.Network.Socket.recv(socket, 0)
      KafkaEx.Network.Socket.close(socket)
    end

    test "retrieve info from a non SSL socket", context do
      {:ok, socket} =
        KafkaEx.Network.Socket.create(
          ~c"localhost",
          context[:port],
          [:binary, {:packet, 0}, {:active, false}],
          false
        )

      info = KafkaEx.Network.Socket.info(socket)
      assert info[:name] == ~c"tcp_inet"
      KafkaEx.Network.Socket.close(socket)
      # OTP 27+ may return :einval instead of :closed for SSL sockets
      assert KafkaEx.Network.Socket.send(socket, ~c"ping") in [{:error, :closed}, {:error, :einval}]
    end
  end

  describe "with ssl socket" do
    setup do
      KafkaEx.TestSupport.SSLServer.start(3030)

      ssl_opts = [
        :binary,
        {:packet, 0},
        {:active, false},
        {:verify, :verify_peer},
        {:cacertfile, ~c"test/fixtures/client.pem"},
        {:certfile, ~c"test/fixtures/client.crt"},
        {:keyfile, ~c"test/fixtures/client.key"}
      ]

      {:ok, [ssl_port: 3030, ssl_opts: ssl_opts]}
    end

    test "create a SSL socket", context do
      {:ok, socket} =
        KafkaEx.Network.Socket.create(~c"localhost", context[:ssl_port], context[:ssl_opts], true)

      assert socket.ssl == true
      KafkaEx.Network.Socket.close(socket)
    end

    test "send and receive using a SSL socket", context do
      {:ok, socket} =
        KafkaEx.Network.Socket.create(~c"localhost", context[:ssl_port], context[:ssl_opts], true)

      KafkaEx.Network.Socket.send(socket, ~c"ping")
      assert {:ok, "ping"} == KafkaEx.Network.Socket.recv(socket, 0)
      KafkaEx.Network.Socket.close(socket)
    end

    test "retrieve info from a SSL socket", context do
      {:ok, socket} =
        KafkaEx.Network.Socket.create(~c"localhost", context[:ssl_port], context[:ssl_opts], true)

      info = KafkaEx.Network.Socket.info(socket)
      assert info[:name] == ~c"tcp_inet"
      KafkaEx.Network.Socket.close(socket)
      # OTP 27+ may return :einval instead of :closed for SSL sockets
      assert KafkaEx.Network.Socket.send(socket, ~c"ping") in [{:error, :closed}, {:error, :einval}]
    end
  end

  describe "connect timeout" do
    # 192.0.2.1 is RFC 5737 TEST-NET-1 — guaranteed unrouted, so the SYN is
    # black-holed and connect can only return via the connect timeout. Without a
    # timeout (the pre-fix :infinity default) this call blocks for the full OS
    # TCP timeout and the test would hang until the ExUnit case timeout below.
    @tag timeout: 5_000
    test "create/5 bounds a connect to an unreachable host by connect_timeout" do
      assert {:error, :timeout} =
               KafkaEx.Network.Socket.create({192, 0, 2, 1}, 9092, [:binary, {:packet, 4}], false, 200)
    end
  end
end
