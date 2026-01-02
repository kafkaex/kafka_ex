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
      assert {:error, :closed} == KafkaEx.Network.Socket.send(socket, ~c"ping")
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
      assert {:error, :closed} == KafkaEx.Network.Socket.send(socket, ~c"ping")
    end
  end
end
