defmodule KafkaEx.Socket.Test do
  use ExUnit.Case, async: false

  setup_all do
    {:ok, _} = Application.ensure_all_started(:ssl)
  end

  describe "without SSL socket" do
    setup do
      KafkaEx.TestSupport.Server.start(3040)
      {:ok, [port: 3040]}
    end

    test "create a non SSL socket", context do
      {:ok, socket} =
        KafkaEx.Socket.create(
          ~c"localhost",
          context[:port],
          [:binary, {:packet, 0}],
          false
        )

      assert socket.ssl == false
      KafkaEx.Socket.close(socket)
    end

    test "send and receive using a non SSL socket", context do
      {:ok, socket} =
        KafkaEx.Socket.create(
          ~c"localhost",
          context[:port],
          [:binary, {:packet, 0}, {:active, false}],
          false
        )

      KafkaEx.Socket.send(socket, ~c"ping")
      assert {:ok, "ping"} == KafkaEx.Socket.recv(socket, 0)
      KafkaEx.Socket.close(socket)
    end

    test "retrieve info from a non SSL socket", context do
      {:ok, socket} =
        KafkaEx.Socket.create(
          ~c"localhost",
          context[:port],
          [:binary, {:packet, 0}, {:active, false}],
          false
        )

      info = KafkaEx.Socket.info(socket)
      assert info[:name] == ~c"tcp_inet"
      KafkaEx.Socket.close(socket)
      assert {:error, :closed} == KafkaEx.Socket.send(socket, ~c"ping")
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
        KafkaEx.Socket.create(~c"localhost", context[:ssl_port], context[:ssl_opts], true)

      assert socket.ssl == true
      KafkaEx.Socket.close(socket)
    end

    test "send and receive using a SSL socket", context do
      {:ok, socket} =
        KafkaEx.Socket.create(~c"localhost", context[:ssl_port], context[:ssl_opts], true)

      KafkaEx.Socket.send(socket, ~c"ping")
      assert {:ok, "ping"} == KafkaEx.Socket.recv(socket, 0)
      KafkaEx.Socket.close(socket)
    end

    test "retrieve info from a SSL socket", context do
      {:ok, socket} =
        KafkaEx.Socket.create(~c"localhost", context[:ssl_port], context[:ssl_opts], true)

      info = KafkaEx.Socket.info(socket)
      assert info[:name] == ~c"tcp_inet"
      KafkaEx.Socket.close(socket)
      assert {:error, :closed} == KafkaEx.Socket.send(socket, ~c"ping")
    end
  end
end
