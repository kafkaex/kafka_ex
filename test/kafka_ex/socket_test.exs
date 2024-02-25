defmodule KafkaEx.Socket.Test do
  use ExUnit.Case, async: false

  setup_all do
    :ssl.start()
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
      {:ok, [ssl_port: 3030]}
    end

    test "create a SSL socket", context do
      {:ok, socket} =
        KafkaEx.Socket.create(
          ~c"localhost",
          context[:ssl_port],
          [:binary, {:packet, 0}],
          true
        )

      assert socket.ssl == true
      KafkaEx.Socket.close(socket)
    end

    test "send and receive using a SSL socket", context do
      {:ok, socket} =
        KafkaEx.Socket.create(
          ~c"localhost",
          context[:ssl_port],
          [:binary, {:packet, 0}, {:active, false}],
          true
        )

      KafkaEx.Socket.send(socket, ~c"ping")
      assert {:ok, "ping"} == KafkaEx.Socket.recv(socket, 0)
      KafkaEx.Socket.close(socket)
    end

    test "retrieve info from a SSL socket", context do
      {:ok, socket} =
        KafkaEx.Socket.create(
          ~c"localhost",
          context[:ssl_port],
          [:binary, {:packet, 0}, {:active, false}],
          true
        )

      info = KafkaEx.Socket.info(socket)
      assert info[:name] == ~c"tcp_inet"
      KafkaEx.Socket.close(socket)
      assert {:error, :closed} == KafkaEx.Socket.send(socket, ~c"ping")
    end
  end
end
