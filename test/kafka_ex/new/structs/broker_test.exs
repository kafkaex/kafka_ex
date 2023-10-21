defmodule KafkaEx.New.Structs.BrokerTest do
  use ExUnit.Case, async: false
  import KafkaEx.TestHelpers

  alias KafkaEx.New.Structs.Broker

  setup do
    port = get_free_port(3040)
    pid = KafkaEx.TestSupport.Server.start(port)

    {:ok, socket} = KafkaEx.Socket.create(~c"localhost", 3040, [:binary, {:packet, 0}], false)

    on_exit(fn ->
      KafkaEx.Socket.close(socket)
      Process.exit(pid, :normal)
    end)

    {:ok, [socket: socket, port: port]}
  end

  describe "connect_broker/1" do
    @tag skip: true
    test "connects broker via socket" do
    end
  end

  describe "put_socket/2" do
    test "nullify broker socket" do
      broker = %Broker{socket: %KafkaEx.Socket{}} |> Broker.put_socket(nil)

      assert is_nil(broker.socket)
    end

    test "updates broker socket to new one" do
      socket = %KafkaEx.Socket{}
      broker = %Broker{socket: nil} |> Broker.put_socket(socket)

      assert broker.socket == socket
    end
  end

  describe "connected?/1" do
    test "returns false if socket is nil" do
      broker = %Broker{socket: nil}

      refute Broker.connected?(broker)
    end

    test "returns false if socket is not connected" do
      broker = %Broker{socket: nil}

      refute Broker.connected?(broker)
    end

    test "returns true if socket is connected", %{socket: socket} do
      broker = %Broker{socket: socket}

      assert Broker.connected?(broker)
    end
  end

  describe "has_socket?/1" do
    test "returns false if broker doesn't have a socket" do
      broker = %Broker{socket: nil}
      socket = %KafkaEx.Socket{}

      refute Broker.has_socket?(broker, socket)
    end

<<<<<<< HEAD
    test "returns false if broker has different socket", %{socket: socket_one} do
      {:ok, socket_two} =
        KafkaEx.Socket.create(~c"localhost", 3040, [:binary, {:packet, 0}], false)
=======
    test "returns false if broker has different socket", %{
      socket: socket_one,
      port: port
    } do
      {:ok, socket_two} = KafkaEx.Socket.create('localhost', port, [:binary, {:packet, 0}], false)
>>>>>>> 9b52187 ([Kayrock] Refactor test helpers, add random ports & fix compile issues)

      broker = %Broker{socket: nil} |> Broker.put_socket(socket_one)

      refute Broker.has_socket?(broker, socket_two.socket)
      KafkaEx.Socket.close(socket_two)
    end

    test "returns true if broker has same socket", %{socket: socket} do
      broker = %Broker{socket: nil} |> Broker.put_socket(socket)

      assert Broker.has_socket?(broker, socket.socket)
    end
  end
end
