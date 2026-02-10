defmodule KafkaEx.Cluster.BrokerTest do
  use ExUnit.Case, async: false
  import KafkaEx.TestHelpers

  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Network.Socket

  setup do
    port = get_free_port(3040)
    pid = KafkaEx.TestSupport.Server.start(port)

    {:ok, socket} = Socket.create(~c"localhost", port, [:binary, {:packet, 0}], false)

    on_exit(fn ->
      Socket.close(socket)
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
      broker = %Broker{socket: %Socket{}} |> Broker.put_socket(nil)

      assert is_nil(broker.socket)
    end

    test "updates broker socket to new one" do
      socket = %Socket{}
      broker = %Broker{socket: nil} |> Broker.put_socket(socket)

      assert broker.socket == socket
    end
  end

  describe "address/1" do
    test "returns host:port string" do
      broker = %Broker{host: "kafka.example.com", port: 9092}
      assert "kafka.example.com:9092" == Broker.address(broker)
    end

    test "handles localhost" do
      broker = %Broker{host: "localhost", port: 9092}
      assert "localhost:9092" == Broker.address(broker)
    end
  end

  describe "to_string/1" do
    test "returns formatted broker identifier" do
      broker = %Broker{node_id: 1, host: "kafka.example.com", port: 9092}
      assert "broker 1 (kafka.example.com:9092)" == Broker.to_string(broker)
    end

    test "handles different node_ids" do
      broker = %Broker{node_id: 42, host: "localhost", port: 9093}
      assert "broker 42 (localhost:9093)" == Broker.to_string(broker)
    end
  end

  describe "has_socket?/1 (arity 1)" do
    test "returns false for nil socket" do
      broker = %Broker{socket: nil}
      refute Broker.has_socket?(broker)
    end

    test "returns true for non-nil socket" do
      broker = %Broker{socket: %Socket{}}
      assert Broker.has_socket?(broker)
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
      socket = %Socket{}

      refute Broker.has_socket?(broker, socket)
    end

    test "returns false if broker has different socket", %{socket: socket_one, port: port} do
      {:ok, socket_two} =
        Socket.create(~c"localhost", port, [:binary, {:packet, 0}], false)

      broker = %Broker{socket: nil} |> Broker.put_socket(socket_one)

      refute Broker.has_socket?(broker, socket_two.socket)
      Socket.close(socket_two)
    end

    test "returns true if broker has same socket", %{socket: socket} do
      broker = %Broker{socket: nil} |> Broker.put_socket(socket)

      assert Broker.has_socket?(broker, socket.socket)
    end
  end
end
