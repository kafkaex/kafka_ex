defmodule KafkaEx.Network.NetworkClientTest do
  use ExUnit.Case, async: true
  import KafkaEx.TestHelpers

  use Hammox.Protect,
    module: KafkaEx.Network.NetworkClient,
    behaviour: KafkaEx.Network.Behaviour

  describe "close_socket/1" do
    test "closes the socket" do
      {:ok, socket} =
        :gen_tcp.listen(get_free_port(3001), [
          :binary,
          {:active, false},
          {:reuseaddr, true},
          {:packet, 0}
        ])

      kafka_ex_socket = %KafkaEx.Network.Socket{socket: socket}

      assert :ok == close_socket(kafka_ex_socket)
      assert {:error, :closed} == :gen_tcp.send(socket, <<>>)
    end

    test "does not fail if socket is nil" do
      assert :ok == close_socket(nil)
    end
  end

  describe "create_socket/3" do
    setup do
      port = get_free_port(3040)
      pid = KafkaEx.TestSupport.Server.start(port)

      on_exit(fn ->
        Process.exit(pid, :normal)
      end)

      {:ok, [port: port]}
    end

    test "creates a socket", %{port: port} do
      kafka_ex_socket = create_socket("localhost", port, [], false)

      assert kafka_ex_socket.socket
      assert kafka_ex_socket.ssl == false
    end

    test "returns nil if socket creation fails" do
      port = get_free_port(3040)
      assert nil == create_socket("localhost", port, [], true)
    end
  end

  describe "send_sync_request/3" do
    test "returns {:error, :not_connected} when broker socket is nil" do
      broker = %{socket: nil, host: "localhost", port: 9092}
      assert {:error, :not_connected} == KafkaEx.Network.NetworkClient.send_sync_request(broker, <<>>, 1000)
    end

    test "returns {:error, :no_broker} when broker is nil" do
      assert {:error, :no_broker} == KafkaEx.Network.NetworkClient.send_sync_request(nil, <<>>, 1000)
    end
  end

  describe "close_socket/3" do
    test "closes socket with broker context and reason" do
      {:ok, socket} =
        :gen_tcp.listen(get_free_port(3050), [
          :binary,
          {:active, false},
          {:reuseaddr, true},
          {:packet, 0}
        ])

      kafka_ex_socket = %KafkaEx.Network.Socket{socket: socket}
      broker = %{host: "localhost", port: 9092}

      assert :ok == KafkaEx.Network.NetworkClient.close_socket(broker, kafka_ex_socket, :shutdown)
      assert {:error, :closed} == :gen_tcp.send(socket, <<>>)
    end

    test "returns :ok when socket is nil" do
      broker = %{host: "localhost", port: 9092}
      assert :ok == KafkaEx.Network.NetworkClient.close_socket(broker, nil, :shutdown)
    end
  end

  describe "send_async_request/2" do
    test "returns {:error, :not_connected} when broker socket is nil" do
      broker = %{socket: nil, host: "localhost", port: 9092}
      assert {:error, :not_connected} == KafkaEx.Network.NetworkClient.send_async_request(broker, <<>>)
    end

    test "sends data successfully when socket is connected" do
      port = get_free_port(3060)
      pid = KafkaEx.TestSupport.Server.start(port)

      {:ok, tcp_socket} = :gen_tcp.connect(~c"localhost", port, [:binary, {:active, false}, {:packet, 0}])

      kafka_socket = %KafkaEx.Network.Socket{socket: tcp_socket, ssl: false}
      broker = %{socket: kafka_socket, host: "localhost", port: port}

      assert :ok == KafkaEx.Network.NetworkClient.send_async_request(broker, "test data")

      Process.exit(pid, :normal)
      :gen_tcp.close(tcp_socket)
    end
  end

  describe "format_host/1" do
    test "format_host returns Erlang IP address format if IP address string is specified" do
      assert {100, 20, 3, 4} == format_host("100.20.3.4")
    end

    test "format_host returns the char list version of the string passed in if host is not IP address" do
      assert ~c"host" == format_host("host")
    end

    test "format_host handles hosts with embedded digits correctly" do
      assert ~c"host0" == format_host("host0")
    end

    test "format_host correct handles hosts embedded with ip address" do
      assert ~c"ip.10.4.1.11" == format_host("ip.10.4.1.11")
      assert ~c"ip-10-4-1-11" == format_host("ip-10-4-1-11")
    end

    test "format_host handles all-zeros IP" do
      assert {0, 0, 0, 0} == format_host("0.0.0.0")
    end

    test "format_host handles max octets IP" do
      assert {255, 255, 255, 255} == format_host("255.255.255.255")
    end

    test "format_host handles hostname with numbers" do
      assert ~c"broker1" == format_host("broker1")
      assert ~c"kafka-broker-01.example.com" == format_host("kafka-broker-01.example.com")
    end
  end
end
