defmodule KafkaEx.NetworkClientTest do
  use ExUnit.Case, async: true

  use Hammox.Protect,
    module: KafkaEx.NetworkClient,
    behaviour: KafkaEx.NetworkClient.Behaviour

  describe "close_socket/1" do
    test "closes the socket" do
      {:ok, socket} =
        :gen_tcp.listen(3001, [
          :binary,
          {:active, false},
          {:reuseaddr, true},
          {:packet, 0}
        ])

      kafka_ex_socket = %KafkaEx.Socket{socket: socket}

      assert :ok == close_socket(kafka_ex_socket)
      assert {:error, :closed} == :gen_tcp.send(socket, <<>>)
    end

    test "does not fail if socket is nil" do
      assert :ok == close_socket(nil)
    end
  end

  describe "create_socket/3" do
    setup do
      pid = KafkaEx.TestSupport.Server.start(3040)

      on_exit(fn ->
        Process.exit(pid, :normal)
      end)
    end

    test "creates a socket" do
      kafka_ex_socket = create_socket("localhost", 3040, [], false)

      assert kafka_ex_socket.socket
      assert kafka_ex_socket.ssl == false
    end

    test "returns nil if socket creation fails" do
      assert nil == create_socket("localhost", 3002, [], true)
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
  end
end
