defmodule KafkaEx.NetworkClient.Test do
  use ExUnit.Case

  alias KafkaEx.Network.{Client, Error}

  @moduletag :capture_log

  test "client should raise exception when it cannot connect" do
    {:module, mod, _, _} = defmodule FailingConnect do
      def connect(host, port) do
        send(self(), {:connect, [host, port]})
        {:error, :reason}
      end
    end
    Mix.Shell.Process.flush(&IO.inspect(&1))
    assert_raise Error, fn ->
      Client.connect("fake_host", 99, mod)
    end
    assert_receive {:connect, ['fake_host', 99]}
  end

  test "client should raise exception when it cannot send" do
    {:module, mod, _, _} = defmodule FailingSend do
      def send(socket, data) do
        Kernel.send(self(), {:send, [socket, data]})
        {:error, :reason}
      end
    end
    assert_raise Error, fn ->
      Client.send_async_request(%{socket: :socket, host: :host, port: :port}, "hello", mod)
    end
    assert_receive {:send, [:socket, "hello"]}
    assert_raise Error, fn ->
      Client.send_sync_request(%{socket: :socket, host: :host, port: :port}, "hello", 10, mod)
    end
    assert_receive {:send, [:socket, "hello"]}
  end

  test "client should raise exception when it cannot receive" do
    {:module, mod, _, _} = defmodule FailingRecv do
      def send(socket, data) do
        Kernel.send(self(), {:send, [socket, data]})
        :ok
      end
      def recv(socket, timeout) do
        Kernel.send(self(), {:recv, [socket, timeout]})
        {:error, :reason}
      end
    end
    assert_raise Error, fn ->
      Client.send_sync_request(%{socket: :socket, host: :host, port: :port}, "hello", 10, mod)
    end
    assert_receive {:send, [:socket, "hello"]}
    assert_receive {:recv, [:socket, 10]}
  end

  test "client should always be able to close the connection" do
    {:module, mod, _, _} = defmodule Close do
      def close(socket) do
        send(self(), {:close, [socket]})
        :ok
      end
    end
    assert :ok == Client.close(%{socket: :socket}, mod)
    assert_receive {:close, [:socket]}
  end

  test "format_host returns Erlang IP address format if IP address string is specified" do
    assert {100, 20, 3, 4} == Client.format_host("100.20.3.4")
  end

  test "format_host returns the char list version of the string passed in if host is not IP address" do
    assert 'host' == Client.format_host("host")
  end

  test "format_host handles hosts with embedded digits correctly" do
    assert 'host0' == Client.format_host("host0")
  end

  test "format_host correct handles hosts embedded with ip address" do
    assert 'ip.10.4.1.11' == Client.format_host("ip.10.4.1.11")
    assert 'ip-10-4-1-11' == Client.format_host("ip-10-4-1-11")
  end

end
