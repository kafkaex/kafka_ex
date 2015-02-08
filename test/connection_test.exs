defmodule Kafka.Connection.Test do
  use ExUnit.Case
  import Mock

  test "connect_brokers returns {{host, port}, socket} on successful connection" do
    host = "foo"
    port = 1024
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:ok, :socket} end] do
      assert :socket = Kafka.Connection.connect_brokers([{host, port}])
      assert called :gen_tcp.connect(to_char_list(host), port, [:binary, {:packet, 4}])
    end
  end

  test "connect_brokers raises ConnectionError on unsuccessful connection" do
    host = "foo"
    port = 1024
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:error, :something} end] do
      assert_raise Kafka.ConnectionError, "Error cannot connect", fn ->
        Kafka.Connection.connect_brokers([{host, port}])
      end
      assert called :gen_tcp.connect(to_char_list(host), port, [:binary, {:packet, 4}])
    end
  end

  test "connect_brokers connects successfully to IP address" do
    host = "127.0.0.1"
    port = 1024
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:ok, :socket} end] do
      assert :socket = Kafka.Connection.connect_brokers([{host, port}])
      assert called :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, {:packet, 4}])
    end
  end
end
