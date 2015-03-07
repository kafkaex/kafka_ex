defmodule KafkaEx.Connection.Test do
  use ExUnit.Case
  import Mock

  test "format_host returns Erlang IP address format if IP address string is specified" do
    assert {100, 20, 3, 4} == KafkaEx.Connection.format_host("100.20.3.4")
  end

  test "format_host returns the char list version of the string passed in if host is not IP address" do
    assert 'host' == KafkaEx.Connection.format_host("host")
  end

  test "format_host handles hosts with embedded digits correctly" do
    assert 'host0' == KafkaEx.Connection.format_host("host0")
  end

  test "format_uri handles port as char_list" do
    assert {'host0', 9092} = KafkaEx.Connection.format_uri('host0', '9092')
  end

  test "format_uri handles a string port" do
    assert {'host0', 9092} = KafkaEx.Connection.format_uri('host0', "9092")
  end

  test "format_uri handles a char_list host" do
    assert {'host0', 9092} = KafkaEx.Connection.format_uri('host0', "9092")
  end

  test "connect_brokers returns {{host, port}, socket} on successful connection" do
    host = "foo"
    port = 1024
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:ok, :socket} end] do
      assert %{{"foo", 1024} => :socket} = KafkaEx.Connection.connect_brokers([{host, port}], %{})
      assert called :gen_tcp.connect(to_char_list(host), port, [:binary, {:packet, 4}])
    end
  end

  test "connect_brokers raises ConnectionError on unsuccessful connection" do
    host = "foo"
    port = 1024
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:error, :something} end] do
      assert_raise KafkaEx.ConnectionError, "Error: Cannot connect to any of the broker(s) provided", fn ->
        KafkaEx.Connection.connect_brokers([{host, port}])
      end
      assert called :gen_tcp.connect(to_char_list(host), port, [:binary, {:packet, 4}])
    end
  end

  test "connect_brokers raises ConnectionError on badly formatted broker info" do
    host = 1
    port = 1024
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:error, :something} end] do
      assert_raise KafkaEx.ConnectionError, "Error: Bad broker format '{1, 1024}'", fn ->
        KafkaEx.Connection.connect_brokers([{host, port}])
      end
      refute called :gen_tcp.connect(to_char_list(host), port, [:binary, {:packet, 4}])
    end
  end

  test "connect_brokers connects successfully to IP address" do
    host = "127.0.0.1"
    port = 1024
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:ok, :socket} end] do
      assert %{{"127.0.0.1", 1024} => :socket} == KafkaEx.Connection.connect_brokers([{host, port}])
      assert called :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, {:packet, 4}])
    end
  end
end
