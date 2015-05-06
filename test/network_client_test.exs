defmodule KafkaEx.NetworkClient.Test do
  use ExUnit.Case
  import Mock

  defmodule FakeProtocol do
    def create_request(_, _, data), do: data
  end

  test "format_host returns Erlang IP address format if IP address string is specified" do
    assert {100, 20, 3, 4} == KafkaEx.NetworkClient.format_host("100.20.3.4")
  end

  test "format_host returns the char list version of the string passed in if host is not IP address" do
    assert 'host' == KafkaEx.NetworkClient.format_host("host")
  end

  test "format_host handles hosts with embedded digits correctly" do
    assert 'host0' == KafkaEx.NetworkClient.format_host("host0")
  end

  test "update_from_metadata updates the host list" do
    client = KafkaEx.NetworkClient.new("test")
    client = %{client | hosts: %{"foo:9092" => nil, "bar:9092" => nil}}
    client = KafkaEx.NetworkClient.update_from_metadata(client, ["bar:9092", "baz:9093"])
    assert client.hosts == %{"bar:9092" => nil, "baz:9093" => nil}
  end

  test "send_request creates and sends the specified request" do
    client = KafkaEx.NetworkClient.new("test")
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:ok, :socket} end,
                                     send: fn(_, _) -> :ok end] do
      client = KafkaEx.NetworkClient.send_request(client, [{"foo", 9092}], ["request"], FakeProtocol)
      assert called :gen_tcp.connect(to_char_list("foo"), 9092, [:binary, {:packet, 4}])
      assert called :gen_tcp.send(:socket, "request")
    end
  end

  test "send_request increments correlation_id" do
    client = Map.put(KafkaEx.NetworkClient.new("test"), :correlation_id, 1)
    with_mock :gen_tcp, [:unstick], [connect: fn(_, _, _) -> {:ok, :socket} end, send: fn(_, "request") -> :ok end] do
      client = KafkaEx.NetworkClient.send_request(client, [{"foo", 9092}], ["request"], FakeProtocol)
      assert client.correlation_id == 2
    end
  end
end
