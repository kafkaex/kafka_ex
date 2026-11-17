defmodule KafkaExBench.ToxiproxyTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Toxiproxy

  test "verify_latency! accepts exactly one latency toxic with the expected value" do
    toxics = [%{"name" => "latency", "attributes" => %{"latency" => 2, "jitter" => 0}}]
    assert Toxiproxy.verify_latency!("kafka1_ssl", toxics, 2) == :ok
  end

  test "verify_latency! rejects wrong value, missing toxic and extra toxics naming the proxy" do
    latency = fn ms -> %{"name" => "latency", "attributes" => %{"latency" => ms}} end
    other = %{"name" => "bandwidth", "attributes" => %{"rate" => 1}}

    for toxics <- [[latency.(5)], [], [latency.(2), other], [latency.(2), latency.(2)]] do
      error = assert_raise RuntimeError, fn -> Toxiproxy.verify_latency!("kafka2_scram", toxics, 2) end
      assert error.message =~ "kafka2_scram"
    end
  end

  test "ensure_status! raises with proxy name and result" do
    assert Toxiproxy.ensure_status!({:ok, 200}, 200, "kafka1_ssl") == :ok
    error = assert_raise RuntimeError, fn -> Toxiproxy.ensure_status!({:ok, 409}, 200, "kafka1_ssl") end
    assert error.message =~ "kafka1_ssl"
    assert error.message =~ "409"
  end

  test "parse_version reads a JSON body" do
    assert Toxiproxy.parse_version(~s({"version":"2.9.0"})) == "2.9.0"
  end

  test "parse_version returns a plain body trimmed" do
    assert Toxiproxy.parse_version("2.5.0\n") == "2.5.0"
  end

  test "parse_version rejects JSON without a version" do
    assert_raise RuntimeError, ~r/version/, fn -> Toxiproxy.parse_version(~s({"other":1})) end
  end

  test "ensure_get_status! raises with path and status" do
    assert Toxiproxy.ensure_get_status!({:ok, {{~c"HTTP/1.1", 200, ~c"OK"}, [], "body"}}, "/version") == "body"

    error =
      assert_raise RuntimeError, fn ->
        Toxiproxy.ensure_get_status!({:ok, {{~c"HTTP/1.1", 404, ~c"Not Found"}, [], ""}}, "/proxies/x/toxics")
      end

    assert error.message =~ "/proxies/x/toxics"
    assert error.message =~ "404"
  end

  test "ensure_get_status! raises with path on a transport error" do
    error = assert_raise RuntimeError, fn -> Toxiproxy.ensure_get_status!({:error, :econnrefused}, "/version") end
    assert error.message =~ "/version"
    assert error.message =~ "econnrefused"
  end
end
