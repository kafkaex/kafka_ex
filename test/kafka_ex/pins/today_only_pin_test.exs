defmodule KafkaEx.Pins.TodayOnlyPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Config

  @moduletag pin_set: :invariant

  test ":no_broker is returned well before the request deadline" do
    %{client: client} = start_pinned()
    started = System.monotonic_time(:millisecond)

    {result, _log} = with_log(fn -> API.produce(client, "no-such-topic", 0, [%{value: "v"}]) end)

    assert result == {:error, :no_broker}
    assert System.monotonic_time(:millisecond) - started < div(Config.request_timeout(), 10)
  end

  test "a known leader whose broker is unreachable returns :no_broker after the reconnect window" do
    with_request_timeout(10_000)
    %{fake: fake, client: client, host: host, gate: gate} = start_pinned()
    watch_close(host)
    drop_connection(fake, host)
    PinGate.close(gate)
    refreshes = count(fake, :metadata)
    started = System.monotonic_time(:millisecond)

    {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

    assert result == {:error, :no_broker}
    assert System.monotonic_time(:millisecond) - started < 5_000
    assert count(fake, :metadata) == refreshes
  end

  test "correlation_id/1 counts network attempts" do
    %{fake: fake, client: client} = start_pinned()
    FakeTransport.respond(fake, :produce, produce_reply())
    FakeTransport.respond(fake, :fetch, :close)
    {:ok, before} = API.correlation_id(client)

    {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    {{:error, :closed}, _log} = with_log(fn -> API.fetch(client, @topic, 0, 0, max_wait_time: 10) end)

    assert {:ok, after_calls} = API.correlation_id(client)
    assert after_calls - before == count(fake, :produce) + count(fake, :fetch)
    assert after_calls - before == 4
  end
end
