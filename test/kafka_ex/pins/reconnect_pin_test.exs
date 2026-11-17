defmodule KafkaEx.Pins.ReconnectPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Messages.RecordMetadata

  @moduletag pin_set: :invariant

  setup do
    pinned = start_pinned()
    FakeTransport.respond(pinned.fake, :produce, produce_reply())
    watch_close(pinned.host)
    pinned
  end

  defp drop_connection(fake, host) do
    FakeTransport.close(fake, {host, 9092})
    assert_receive {:pin_closed, %{port: 9092, reason: :remote_closed}}, 5_000
  end

  describe "reconnects" do
    test "after the peer closes the socket, the next request succeeds", %{fake: fake, client: client, host: host} do
      drop_connection(fake, host)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert {:ok, %RecordMetadata{base_offset: 7}} = result
      assert count(fake, :produce) == 1
    end

    test "a broker down for less than the reconnect window serves the request", %{
      fake: fake,
      client: client,
      host: host,
      gate: gate
    } do
      drop_connection(fake, host)
      PinGate.close_for(gate, 700)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert {:ok, %RecordMetadata{base_offset: 7}} = result
      assert count(fake, :produce) == 1
    end

    test "a broker down for longer than the window serves the first request after it is back", %{
      fake: fake,
      client: client,
      host: host,
      gate: gate
    } do
      with_request_timeout(2_000)
      drop_connection(fake, host)
      PinGate.close(gate)
      {_while_down, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
      PinGate.open(gate)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert {:ok, %RecordMetadata{base_offset: 7}} = result
    end
  end

  describe "a peer close after the write" do
    for {api, attempts, call} <- [
          {:produce, 1, quote(do: API.produce(var!(client), @topic, 0, [%{value: "v"}]))},
          {:fetch, 3, quote(do: API.fetch(var!(client), @topic, 0, 0, max_wait_time: 10))},
          {:offset_commit, 3,
           quote(do: API.commit_offset(var!(client), "g", @topic, [%{partition_num: 0, offset: 1}]))},
          {:heartbeat, 3, quote(do: API.heartbeat(var!(client), "g", "m", 1))}
        ] do
      test "#{api} returns :closed after #{attempts} write(s)", %{fake: fake, client: client} do
        FakeTransport.respond(fake, unquote(api), :close)
        var!(client) = client

        {result, _log} = with_log(fn -> unquote(call) end)

        assert result == {:error, :closed}
        assert count(fake, unquote(api)) == unquote(attempts)
      end
    end
  end
end
