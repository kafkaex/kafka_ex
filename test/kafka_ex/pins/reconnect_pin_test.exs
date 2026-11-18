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

  describe "reconnects" do
    test "after the peer closes the socket, the next request succeeds", %{fake: fake, client: client, host: host} do
      {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
      drop_connection(fake, host)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert {:ok, %RecordMetadata{base_offset: 7}} = result
      assert count(fake, :produce) == 2
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
      # Keeps a request that a future client may queue below ExUnit's timeout.
      with_request_timeout(2_000)
      drop_connection(fake, host)
      PinGate.close(gate)
      {while_down, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
      PinGate.open(gate)
      # Gives a connection whose reconnect backoff grew while the broker was down time to come back.
      with_request_timeout(10_000)

      {result, _log} = with_log(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert {:error, _} = while_down
      assert {:ok, %RecordMetadata{base_offset: 7}} = result
    end
  end

  describe "a peer close after the write" do
    for {api, node, attempts, call} <- [
          {:produce, 1, 1, quote(do: API.produce(var!(client), @topic, 0, [%{value: "v"}]))},
          {:fetch, 1, 3, quote(do: API.fetch(var!(client), @topic, 0, 0, max_wait_time: 10))},
          {:offset_commit, 2, 3,
           quote(do: API.commit_offset(var!(client), "g", @topic, [%{partition_num: 0, offset: 1}]))},
          {:heartbeat, 2, 3, quote(do: API.heartbeat(var!(client), "g", "m", 1))}
        ] do
      test "#{api} returns :closed after #{attempts} write(s)", %{fake: fake, client: client} do
        FakeTransport.respond(fake, unquote(api), :close)
        var!(client) = client

        {result, _log} = with_log(fn -> unquote(call) end)

        assert result == {:error, :closed}
        assert count(fake, unquote(api)) == unquote(attempts)
        assert Enum.uniq(nodes(fake, unquote(api))) == [unquote(node)]
      end
    end
  end
end
