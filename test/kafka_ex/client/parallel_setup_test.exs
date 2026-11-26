defmodule KafkaEx.Client.ParallelSetupTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.ConnectionStateMachine.ReconnectBackoff

  @moduletag capture_log: true

  @handshake_ms 400
  @short_handshake_ms 100
  @min_capped_backoff_ms 400
  @window_ms 1_500
  # Far below the call's own deadline, so only the crash, not that deadline, can answer it.
  @crash_answered_within_ms 5_000

  describe "first fetch connections" do
    test "to different brokers run their handshakes at the same time" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      :ok = FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :api_versions)

      fetches = for partition <- [0, 1], do: Task.async(fn -> API.fetch(client, @topic, partition, 0) end)

      assert_receive {:fake_transport, :request, %{api: :api_versions, port: 9092, host: ^host}}, 2_000
      assert_receive {:fake_transport, :request, %{api: :api_versions, port: 9093, host: ^host}}, 2_000

      FakeTransport.release(fake, :api_versions)
      assert [{:ok, _}, {:ok, _}] = Task.await_many(fetches, 10_000)
    end

    test "to different brokers finish in under two scripted handshakes" do
      %{fake: fake, client: client} = start_pinned()
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      FakeTransport.delay_connects(fake, @handshake_ms)

      started = System.monotonic_time(:millisecond)
      fetches = for partition <- [0, 1], do: Task.async(fn -> API.fetch(client, @topic, partition, 0) end)
      assert [{:ok, _}, {:ok, _}] = Task.await_many(fetches, 10_000)

      assert System.monotonic_time(:millisecond) - started < 2 * @handshake_ms
    end

    test "to one broker are written in arrival order once it connects" do
      %{fake: fake, client: client} = start_pinned(topics: %{@topic => [1, 1]})
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      :ok = FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :api_versions)
      watch_fetch_stops()

      first = Task.async(fn -> API.fetch(client, @topic, 0, 0) end)
      assert_receive {:fake_transport, :request, %{api: :api_versions}}, 2_000
      second = Task.async(fn -> API.fetch(client, @topic, 1, 0) end)

      FakeTransport.release(fake, :api_versions)
      assert [{:ok, _}, {:ok, _}] = Task.await_many([first, second], 10_000)

      assert_receive {:fetch_stop, earlier}
      assert_receive {:fetch_stop, later}
      assert [earlier, later] == [0, 1]
      assert count(fake, :fetch) == 2
    end

    test "whose handshake crashes is retried on a new connection, as a waiting call would be" do
      raise_once = :atomics.new(1, [])
      provider = fn -> if :atomics.compare_exchange(raise_once, 1, 1, 0) == :ok, do: raise("down"), else: {:ok, "t"} end
      %{fake: fake, client: client} = start_pinned(client: [auth: oauthbearer(provider)])
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      :atomics.put(raise_once, 1, 1)

      started = System.monotonic_time(:millisecond)
      assert {:ok, _} = API.fetch(client, @topic, 0, 0)
      assert System.monotonic_time(:millisecond) - started < @crash_answered_within_ms
      assert :atomics.get(raise_once, 1) == 0
      assert Process.alive?(client)
    end
  end

  describe "a first fetch to an unreachable broker" do
    test "errors in under two reconnect windows, as a call waiting in the client did" do
      %{client: client, gate: gate} = start_pinned(connection: [reconnect_window: @window_ms])
      PinGate.close(gate)

      started = System.monotonic_time(:millisecond)
      assert {:error, :no_broker} = API.fetch(client, @topic, 1, 0, max_wait_time: 10)

      assert System.monotonic_time(:millisecond) - started < 2 * @window_ms
    end
  end

  describe "a first connect of another role to a broker whose fetch connection is connecting" do
    test "starts when that connect succeeds, not when its backoff slot opens" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      FakeTransport.respond(fake, :produce, produce_reply())
      raise_next_backoff_to_cap(:sys.get_state(client).reconnect_backoff, {host, 9092})
      FakeTransport.delay_connects(fake, @short_handshake_ms)

      started = System.monotonic_time(:millisecond)
      fetch = Task.async(fn -> API.fetch(client, @topic, 0, 0) end)
      wait_for(fn -> :ets.member(:sys.get_state(client).conns, {host, 9092, :fetch}) end)
      assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
      assert {:ok, _} = Task.await(fetch, 10_000)

      assert System.monotonic_time(:millisecond) - started < @min_capped_backoff_ms + @short_handshake_ms
    end
  end

  # Claims spaced past each backoff raise the failure count; connected/3 then reopens the slot at once.
  defp raise_next_backoff_to_cap(backoff, address) do
    now = System.monotonic_time(:millisecond)
    for step <- 1..4, do: :ok = ReconnectBackoff.claim(backoff, address, now + step * 10_000)
    ReconnectBackoff.connected(backoff, address, now)
  end

  defp wait_for(condition) do
    KafkaEx.TestHelpers.wait_for_value(condition, & &1, 10, 300)
  end

  defp watch_fetch_stops do
    forward_telemetry([[:kafka_ex, :fetch, :stop]], fn _event, _measurements, metadata ->
      {:fetch_stop, metadata.partition}
    end)
  end
end
