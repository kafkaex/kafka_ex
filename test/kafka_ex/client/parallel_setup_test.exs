defmodule KafkaEx.Client.ParallelSetupTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.ConnectionStateMachine.ReconnectBackoff

  @moduletag capture_log: true

  @handshake_ms 400
  @short_handshake_ms 100
  @min_capped_backoff_ms 400
  @window_ms 1_500
  # Far below the parked call's own deadline, so only the crash, not that deadline, can answer it.
  @crash_answered_within_ms 5_000
  @long_poll_ms 1_500
  @blocked_connect_ms 4_500

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
      wait_for(fn -> map_size(:sys.get_state(client).parked) == 2 end)

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

  describe "parked first fetches" do
    # Scaled-down 10 s long-polls: the third resume starts after two answered long-polls.
    test "each get a full attempt budget from their resume, so none is written then retried" do
      connection = [connect_timeout: 500, reconnect_window: 100]
      %{fake: fake, client: client} = start_pinned(topics: %{@topic => [1, 1, 1]}, client: [connection: connection])
      FakeTransport.respond(fake, :fetch, {:after, @long_poll_ms, &{:frame, fetch_frame(&1, -1)}})
      FakeTransport.hold(fake, :api_versions)
      opts = [max_wait_time: @long_poll_ms, network_timeout: @long_poll_ms + 500]

      fetches = for partition <- 0..2, do: Task.async(fn -> API.fetch(client, @topic, partition, 0, opts) end)
      wait_for(fn -> map_size(:sys.get_state(client).parked) == 3 end)
      FakeTransport.release(fake, :api_versions)

      assert [{:ok, _}, {:ok, _}, {:ok, _}] = Task.await_many(fetches, 15_000)
      assert count(fake, :fetch) == 3
    end

    test "whose connect outlasts the parked deadline end that attempt :timeout and retry" do
      %{fake: fake, client: client} = start_pinned(client: [connection: [connect_timeout: 50, reconnect_window: 0]])
      FakeTransport.delay_connects(fake, @blocked_connect_ms)

      started = System.monotonic_time(:millisecond)
      assert {:error, _timeout} = API.fetch(client, @topic, 0, 0, max_wait_time: 1, network_timeout: 1)

      assert System.monotonic_time(:millisecond) - started < @blocked_connect_ms
      assert Process.alive?(client)
    end

    test "resume on the new leader after a leader move" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      :ok = FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :api_versions)

      fetch = Task.async(fn -> API.fetch(client, @topic, 0, 0) end)
      assert_receive {:fake_transport, :request, %{api: :api_versions, port: 9092}}, 2_000
      wait_for(fn -> map_size(:sys.get_state(client).parked) == 1 end)
      FakeTransport.respond(fake, :metadata, metadata_reply(host, [{@topic, [2, 2]}]))
      assert {:ok, _} = API.topics_metadata(client, [@topic])
      FakeTransport.release(fake, :api_versions)

      assert {:ok, _} = Task.await(fetch, 10_000)
      assert [%{port: 9093}] = requests(fake, :fetch)
    end

    test "on a connection that died before its park keep the connection a retry started" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      :ok = FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :api_versions)

      first = Task.async(fn -> API.fetch(client, @topic, 0, 0) end)
      assert_receive {:fake_transport, :request, %{api: :api_versions, port: 9092}}, 2_000
      wait_for(fn -> map_size(:sys.get_state(client).parked) == 1 end)
      %{pid: dead} = :sys.get_state(client).conns[{host, 9092, :fetch}]

      :ok = :sys.suspend(client)
      second = Task.async(fn -> API.fetch(client, @topic, 0, 0) end)
      wait_for(fn -> Process.info(client, :message_queue_len) == {:message_queue_len, 1} end)
      Process.exit(dead, :kill)
      wait_for(fn -> not Process.alive?(dead) end)
      FakeTransport.release(fake, :api_versions)
      :ok = :sys.resume(client)

      assert [{:ok, _}, {:ok, _}] = Task.await_many([first, second], 10_000)
      assert fake |> FakeTransport.connects() |> Enum.count(&(&1.role == :fetch)) == 2
    end

    test "end their fetch span with an exception when the client stops, and their callers exit" do
      %{fake: fake, client: client} = start_pinned()
      FakeTransport.hold(fake, :api_versions)
      watch_fetch_exceptions()

      fetch = Task.async(fn -> catch_exit(API.fetch(client, @topic, 0, 0)) end)
      wait_for(fn -> map_size(:sys.get_state(client).parked) == 1 end)
      GenServer.stop(client)

      assert_receive {:fetch_exception, :exit, :normal}
      assert {:normal, {GenServer, :call, _}} = Task.await(fetch, 10_000)
    end
  end

  describe "a first fetch to an unreachable broker" do
    test "errors in under two reconnect windows, as a call waiting in the client did" do
      %{client: client, gate: gate} = start_pinned(client: [connection: [reconnect_window: @window_ms]])
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
      wait_for(fn -> map_size(:sys.get_state(client).parked) == 1 end)
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

  defp watch_fetch_stops do
    forward_telemetry([[:kafka_ex, :fetch, :stop]], fn _event, _measurements, metadata ->
      {:fetch_stop, metadata.partition}
    end)
  end

  defp watch_fetch_exceptions do
    forward_telemetry([[:kafka_ex, :fetch, :exception]], fn _event, _measurements, metadata ->
      {:fetch_exception, metadata.kind, metadata.reason}
    end)
  end

  defp wait_for(condition) do
    KafkaEx.TestHelpers.wait_for_value(condition, & &1, 10, 300)
  end
end
