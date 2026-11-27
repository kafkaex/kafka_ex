defmodule KafkaEx.Client.ParallelSetupTest do
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag capture_log: true

  @handshake_ms 400
  @short_handshake_ms 100
  @min_capped_backoff_ms 400
  @window_ms 1_500
  # Far below the call's own deadline, so only the crash, not that deadline, can answer it.
  @crash_answered_within_ms 5_000
  @partitions 40
  @all_partitions_within_ms 1_000

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

    test "to one broker each run their own handshake, which one release lets all finish" do
      %{fake: fake, client: client, host: host} = start_pinned(topics: %{@topic => [1, 1]})
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      :ok = FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :api_versions)

      fetches = for partition <- [0, 1], do: Task.async(fn -> API.fetch(client, @topic, partition, 0) end)

      for _handshake <- 1..2 do
        assert_receive {:fake_transport, :request, %{api: :api_versions, port: 9092, host: ^host}}, 2_000
      end

      FakeTransport.release(fake, :api_versions)
      assert [{:ok, _}, {:ok, _}] = Task.await_many(fetches, 10_000)
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

  describe "fetch connections to one broker" do
    test "all reconnect at once after the broker drops them" do
      %{fake: fake, client: client, host: host} = start_pinned(topics: %{@topic => List.duplicate(1, @partitions)})
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      {0, _ms} = fetch_all(client)

      FakeTransport.close(fake, {host, 9092})

      {errors, ms} = fetch_all(client)
      assert errors == 0
      assert ms < @all_partitions_within_ms
    end

    test "all connect after the first one when connects are slow" do
      %{fake: fake, client: client} = start_pinned(topics: %{@topic => List.duplicate(1, @partitions)})
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      FakeTransport.delay_connects(fake, @handshake_ms)

      {errors, ms} = fetch_all(client)
      assert errors == 0
      assert ms < 2 * @handshake_ms + @all_partitions_within_ms
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
      # A closed gate whose slot is free after four failures, so the next claim reserves a capped slot.
      :ets.insert(:sys.get_state(client).reconnect_backoff, {{host, 9092}, System.monotonic_time(:millisecond), 4})
      FakeTransport.delay_connects(fake, @short_handshake_ms)

      started = System.monotonic_time(:millisecond)
      fetch = Task.async(fn -> API.fetch(client, @topic, 0, 0) end)
      wait_for(fn -> :ets.member(:sys.get_state(client).conns, {host, 9092, {:fetch, @topic, 0}}) end)
      assert {:ok, _} = API.produce(client, @topic, 0, [%{value: "v"}])
      assert {:ok, _} = Task.await(fetch, 10_000)

      assert System.monotonic_time(:millisecond) - started < @min_capped_backoff_ms + @short_handshake_ms
    end
  end

  defp fetch_all(client) do
    started = System.monotonic_time(:millisecond)

    fetches =
      for p <- 0..(@partitions - 1), do: Task.async(fn -> API.fetch(client, @topic, p, 0, max_wait_time: 10) end)

    errors = fetches |> Task.await_many(30_000) |> Enum.count(&match?({:error, _}, &1))
    {errors, System.monotonic_time(:millisecond) - started}
  end

  defp wait_for(condition) do
    KafkaEx.TestHelpers.wait_for_value(condition, & &1, 10, 300)
  end
end
