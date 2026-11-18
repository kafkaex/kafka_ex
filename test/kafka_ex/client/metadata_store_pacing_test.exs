defmodule KafkaEx.Client.MetadataStorePacingTest do
  use KafkaEx.TestSupport.StoreCase, async: false

  @moduletag capture_log: true

  @brokers [{1, 9092}, {2, 9093}]
  @scaled [min_interval: 10, backoff_base: 10, backoff_cap: 100, ladder_delay: 30]

  setup do
    start_fake(@brokers, topics: %{"pin" => [1, 2], "other" => [1]}, api_versions: versions())
  end

  describe "coalescing" do
    test "concurrent triggers for one key share one request", ctx do
      %{store: store} = start_store(ctx)
      await!(store)
      mark = count(ctx.fake, :metadata)
      FakeTransport.hold(ctx.fake, :metadata)

      tasks = for _ <- 1..20, do: Task.async(fn -> MetadataStore.refresh(store, refresh(["pin"], false), 5_000) end)
      await_parked(tasks, store)
      FakeTransport.release(ctx.fake, :metadata)

      assert Enum.all?(Task.await_many(tasks, 5_000), &match?({:ok, _}, &1))
      assert count(ctx.fake, :metadata) - mark == 1
    end

    test "an incompatible trigger starts its own round" do
      ctx = start_fake(@brokers, topics: %{"pin" => [1, 2]}, api_versions: Map.put(versions(), :metadata, 4..4))
      %{store: store} = start_store(ctx)
      await!(store)
      mark = count(ctx.fake, :metadata)
      FakeTransport.hold(ctx.fake, :metadata)

      tasks =
        for allow <- List.duplicate(false, 10) ++ List.duplicate(true, 10),
            do: Task.async(fn -> MetadataStore.refresh(store, refresh(["pin"], allow), 5_000) end)

      await_parked(tasks, store)
      FakeTransport.release(ctx.fake, :metadata)

      assert Enum.all?(Task.await_many(tasks, 5_000), &match?({:ok, _}, &1))
      flags = ctx.fake |> requests(:metadata) |> Enum.drop(mark) |> Enum.map(&:binary.last(&1.frame))
      assert Enum.sort(flags) == [0, 1]
    end

    test "a topic trigger joins an in-flight periodic refresh that already covers it", ctx do
      %{store: store, mark: mark} = held_periodic_round(ctx)

      task = Task.async(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end)
      await_parked([task], store)
      FakeTransport.release(ctx.fake, :metadata)

      assert {:ok, _} = Task.await(task, 5_000)
      assert count(ctx.fake, :metadata) - mark == 1
    end

    test "a trigger for an uncovered topic starts its own round", ctx do
      %{store: store, mark: mark} = held_periodic_round(ctx)

      task = Task.async(fn -> MetadataStore.refresh(store, refresh(["other"]), 5_000) end)
      await_parked([task], store)
      FakeTransport.release(ctx.fake, :metadata)

      assert {:ok, _} = Task.await(task, 5_000)
      assert count(ctx.fake, :metadata) - mark == 2
    end

    test "concurrent coordinator lookups for one group share one FindCoordinator", ctx do
      FakeTransport.respond(ctx.fake, :find_coordinator, coordinator_reply(ctx.host))
      %{store: store} = start_store(ctx)
      await!(store)
      FakeTransport.hold(ctx.fake, :find_coordinator)

      tasks = for _ <- 1..20, do: Task.async(fn -> MetadataStore.coordinator(store, "g", 5_000) end)
      await_parked(tasks, store)
      FakeTransport.release(ctx.fake, :find_coordinator)

      assert Task.await_many(tasks, 5_000) == List.duplicate(:ok, 20)
      assert count(ctx.fake, :find_coordinator) == 1
    end

    test "forget_coordinator/3 drops the row and the next coordinator/3 sends one FindCoordinator", ctx do
      FakeTransport.respond(ctx.fake, :find_coordinator, coordinator_reply(ctx.host))
      %{store: store} = start_store(ctx)
      %{table: tid} = await!(store)
      assert :ok = MetadataStore.coordinator(store, "g", 5_000)
      assert {:ok, 2, _broker} = Snapshot.select(tid, NodeSelector.consumer_group("g"), %{})

      assert :ok = MetadataStore.forget_coordinator(store, "g", 1_000)

      assert Snapshot.select(tid, NodeSelector.consumer_group("g"), %{}) == {:error, :no_such_consumer_group}
      assert :ok = MetadataStore.coordinator(store, "g", 5_000)
      assert count(ctx.fake, :find_coordinator) == 2
    end
  end

  describe "pacing" do
    test "the first trigger for a key is never delayed" do
      ctx = start_fake(@brokers, topics: %{"pin" => [1, 2], "slow" => [-1], "other" => [1]}, api_versions: versions())
      %{store: store} = start_store(ctx, store: [backoff_base: 10_000, backoff_cap: 10_000])
      await!(store)
      assert {:ok, _} = MetadataStore.refresh(store, refresh(["slow"]), 5_000)
      slow = park_refresh(store, "slow")
      mark = count(ctx.fake, :metadata)

      assert {:ok, _} = MetadataStore.refresh(store, refresh(["other"]), 1_000)
      assert count(ctx.fake, :metadata) - mark == 1
      assert {:ok, _} = Task.await(slow, 1_000)

      slow = park_refresh(store, "slow")
      assert {:ok, _} = MetadataStore.refresh(store, refresh(["other"]), 1_000)
      assert {:ok, _} = Task.await(slow, 1_000)
      assert count(ctx.fake, :metadata) - mark == 2
    end

    test "a scheduled trigger whose key backs off while it waits is re-armed to its new ready time" do
      ctx = start_fake(@brokers, topics: %{"pin" => [1, 2], "x" => [1]}, api_versions: versions())
      %{store: store} = start_store(ctx, store: [min_interval: 1_000, backoff_base: 3_000, backoff_cap: 3_000])
      await!(store)
      assert {:ok, _} = MetadataStore.refresh(store, refresh(["x"]), 5_000)
      waiting = park_refresh(store, "x")

      FakeTransport.hold(ctx.fake, :metadata)
      tracked = Task.async(fn -> MetadataStore.refresh(store, refresh([], false), 5_000) end)
      await_parked([tracked], store)
      leaderless = Task.async(fn -> MetadataStore.refresh(store, refresh(["x"], false), 5_000) end)
      await_parked([leaderless], store)
      FakeTransport.respond(ctx.fake, :metadata, metadata_reply(ctx.host, %{"pin" => [1, 2], "x" => [-1]}))
      FakeTransport.release(ctx.fake, :metadata)
      assert [{:ok, _}, {:ok, _}] = Task.await_many([tracked, leaderless], 5_000)

      assert Task.yield(waiting, 1_500) == nil
      assert {:ok, _} = Task.await(waiting, 5_000)
    end

    test "a coordinator outside the snapshot's brokers backs the lookup off", ctx do
      FakeTransport.respond(ctx.fake, :find_coordinator, coordinator_reply(ctx.host))
      %{store: store} = start_store(ctx, store: [coordinator_base: 10_000, coordinator_cap: 10_000])
      await!(store)
      assert :ok = MetadataStore.coordinator(store, "g", 1_000)
      assert :ok = MetadataStore.coordinator(store, "g", 1_000)

      FakeTransport.respond(ctx.fake, :find_coordinator, %{node_id: 7, host: ctx.host, port: 9099, error_message: nil})
      assert :ok = MetadataStore.coordinator(store, "g", 1_000)

      assert {:timeout, _call} = catch_exit(MetadataStore.coordinator(store, "g", 1_000))
    end

    test "the sweep drops backoff entries that no longer constrain a trigger" do
      ctx = start_fake(@brokers, topics: %{"pin" => [1, 2], "x" => [-1]}, api_versions: versions())
      %{store: store} = start_store(ctx, store: [min_interval: 300, backoff_base: 100, backoff_cap: 500])
      await!(store)

      assert {:ok, _} = MetadataStore.refresh(store, refresh(["pin", "x"]), 5_000)
      assert %{"pin" => %{failures: 0}, "x" => %{failures: 1}} = backoffs(store)

      refute_receive :never_sent, 500
      assert Map.keys(backoffs(store)) == ["x"]

      refute_receive :never_sent, 500
      assert backoffs(store) == %{}
    end

    test "a failed metadata round keeps its keys' failures" do
      ctx = start_fake(@brokers, topics: %{"pin" => [1, 2], "x" => [-1]}, api_versions: versions())
      %{store: store} = start_store(ctx, store: [min_interval: 10, backoff_base: 200, backoff_cap: 5_000])
      await!(store)
      assert {:ok, _} = MetadataStore.refresh(store, refresh(["x"]), 5_000)
      assert %{"x" => %{failures: 1}} = backoffs(store)

      FakeTransport.respond(ctx.fake, :metadata, :close)
      assert {:error, _reason} = MetadataStore.refresh(store, refresh(["x"]), 5_000)

      assert %{"x" => %{failures: 1}} = backoffs(store)
    end

    test "sequential paced triggers are each answered by their own request, never dropped" do
      ctx = start_fake(@brokers, topics: %{"pin" => [1, 2], "x" => [-1]}, api_versions: versions())
      %{store: store} = start_store(ctx, store: @scaled)
      await!(store)
      mark = count(ctx.fake, :metadata)

      for _ <- 1..3, do: assert({:ok, _} = MetadataStore.refresh(store, refresh(["x"]), 5_000))

      assert count(ctx.fake, :metadata) - mark == 3
    end

    test "a leaderless key under concurrent triggers stays within its schedule" do
      ctx = start_fake(@brokers, topics: %{"pin" => [1, 2], "x" => [-1]}, api_versions: versions())
      %{store: store} = start_store(ctx, store: @scaled)
      await!(store)

      sent =
        hammer(ctx.fake, :metadata, 10, 500, fn -> {:ok, _} = MetadataStore.refresh(store, refresh(["x"]), 5_000) end)

      assert sent <= 9
    end

    test "a missing topic is asked at most the schedule's count", ctx do
      %{store: store} = start_store(ctx, store: @scaled)
      await!(store)

      sent =
        hammer(ctx.fake, :metadata, 10, 500, fn ->
          {:ok, _} = MetadataStore.refresh(store, refresh(["absent"]), 5_000)
        end)

      assert sent <= 3 + 9
    end

    test "a down coordinator is looked up at most five times in 3 s", ctx do
      FakeTransport.respond(ctx.fake, :find_coordinator, %{
        error_code: 15,
        error_message: nil,
        node_id: -1,
        host: "",
        port: -1
      })

      %{store: store} = start_store(ctx)
      await!(store)

      sent =
        hammer(ctx.fake, :find_coordinator, 20, 3_000, fn ->
          {:error, _} = MetadataStore.coordinator(store, "g", 5_000)
        end)

      assert sent <= 5
    end
  end

  defp held_periodic_round(ctx) do
    %{store: store} = start_store(ctx, interval: 500)
    await!(store)
    mark = count(ctx.fake, :metadata)
    FakeTransport.subscribe(ctx.fake)
    FakeTransport.hold(ctx.fake, :metadata)
    assert_receive {:fake_transport, :request, %{api: :metadata, action: :hold}}, 2_000
    %{store: store, mark: mark}
  end

  defp coordinator_reply(host), do: %{node_id: 2, host: host, port: 9093, error_message: nil}

  defp park_refresh(store, topic) do
    task = Task.async(fn -> MetadataStore.refresh(store, refresh([topic]), 30_000) end)
    await_parked([task], store)
    task
  end

  defp backoffs(store), do: :sys.get_state(store) |> elem(1) |> Map.fetch!(:backoffs)

  # Callers parked in the call have sent it; the store handles them before the later table/2 call.
  defp await_parked(tasks, store) do
    wait_parked(tasks, now() + 2_000)
    {:ok, _tid} = MetadataStore.table(store, 1_000)
  end

  defp wait_parked(tasks, deadline) do
    cond do
      Enum.all?(tasks, &(Process.info(&1.pid, :status) == {:status, :waiting})) ->
        :ok

      now() < deadline ->
        refute_receive :never_sent, 1
        wait_parked(tasks, deadline)

      true ->
        flunk("the callers never parked in the store")
    end
  end

  # Counts the requests sent before the window closes: callers parked at its end are answered by one more round.
  defp hammer(fake, operation, callers, window, call) do
    mark = count(fake, operation)
    deadline = System.monotonic_time() + System.convert_time_unit(window, :millisecond, :native)
    counters = :counters.new(2, [])
    handler = "pacing-stamps-#{System.unique_integer([:positive])}"
    config = %{operation: operation, deadline: deadline, counters: counters}
    :telemetry.attach(handler, [:kafka_ex, :request, :start], &__MODULE__.stamp/4, config)
    on_exit(fn -> :telemetry.detach(handler) end)

    1..callers
    |> Enum.map(fn _ -> Task.async(fn -> until(deadline, call) end) end)
    |> Task.await_many(window + 5_000)

    assert :counters.get(counters, 2) == count(fake, operation) - mark, "a request span carried more than one request"
    :counters.get(counters, 1)
  end

  @doc false
  def stamp(_event, %{monotonic_time: at}, %{operation: operation}, %{operation: operation} = config) do
    :counters.add(config.counters, 2, 1)
    if at < config.deadline, do: :counters.add(config.counters, 1, 1)
  end

  def stamp(_event, _measurements, _metadata, _config), do: :ok

  defp until(deadline, call) do
    if System.monotonic_time() < deadline do
      call.()
      until(deadline, call)
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
