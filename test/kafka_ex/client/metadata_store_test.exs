defmodule KafkaEx.Client.MetadataStoreTest do
  use KafkaEx.TestSupport.StoreCase, async: false

  alias KafkaEx.Client.MetadataStore.Round
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic

  @quiet [backoff_base: 60_000, backoff_cap: 60_000]

  setup do
    start_fake([{1, 9092}, {2, 9093}], topics: %{"pin" => [1, 2]}, api_versions: versions())
  end

  describe "lifecycle and boot" do
    test "init does no network I/O", ctx do
      FakeTransport.respond(ctx.fake, :api_versions, :no_reply)

      %{store: store} = start_store(ctx, connection: [connect_timeout: 300])

      assert {:ok, _tid} = MetadataStore.table(store, 100)
    end

    test "boot: await_ready returns the table and the boot connection's versions, over exactly one :metadata connect",
         ctx do
      %{store: store} = start_store(ctx)

      assert %{table: tid, api_versions: versions} = await!(store)
      assert {:ok, [%{name: "pin"}]} = MetadataTable.topics(tid, ["pin"])
      assert Map.has_key?(versions, KafkaEx.Protocol.KayrockProtocol.api_key(:metadata))
      assert [%{role: :metadata}] = FakeTransport.connects(ctx.fake)
    end

    test "a boot where every bootstrap refuses answers await_ready with {:error, {:boot, _}} and the store keeps retrying",
         ctx do
      gate = PinGate.new()
      PinGate.close(gate)

      {%{store: store, supervisor: supervisor}, _log} = with_log(fn -> start_store(ctx, gate: gate) end)

      {result, _log} = with_log(fn -> MetadataStore.await_ready(store, 5_000) end)
      assert {:error, {:boot, _reason}} = result

      PinGate.open(gate)
      await!(store)
      assert ClientSupervisor.metadata_store(supervisor) == store
    end

    test "a boot whose Metadata fails after a connect is ready with the bootstrap brokers only", ctx do
      FakeTransport.respond(ctx.fake, :metadata, :close)
      %{store: store} = start_store(ctx)

      {result, _log} = with_log(fn -> MetadataStore.await_ready(store, 5_000) end)

      assert {:ok, %{table: tid}} = result
      assert MetadataTable.topics(tid, ["pin"]) == {:ok, []}
      assert Enum.sort(MetadataStore.candidates(store, 1_000)) == [{ctx.host, 9092}, {ctx.host, 9093}]
    end

    test "the periodic tick refreshes the tracked topics at the interval", ctx do
      %{store: store} = start_store(ctx, interval: 100)
      await!(store)
      FakeTransport.subscribe(ctx.fake)

      for _ <- 1..2 do
        assert_receive {:fake_transport, :request, %{api: :metadata} = request}, 1_000
        assert requested_topics(request) == MapSet.new(["pin"])
      end
    end

    test "a broker that closes a connection a refresh used is logged as that broker", ctx do
      %{store: store} = start_store(ctx, bootstrap: [9092])
      await!(store)
      assert {:ok, _} = MetadataStore.refresh(store, refresh(["pin"]), 5_000)
      %{node_id: node_id, port: port} = ctx.fake |> requests(:metadata) |> List.last()
      watch_close(ctx.host)

      {_, log} =
        with_log([level: :debug], fn ->
          FakeTransport.close(ctx.fake, {ctx.host, port})
          assert_receive {:pin_closed, %{port: ^port, reason: :remote_closed}}, 5_000
        end)

      assert log =~ "broker #{node_id} (#{ctx.host}:#{port}) closed connection"
      refute log =~ ~r/\] #{Regex.escape(ctx.host)}:#{port} closed connection/
    end
  end

  describe "degraded" do
    setup do
      watch_store_events()
    end

    test "a round with no reachable broker degrades; a failed load never emits degraded", ctx do
      gate = PinGate.new()
      PinGate.close(gate)
      {%{store: store}, _log} = with_log(fn -> start_store(ctx, gate: gate) end)

      {result, _log} = with_log(fn -> MetadataStore.await_ready(store, 5_000) end)
      assert {:error, {:boot, _reason}} = result
      refute_receive {:store_event, _event, _measurements, _metadata}, 200

      PinGate.open(gate)
      await!(store)
      PinGate.close(gate)
      FakeTransport.close(ctx.fake, {ctx.host, 9092})
      FakeTransport.close(ctx.fake, {ctx.host, 9093})

      {result, _log} = with_log(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end)
      assert result == {:error, :no_broker}
      assert_receive {:store_event, [:kafka_ex, :metadata_store, :degraded], _, %{reason: :no_broker}}, 1_000
    end
  end

  describe "lost refresh" do
    test "a refresh whose response never comes releases its key and fails over", ctx do
      with_request_timeout(200)
      %{store: store} = start_store(ctx, store: @quiet)
      await!(store)
      mark = length(non_handshake(ctx.fake))
      FakeTransport.hold(ctx.fake, :metadata)

      {result, _log} = with_log(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end)

      assert {:error, _reason} = result
      walk = ctx.fake |> non_handshake() |> Enum.drop(mark)
      assert Enum.frequencies_by(walk, &{&1.api, &1.node_id}) == %{{:metadata, 1} => 1, {:metadata, 2} => 1}

      FakeTransport.release(ctx.fake, :metadata)
      assert {:ok, _} = MetadataStore.refresh(store, refresh(["pin"]), 5_000)
    end

    test "a :metadata connection killed mid-refresh fails over to the next broker", ctx do
      %{store: store} = start_store(ctx, store: @quiet)
      await!(store)
      FakeTransport.subscribe(ctx.fake)
      FakeTransport.hold(ctx.fake, :metadata)
      [current | _] = MetadataStore.candidates(store, 1_000)
      {:ok, conn} = MetadataStore.connection(store, current, 1_000)

      task = Task.async(fn -> with_log(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end) end)
      assert_receive {:fake_transport, :request, %{api: :metadata, node_id: first}}, 1_000
      Process.exit(conn, :kill)
      assert_receive {:fake_transport, :request, %{api: :metadata, node_id: second}}, 2_000
      FakeTransport.release(ctx.fake, :metadata)

      assert {{:ok, _}, _log} = Task.await(task, 5_000)
      assert second != first
      assert count(ctx.fake, :metadata, second) == 1
    end

    test "a round killed by its backstop answers its waiters" do
      ctx = start_fake([{1, 9092}], topics: %{"pin" => [1]}, api_versions: versions())
      with_request_timeout(100)

      %{store: store} =
        start_store(ctx, bootstrap: [9092], connection: [connect_timeout: 100], store: [ladder_delay: 10] ++ @quiet)

      await!(store)
      block_rounds()

      {result, _log} = with_log(fn -> MetadataStore.refresh(store, refresh(["pin"]), 10_000) end)

      assert result == {:error, :round_timeout}
    end

    test "a round that dies answers its waiters with :round_down and the store survives", ctx do
      %{store: store, supervisor: supervisor} = start_store(ctx, store: @quiet)
      await!(store)
      on_round_request(fn -> Process.exit(self(), :kill) end)

      {result, _log} = with_log(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end)

      assert result == {:error, :round_down}
      assert ClientSupervisor.metadata_store(supervisor) == store
    end
  end

  describe "freshness through the store" do
    test "two overlapping refreshes that complete in reverse order leave the newer leader in place", ctx do
      %{store: store} = start_store(ctx)
      %{table: tid} = await!(store)
      FakeTransport.subscribe(ctx.fake)
      FakeTransport.hold(ctx.fake, :metadata)

      older = Task.async(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end)
      assert_receive {:fake_transport, :request, %{api: :metadata, action: :hold}}, 1_000
      seq = MetadataStore.next_seq(store, 1_000)

      :ok =
        MetadataStore.merge(
          store,
          seq,
          layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [2, 2]}).metadata,
          {:topics, ["pin"]},
          1_000
        )

      FakeTransport.release(ctx.fake, :metadata)

      assert {:ok, _} = Task.await(older, 5_000)
      assert {:ok, 2, _broker} = MetadataTable.select(tid, NodeSelector.topic_partition("pin", 0), %{})
    end

    test "a zero-broker response is ignored", ctx do
      %{store: store} = start_store(ctx, store: [ladder_delay: 10])
      %{table: tid} = await!(store)
      {:ok, counter} = MetadataTable.counter(tid)
      FakeTransport.respond(ctx.fake, :metadata, %{brokers: [], topics: []})

      {result, _log} = with_log(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end)

      assert {:ok, _} = result
      assert MetadataTable.counter(tid) == {:ok, counter}
    end

    test "a zero-broker response logs nothing about missing topics", ctx do
      %{store: store} = start_store(ctx, store: [ladder_delay: 10])
      await!(store)
      FakeTransport.respond(ctx.fake, :metadata, %{brokers: [], topics: []})

      {result, log} = with_log(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end)

      assert {:ok, _} = result
      refute log =~ "Kafka metadata unavailable"
    end

    test "a merge issued before a refresh that drops a topic cannot bring the topic back" do
      ctx = start_fake([{1, 9092}, {2, 9093}], topics: %{"pin" => [1, 2], "gone" => [1]}, api_versions: versions())
      %{store: store} = start_store(ctx, initial_topics: [])
      %{table: tid} = await!(store)
      with_gone = layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [1, 2], "gone" => [1]})
      without = layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [1, 2]})
      seq = MetadataStore.next_seq(store, 1_000)

      FakeTransport.respond(ctx.fake, :metadata, without.reply)
      assert {:ok, _} = MetadataStore.refresh(store, refresh([]), 5_000)
      :ok = MetadataStore.merge(store, seq, with_gone.metadata, {:topics, ["gone"]}, 1_000)

      assert MetadataTable.topics(tid, ["gone"]) == {:ok, []}
    end

    test "freshness entries older than every live seq are pruned, newer ones kept" do
      freshness = %{topics: %{"old" => 2, "edge" => 3, "new" => 4}, brokers: 4}

      assert MetadataStore.prune(freshness, 3) == %{topics: %{"edge" => 3, "new" => 4}, brokers: 4}
    end

    test "routing after several refresh rounds equals the ClusterMetadata merge and select_node" do
      ctx =
        start_fake([{1, 9092}, {2, 9093}, {2, 9094}], topics: %{"pin" => [1, 2]}, api_versions: versions())

      boot = layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [1, 2]})
      FakeTransport.respond(ctx.fake, :metadata, boot.reply)
      %{store: store} = start_store(ctx, store: [ladder_delay: 10])
      %{table: tid} = await!(store)

      rounds = [
        {["pin", "new"], layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [1, 2], "new" => [2]})},
        {["pin"], layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [1, 2]})},
        {["pin"], layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [2, 2]})},
        {["pin"], layout(ctx.host, [{1, 9092}, {2, 9094}], %{"pin" => [2, 2]})}
      ]

      assert_routes_match(tid, boot.metadata)

      Enum.reduce(rounds, boot.metadata, fn {topics, layout}, expected ->
        FakeTransport.respond(ctx.fake, :metadata, layout.reply)
        {result, _log} = with_log(fn -> MetadataStore.refresh(store, refresh(topics), 5_000) end)
        assert {:ok, _} = result
        {expected, _departed} = ClusterMetadata.merge_brokers(expected, layout.metadata)
        assert_routes_match(tid, expected)
        expected
      end)
    end
  end

  describe "connections" do
    test "the store's :metadata connections stop when the store dies", ctx do
      watch_close(ctx.host)
      %{store: store} = start_store(ctx)
      await!(store)
      monitor = Process.monitor(store)

      Process.exit(store, :kill)

      assert_receive {:DOWN, ^monitor, :process, ^store, :killed}, 1_000
      assert_receive {:pin_closed, %{reason: :shutdown, port: 9092}}, 2_000
    end

    test "connection/3 hands out a store-owned :metadata connection and reuses it", ctx do
      %{store: store} = start_store(ctx)
      await!(store)

      {:ok, conn} = MetadataStore.connection(store, {ctx.host, 9093}, 1_000)
      assert {:ok, ^conn} = MetadataStore.connection(store, {ctx.host, 9093}, 1_000)
      assert {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)

      assert [{9092, :metadata}, {9093, :metadata}] = Enum.map(FakeTransport.connects(ctx.fake), &{&1.port, &1.role})
    end

    test "while the connection supervisor restarts the store answers without a connection, and connects once it is back",
         ctx do
      %{store: store, supervisor: supervisor} = start_store(ctx, bootstrap: [9092], store: @quiet)
      await!(store)
      :ok = Supervisor.terminate_child(supervisor, :connections)

      {{connection, refreshed}, _log} =
        with_log(fn ->
          {MetadataStore.connection(store, {ctx.host, 9093}, 1_000),
           MetadataStore.refresh(store, refresh(["pin"]), 5_000)}
        end)

      assert connection == {:ok, nil}
      assert {:error, _not_connected} = refreshed
      assert ClientSupervisor.metadata_store(supervisor) == store

      {:ok, _supervisor} = Supervisor.restart_child(supervisor, :connections)
      assert {:ok, conn} = MetadataStore.connection(store, {ctx.host, 9093}, 1_000)
      assert is_pid(conn)
    end

    test "a boot inside the connection supervisor's restart window fails the boot and keeps the store", ctx do
      gate = PinGate.new()
      PinGate.close(gate)

      {%{store: store, supervisor: supervisor}, _log} =
        with_log(fn ->
          %{store: store} = started = start_store(ctx, gate: gate, store: [backoff_base: 500, backoff_cap: 500])
          {:error, {:boot, _reason}} = MetadataStore.await_ready(store, 5_000)
          started
        end)

      :ok = Supervisor.terminate_child(supervisor, :connections)

      {result, _log} = with_log(fn -> MetadataStore.await_ready(store, 5_000) end)

      assert result == {:error, {:boot, :no_broker}}
      assert ClientSupervisor.metadata_store(supervisor) == store
    end

    test "a connection that fails to start crashes the store", ctx do
      %{store: store} = start_store(ctx, bootstrap: [9092], store: @quiet)
      await!(store)
      monitor = Process.monitor(store)

      :sys.replace_state(store, fn {state, data} ->
        {state, put_in(data.connection.opts, initial_correlation_id: -1)}
      end)

      {exit, _log} = with_log(fn -> catch_exit(MetadataStore.connection(store, {ctx.host, 9093}, 1_000)) end)

      assert {{{:case_clause, {:error, _}}, [_ | _]}, _call} = exit
      assert_receive {:DOWN, ^monitor, :process, ^store, {{:case_clause, {:error, _}}, [_ | _]}}, 1_000
    end

    test "a broker that leaves the snapshot has its :metadata connection stopped with :metadata_update", ctx do
      watch_close(ctx.host)
      %{store: store} = start_store(ctx)
      await!(store)
      {:ok, conn} = MetadataStore.connection(store, {ctx.host, 9093}, 1_000)
      {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)
      monitor = Process.monitor(conn)
      FakeTransport.respond(ctx.fake, :metadata, layout(ctx.host, [{1, 9092}], %{"pin" => [1, 1]}).reply)

      assert {:ok, _} = MetadataStore.refresh(store, refresh(["pin"]), 5_000)

      assert_receive {:pin_closed, %{reason: :metadata_update, port: 9093}}, 2_000
      assert_receive {:DOWN, ^monitor, :process, ^conn, :normal}, 2_000
    end

    test "the client gets one swap message per applied snapshot and none for a coordinator lookup", ctx do
      FakeTransport.respond(ctx.fake, :find_coordinator, %{node_id: 2, host: ctx.host, port: 9093, error_message: nil})
      %{store: store} = start_store(ctx)
      %{table: tid} = await!(store)
      assert_receive {:kafka_ex_metadata_store, :swapped}, 1_000

      assert {:ok, _} = MetadataStore.refresh(store, refresh(["pin"]), 5_000)
      assert_receive {:kafka_ex_metadata_store, :swapped}, 1_000
      refute_received {:kafka_ex_metadata_store, :swapped}

      assert :ok = MetadataStore.coordinator(store, "g", 5_000)
      assert {:ok, 2, _broker} = MetadataTable.select(tid, NodeSelector.consumer_group("g"), %{})
      refute_receive {:kafka_ex_metadata_store, :swapped}, 200
    end
  end

  describe "rounds" do
    test "a load retries an ApiVersions parse error on the same address and boots", ctx do
      attempts = :atomics.new(1, [])
      ranges = %{18 => 0..0, 3 => 0..0, 10 => 1..1}

      FakeTransport.respond(ctx.fake, :api_versions, fn request ->
        if :atomics.add_get(attempts, 1, 1) <= 2 do
          {:frame, <<request.correlation_id + 1::32-signed>>}
        else
          FakeTransport.Wire.api_versions_fields(ranges)
        end
      end)

      {_ready, log} =
        with_log(fn ->
          %{store: store} = start_store(ctx, bootstrap: [9092])
          await!(store)
        end)

      assert log =~ "ApiVersions negotiation failed with :parse_error, retrying in 100ms (2 retries left)"
      assert log =~ "ApiVersions negotiation failed with :parse_error, retrying in 200ms (1 retries left)"
      assert count(ctx.fake, :api_versions, 1) == 3
    end

    test "a load round's bound budgets the ApiVersions retries; other rounds keep theirs" do
      spec = round_spec([{{"h", 1}, self()}, {{"h", 2}, self()}], ladder_delay: 300, connect_timeout: 1_000)

      assert Round.bound(spec) == 2 * (1_000 + 1_500 + 2_000 + 1_000) + 2 * 300 + 1_000
      assert Round.bound(%{spec | kind: :load}) - Round.bound(spec) == 2 * (3 * (1_000 + 1_500) + 100 + 200 + 400)
    end

    test "the newly-missing ladder runs only while another walk fits in the round's deadline", ctx do
      %{store: store} = start_store(ctx, store: @quiet)
      await!(store)
      reply = layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [1, 2]}).reply
      FakeTransport.respond(ctx.fake, :metadata, fn request -> if request.node_id == 1, do: :no_reply, else: reply end)

      candidates =
        for port <- [9092, 9093] do
          {:ok, conn} = MetadataStore.connection(store, {ctx.host, port}, 1_000)
          {{ctx.host, port}, conn}
        end

      spec =
        round_spec(candidates,
          request_opts: [topics: ["pin", "missing"], allow_auto_topic_creation: false],
          request_timeout: 200
        )

      now = System.monotonic_time(:millisecond)
      # Room for the ladder delay and margin after the ~200 ms walk, but not for a second such walk.
      tight = Map.put(spec, :deadline, now + 815)

      {result, _log} = with_log(fn -> Round.run(tight) end)
      assert {:ok, %{missing: ["missing"]}} = result
      assert count(ctx.fake, :metadata, 2) == 1

      ample = Map.put(spec, :deadline, System.monotonic_time(:millisecond) + Round.bound(spec) + 2_000)
      {result, _log} = with_log(fn -> Round.run(ample) end)
      assert {:ok, %{missing: ["missing"]}} = result
      assert count(ctx.fake, :metadata, 2) == 1 + 3
    end

    test "on a slow link the ladder still makes all three attempts its round's bound allows", ctx do
      %{store: store} = start_store(ctx, store: @quiet)
      await!(store)
      answered = :counters.new(1, [])
      %{reply: leaderless} = layout(ctx.host, [{1, 9092}], %{})
      leaderless = %{leaderless | topics: [%{error_code: 5, name: "fresh", is_internal: false, partitions: []}]}
      %{reply: led} = layout(ctx.host, [{1, 9092}], %{"fresh" => [1]})

      FakeTransport.respond(ctx.fake, :metadata, fn _request ->
        Process.sleep(300)
        :counters.add(answered, 1, 1)
        if :counters.get(answered, 1) <= 2, do: leaderless, else: led
      end)

      {:ok, conn} = MetadataStore.connection(store, {ctx.host, 9092}, 1_000)
      spec = round_spec([{{ctx.host, 9092}, conn}], request_opts: [topics: ["fresh"]], ladder_delay: 300)
      spec = %{spec | deadline: System.monotonic_time(:millisecond) + Round.bound(spec)}

      assert {:ok, %{missing: [], metadata: %{topics: %{"fresh" => _}}}} = Round.run(spec)
      assert :counters.get(answered, 1) == 3
    end

    test "one request span per walk, with the answering broker and its wire correlation id", ctx do
      with_request_timeout(200)
      %{store: store} = start_store(ctx, store: @quiet)
      await!(store)
      watch_request_spans()
      reply = layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [1, 2]}).reply
      FakeTransport.respond(ctx.fake, :metadata, fn request -> if request.node_id == 1, do: :no_reply, else: reply end)

      {result, _log} = with_log(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end)

      assert {:ok, _} = result
      assert_receive {:request_span, :start, _start}, 1_000
      assert_receive {:request_span, :stop, stop}, 1_000
      refute_receive {:request_span, _phase, _metadata}, 100
      [answered] = ctx.fake |> requests(:metadata) |> Enum.filter(&(&1.node_id == 2))
      assert stop.broker == %{node_id: 2, host: ctx.host, port: 9093, rack: nil}
      assert stop.correlation_id == answered.correlation_id
      assert stop.result == :ok
    end
  end

  describe "loading" do
    test "a trigger during :loading gets one load round's answer, then is re-run once the store is ready", ctx do
      gate = PinGate.new()
      PinGate.close(gate)
      {%{store: store}, _log} = with_log(fn -> start_store(ctx, gate: gate, store: @quiet) end)

      {result, _log} = with_log(fn -> MetadataStore.refresh(store, refresh(["pin"]), 5_000) end)
      assert {:error, {:loading, _reason}} = result

      PinGate.open(gate)
      assert {:ok, %{unknown: []}} = MetadataStore.refresh(store, refresh(["pin"]), 5_000)
      assert count(ctx.fake, :metadata) == 2
    end
  end

  describe "internal state" do
    test "format_status redacts the secrets fun", ctx do
      %{store: store} = start_store(ctx)
      await!(store)

      {:status, ^store, _module, [_dictionary, _running, _parent, _debug, items]} = :sys.get_status(store)
      [{~c"State", {_state, data}}] = items |> Keyword.get_values(:data) |> List.last()

      assert data.connection.secrets == :redacted
      refute inspect(data, limit: :infinity, printable_limit: :infinity) =~ "#Function"
    end

    test "the store prunes a tombstone once no live seq can need it" do
      ctx = start_fake([{1, 9092}, {2, 9093}], topics: %{"pin" => [1, 2], "gone" => [1]}, api_versions: versions())
      %{store: store} = start_store(ctx, initial_topics: [])
      await!(store)
      seq = MetadataStore.next_seq(store, 1_000)
      assert {:ok, _} = MetadataStore.refresh(store, refresh([]), 5_000)
      assert Map.has_key?(freshness(store).topics, "gone")

      :ok = MetadataStore.merge(store, seq, %ClusterMetadata{}, {:topics, ["pin"]}, 1_000)
      FakeTransport.respond(ctx.fake, :metadata, layout(ctx.host, [{1, 9092}, {2, 9093}], %{"pin" => [1, 2]}).reply)
      assert {:ok, _} = MetadataStore.refresh(store, refresh([]), 5_000)

      refute Map.has_key?(freshness(store).topics, "gone")
    end
  end

  describe "walk" do
    test "a candidate whose request cannot be built is skipped and the next one answers", ctx do
      ranges = %{18 => 0..0, 10 => 1..1}

      FakeTransport.respond(ctx.fake, :api_versions, fn request ->
        FakeTransport.Wire.api_versions_fields(if request.node_id == 1, do: ranges, else: Map.put(ranges, 3, 0..0))
      end)

      {%{table: tid}, log} =
        with_log(fn ->
          %{store: store} = start_store(ctx, bootstrap: [9092, 9093])
          await!(store)
        end)

      assert {:ok, [%{name: "pin"}]} = MetadataTable.topics(tid, ["pin"])
      assert count(ctx.fake, :metadata, 2) == 1
      assert length(String.split(log, "Unable to build metadata request.")) == 2
    end

    test "a walk where no candidate can build its request logs the build failure once", ctx do
      FakeTransport.respond(ctx.fake, :api_versions, FakeTransport.Wire.api_versions_fields(%{18 => 0..0, 10 => 1..1}))

      {%{table: tid}, log} =
        with_log(fn ->
          %{store: store} = start_store(ctx, bootstrap: [9092, 9093])
          await!(store)
        end)

      assert MetadataTable.topics(tid, ["pin"]) == {:ok, []}
      assert count(ctx.fake, :metadata) == 0
      assert length(String.split(log, "Unable to build metadata request.")) == 2
    end

    test "a round logs the builder's unsupported-API line once, however many candidates lack the API", ctx do
      FakeTransport.respond(ctx.fake, :api_versions, FakeTransport.Wire.api_versions_fields(%{18 => 0..0, 10 => 1..1}))

      {_ready, log} =
        with_log(fn ->
          %{store: store} = start_store(ctx, bootstrap: [9092, 9093])
          await!(store)
        end)

      assert [_once] = Regex.scan(~r/Kafka API metadata is not supported by the connected broker\./, log)
    end
  end

  defp round_spec(candidates, overrides) do
    Map.merge(
      %{
        kind: :metadata,
        candidates: candidates,
        brokers: %{},
        request_opts: [topics: [], allow_auto_topic_creation: false],
        known_missing: MapSet.new(),
        ladder_delay: 10,
        connect_timeout: 1_000,
        connect_slack: 1_500,
        request_timeout: 2_000,
        request_slack: 1_000,
        deadline: System.monotonic_time(:millisecond) + 60_000
      },
      Map.new(overrides)
    )
  end

  defp freshness(store), do: :sys.get_state(store) |> elem(1) |> Map.fetch!(:freshness)

  defp watch_request_spans do
    test = self()
    handler = "request-spans-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler,
      [[:kafka_ex, :request, :start], [:kafka_ex, :request, :stop]],
      fn [:kafka_ex, :request, phase], _measurements, metadata, _config ->
        send(test, {:request_span, phase, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp layout(host, brokers, topics) do
    reply = %{
      brokers: Enum.map(brokers, fn {node_id, port} -> %{node_id: node_id, host: host, port: port} end),
      topics:
        Enum.map(topics, fn {name, partitions} ->
          %{
            name: name,
            partitions:
              partitions
              |> Enum.with_index()
              |> Enum.map(fn {leader, index} ->
                %{
                  error_code: 0,
                  partition_index: index,
                  leader_id: leader,
                  replica_nodes: [leader],
                  isr_nodes: [leader]
                }
              end)
          }
        end)
    }

    metadata = %ClusterMetadata{
      brokers: Map.new(brokers, fn {node_id, port} -> {node_id, %Broker{node_id: node_id, host: host, port: port}} end),
      topics: Map.new(reply.topics, &{&1.name, Topic.from_topic_metadata(Map.put(&1, :is_internal, false))})
    }

    %{reply: reply, metadata: metadata}
  end

  defp assert_routes_match(tid, expected) do
    selectors =
      [NodeSelector.controller(), NodeSelector.node_id(1), NodeSelector.node_id(2)] ++
        for topic <- ["pin", "new"],
            partition <- 0..2,
            selector <- [&NodeSelector.topic_partition/2, &NodeSelector.topic_partition_replica/2] do
          selector.(topic, partition)
        end

    for selector <- selectors do
      today = outcome(fn -> select_broker_entry(expected, selector) end)
      assert outcome(fn -> MetadataTable.select(tid, selector, %{}) end) == today, inspect(selector)
    end
  end

  defp select_broker_entry(metadata, selector) do
    with {:ok, node_id} <- ClusterMetadata.select_node(metadata, selector) do
      case ClusterMetadata.broker_by_node_id(metadata, node_id) do
        nil -> {:error, :broker_not_found}
        broker -> {:ok, node_id, broker}
      end
    end
  end

  defp outcome(fun) do
    fun.()
  rescue
    exception -> {:raised, exception.__struct__}
  end

  defp block_rounds, do: on_round_request(fn -> receive(do: (:never -> :ok)) end)

  defp on_round_request(fun) do
    handler = "round-request-#{System.unique_integer([:positive])}"
    :telemetry.attach(handler, [:kafka_ex, :request, :start], fn _, _, _, _ -> fun.() end, nil)
    on_exit(fn -> :telemetry.detach(handler) end)
  end
end
