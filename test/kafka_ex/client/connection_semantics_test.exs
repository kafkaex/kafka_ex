defmodule KafkaEx.Client.ConnectionSemanticsTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Client.MetadataStore
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.TestSupport.FakeTransport.Wire

  @moduletag pin_set: :invariant
  @moduletag capture_log: true

  @sasl_authentication_failed 58
  @pacing_base_ms 100
  @pacing_cap_ms 500
  @idle_timeout 300
  # Covers the ms-truncated claim clock and a Connection preempted between claim and its :start event.
  @gap_slack_ms 5

  describe "a Connection killed mid-request" do
    setup do
      start_pinned()
    end

    test "fails a produce with :timeout and writes it once", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :produce, produce_reply())

      {result, _killed} = kill_mid_request(fake, :produce, fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)

      assert result == {:error, :timeout}
      assert count(fake, :produce) == 1
      assert Process.alive?(client)
    end

    test "fails a JoinGroup with :timeout and writes it once", %{fake: fake, client: client} do
      join = fn ->
        API.join_group(client, "g", "", topics: [@topic], session_timeout: 10_000, rebalance_timeout: 10_000)
      end

      {result, _killed} = kill_mid_request(fake, :join_group, join)

      assert result == {:error, :timeout}
      assert count(fake, :join_group) == 1
      assert Process.alive?(client)
    end

    test "fails a KIP-848 heartbeat of a member with :timeout and writes it once", %{fake: fake, client: client} do
      heartbeat = fn -> API.consumer_group_heartbeat(client, "g", member_id: "m", member_epoch: 3) end

      {result, _killed} = kill_mid_request(fake, :consumer_group_heartbeat, heartbeat)

      assert result == {:error, :timeout}
      assert count(fake, :consumer_group_heartbeat) == 1
      assert Process.alive?(client)
    end

    test "resends a fetch on a new connection", %{fake: fake, client: client} do
      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})

      {result, killed} = kill_mid_request(fake, :fetch, fn -> API.fetch(client, @topic, 0, 0, max_wait_time: 10) end)

      assert {:ok, _fetch} = result
      assert count(fake, :fetch) >= 2
      assert %{conn: resent_on} = fake |> requests(:fetch) |> List.last()
      assert resent_on != killed.conn
      assert owner(fake, resent_on) != killed.pid
      assert Process.alive?(client)
    end
  end

  describe "a peer close" do
    test "after the write keeps the transport atom" do
      %{fake: fake, client: client, host: host} = start_pinned()
      FakeTransport.respond(fake, :produce, produce_reply())
      FakeTransport.hold(fake, :produce)
      :ok = FakeTransport.subscribe(fake)

      produce = Task.async(fn -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
      assert_receive {:fake_transport, :request, %{api: :produce}}, 5_000
      FakeTransport.close(fake, {host, 9092})

      assert Task.await(produce, 10_000) == {:error, :closed}
      assert count(fake, :produce) == 1
    end
  end

  test "a produce held past its request deadline gets :timeout, then liveness closes its connection, without a resend" do
    %{fake: fake, client: client, host: host} = start_pinned(client: [connection: [liveness_slack: 100]])
    with_request_timeout(100)
    watch_close(host)
    FakeTransport.respond(fake, :produce, produce_reply())
    FakeTransport.hold(fake, :produce)

    assert API.produce(client, @topic, 0, [%{value: "v"}]) == {:error, :timeout}
    assert_receive {:pin_closed, %{port: 9092, reason: :liveness}}, 2_000
    assert count(fake, :produce) == 1
  end

  describe "pacing with 100 concurrent callers" do
    test "spaces connect attempts that fail authentication" do
      %{fake: fake, client: client, host: host} = start_paced([auth: oauthbearer(fn -> {:ok, "t"} end)], window: 400)

      FakeTransport.respond(fake, :sasl_authenticate, %{
        error_code: @sasl_authentication_failed,
        error_message: "denied",
        auth_bytes: ""
      })

      assert_paced(client, {host, 9092}, fn _i -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
    end

    test "spaces connect attempts whose OAUTHBEARER token the broker refuses" do
      %{fake: fake, client: client, host: host} = start_paced([auth: oauthbearer(fn -> {:ok, "t"} end)], window: 400)
      FakeTransport.respond(fake, :sasl_authenticate, &FakeTransport.oauthbearer_refusal/1)
      # Kafka refuses any request but the %x01 after an error challenge; a close stands in for that.
      FakeTransport.respond(fake, :produce, :close)

      assert_paced(client, {host, 9092}, fn _i -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
    end

    test "spaces connect attempts whose handshake crashes the connection" do
      raising = :atomics.new(1, [])
      provider = fn -> if :atomics.get(raising, 1) == 1, do: raise("token provider down"), else: {:ok, "t"} end
      %{client: client, host: host} = start_paced(auth: oauthbearer(provider))
      :atomics.put(raising, 1, 1)

      assert_paced(client, {host, 9092}, fn _i -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
    end

    test "spaces connect attempts the broker accepts and closes after ApiVersions" do
      %{fake: fake, client: client, host: host} = start_paced([auth: oauthbearer(fn -> {:ok, "t"} end)], window: 400)
      FakeTransport.respond(fake, :sasl_handshake, :close)

      assert_paced(client, {host, 9092}, fn _i -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
    end

    test "spaces connect attempts of every role to an unreachable broker" do
      %{client: client, host: host, gate: gate} =
        start_paced([], topics: %{"wide" => List.duplicate(2, 20), @topic => [2, 2]})

      PinGate.close(gate)

      assert_paced(client, {host, 9093}, fn i ->
        case rem(i, 22) do
          20 -> API.produce(client, @topic, 0, [%{value: "v"}])
          21 -> API.heartbeat(client, "g", "m", 1)
          partition -> API.fetch(client, "wide", partition, 0, max_wait_time: 10)
        end
      end)

      roles = for {{^host, 9093, role}, _entry} <- :sys.get_state(client).conns, do: role
      assert Enum.sort(roles) == [:coordinator, :data, :fetch]
    end
  end

  test "a request after the backoff interval connects with exactly one attempt" do
    %{fake: fake, client: client, host: host, gate: gate} = start_pinned(client: [connection: [reconnect_window: 200]])
    FakeTransport.respond(fake, :produce, produce_reply())
    watch_close(host)
    drop_connection(fake, host)
    PinGate.close(gate)
    {:error, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    PinGate.open(gate)
    [{_address, next_at, _failures}] = :ets.lookup(:sys.get_state(client).pacing, {host, 9092})
    refute_receive :backoff_elapsed, max(next_at - System.monotonic_time(:millisecond), 0) + 1
    watch_attempts({host, 9092})

    assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 0, [%{value: "v"}])
    assert length(attempts()) == 1
  end

  describe "after a leader move, the old fetch connection stops within the idle timeout" do
    test "when the old leader is dead" do
      %{fake: fake, client: client, host: host, gate: gate} =
        start_pinned(client: [connection: [idle_timeout: @idle_timeout]])

      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      assert {:ok, _} = API.fetch(client, @topic, 0, 0, max_wait_time: 10)
      {old, metadata} = watch_old_connections(client, host)

      move_leader(fake, client, host)
      assert {:ok, _} = API.fetch(client, @topic, 0, 0, max_wait_time: 10)
      assert_new_fetch_connection(fake)
      PinGate.close(gate)
      FakeTransport.close(fake, {host, 9092})

      assert_stops_alone(old, metadata)
    end

    test "when the old connection holds an abandoned id" do
      %{fake: fake, client: client, host: host} =
        start_pinned(client: [connection: [idle_timeout: @idle_timeout, liveness_slack: 200]])

      FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
      assert {:ok, _} = API.fetch(client, @topic, 0, 0, max_wait_time: 10)
      {old, metadata} = watch_old_connections(client, host)

      FakeTransport.respond(fake, :fetch, fn request ->
        if request.port == 9092, do: :no_reply, else: {:frame, fetch_frame(request, -1)}
      end)

      assert {:error, :timeout} = API.fetch(client, @topic, 0, 0, max_wait_time: 10, network_timeout: 100)
      move_leader(fake, client, host)
      assert {:ok, _} = API.fetch(client, @topic, 0, 0, max_wait_time: 10)
      assert_new_fetch_connection(fake)

      assert_stops_alone(old, metadata)
    end
  end

  test "a broker that leaves the snapshot has its connections stopped" do
    %{fake: fake, client: client, host: host} = start_pinned()
    watch_close(host)
    FakeTransport.respond(fake, :produce, produce_reply())
    FakeTransport.respond(fake, :fetch, &{:frame, fetch_frame(&1, -1)})
    FakeTransport.respond(fake, :heartbeat, %{error_code: 0})
    {:ok, _} = API.produce(client, @topic, 1, [%{value: "v"}])
    {:ok, _} = API.fetch(client, @topic, 1, 0, max_wait_time: 10)
    {:ok, _} = API.heartbeat(client, "g", "m", 1)

    departing = for {{_host, 9093, _role}, %{pid: pid}} <- :sys.get_state(client).conns, do: pid
    assert length(departing) == 3
    monitored = Enum.map(departing, &{&1, Process.monitor(&1)})

    FakeTransport.respond(fake, :metadata, Wire.metadata_fields([{1, host, 9092}], %{@topic => [1, 1]}))
    assert {:ok, _} = API.topics_metadata(client, [@topic])

    for {pid, ref} <- monitored, do: assert_receive({:DOWN, ^ref, :process, ^pid, :normal}, 5_000)
    for _pid <- departing, do: assert_received({:pin_closed, %{port: 9093, reason: :metadata_update}})
    refute_received {:pin_closed, _}
  end

  defp kill_mid_request(fake, api, call) do
    FakeTransport.hold(fake, api)
    :ok = FakeTransport.subscribe(fake)
    caller = Task.async(call)

    assert_receive {:fake_transport, :request, %{api: ^api, conn: conn}}, 5_000
    pid = owner(fake, conn)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 5_000
    assert_receive {:fake_transport, :closed, ^conn}, 5_000
    FakeTransport.release(fake, api)

    {Task.await(caller, 10_000), %{conn: conn, pid: pid}}
  end

  defp owner(fake, conn) do
    %{owner: owner} = fake |> FakeTransport.connections() |> Enum.find(&(&1.id == conn))
    owner
  end

  defp oauthbearer(provider),
    do: AuthConfig.new(%{mechanism: :oauthbearer, mechanism_opts: %{token_provider: provider}})

  # stable_period 0 resets boot failures at once; a window past the second backoff keeps an outage unspent.
  defp start_paced(client_opts, opts \\ []) do
    {window, opts} = Keyword.pop(opts, :window, 25)
    connection = [reconnect_window: window, stable_period: 0]
    pinned = start_pinned([client: [connection: connection] ++ client_opts] ++ opts)
    _ = :sys.get_state(metadata_connection(pinned.client, pinned.host))
    pinned
  end

  defp assert_paced(client, address, call) do
    failures = failures(client, address)
    watch_attempts(address)

    1..100 |> Enum.map(fn i -> Task.async(fn -> call.(i) end) end) |> Enum.each(&Task.await(&1, 30_000))
    finished = System.monotonic_time()

    assert [first, _, _ | _] = attempts = attempts()

    gaps =
      attempts |> Enum.zip(tl(attempts)) |> Enum.map(fn {earlier, later} -> to_ms(later - earlier) end)

    for {gap, n} <- Enum.zip(gaps, Stream.iterate(failures + 1, &(&1 + 1))) do
      assert gap >= lower_bound(n) - @gap_slack_ms, "gap #{gap} ms after attempt #{n} is below #{lower_bound(n)} ms"
    end

    assert length(attempts) <= fitting(to_ms(finished - first), failures + 1) + 1
  end

  defp failures(client, address) do
    case :ets.lookup(:sys.get_state(client).pacing, address) do
      [{^address, _next_at, failures}] -> failures
      [] -> 0
    end
  end

  defp lower_bound(n), do: min(0.75 * @pacing_base_ms * Integer.pow(2, n - 1), 0.8 * @pacing_cap_ms)

  defp fitting(duration, n, elapsed \\ 0, attempts \\ 1) do
    elapsed = elapsed + lower_bound(n)
    if elapsed <= duration, do: fitting(duration, n + 1, elapsed, attempts + 1), else: attempts
  end

  defp to_ms(native), do: System.convert_time_unit(native, :native, :microsecond) / 1_000

  defp watch_attempts({host, port}) do
    test = self()
    handler = "attempts-#{host}-#{port}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :connection, :start],
      fn _event, %{monotonic_time: at}, metadata, _config ->
        if metadata.host == host and metadata.port == port, do: send(test, {:attempt, at})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp attempts(collected \\ []) do
    receive do
      {:attempt, at} -> attempts([at | collected])
    after
      0 -> Enum.sort(collected)
    end
  end

  defp watch_old_connections(client, host) do
    conns = :sys.get_state(client).conns
    old = conns |> Map.fetch!({host, 9092, :fetch}) |> Map.fetch!(:pid)
    metadata = metadata_connection(client, host)
    {{old, Process.monitor(old)}, {metadata, Process.monitor(metadata)}}
  end

  # The metadata store owns the :metadata connections.
  defp metadata_connection(client, host) do
    {:ok, pid} = MetadataStore.connection(:sys.get_state(client).store.pid, {host, 9092}, 1_000)
    pid
  end

  defp move_leader(fake, client, host) do
    FakeTransport.respond(fake, :metadata, metadata_reply(host, [{@topic, [2, 2]}]))
    assert {:ok, _} = API.topics_metadata(client, [@topic])
  end

  defp assert_new_fetch_connection(fake) do
    assert %{port: 9093} =
             fake |> FakeTransport.connects() |> Enum.filter(&(&1.role == :fetch)) |> List.last()
  end

  # The metadata connection on the same broker must outlive it: idling out is for fetch roles only.
  defp assert_stops_alone({old, old_ref}, {metadata, metadata_ref}) do
    assert_receive {:DOWN, ^old_ref, :process, ^old, :normal}, round(@idle_timeout * 1.2) + 500
    refute_receive {:DOWN, ^metadata_ref, :process, ^metadata, _reason}, round(@idle_timeout * 1.2) + 100
  end
end
