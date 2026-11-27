defmodule KafkaEx.Client.ConnectionSemanticsTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.MetadataStore
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.TestSupport.FakeTransport.Wire

  @moduletag pin_set: :invariant
  @moduletag capture_log: true

  @sasl_authentication_failed 58
  @backoff_base_ms 100
  @backoff_cap_ms 500
  @idle_timeout 300
  # Covers the ms-truncated claim clock and a connection preempted between claim and its :start event.
  @gap_slack_ms 5

  describe "a connection killed mid-request" do
    setup do
      start_pinned()
    end

    for {name, api, call} <- [
          {"produce", :produce, quote(do: API.produce(var!(client), @topic, 0, [%{value: "v"}]))},
          {"JoinGroup", :join_group,
           quote do
             API.join_group(var!(client), "g", "",
               topics: [@topic],
               session_timeout: 10_000,
               rebalance_timeout: 10_000
             )
           end},
          {"KIP-848 heartbeat of a member", :consumer_group_heartbeat,
           quote(do: API.consumer_group_heartbeat(var!(client), "g", member_id: "m", member_epoch: 3))}
        ] do
      test "fails a #{name} with :timeout and writes it once", %{fake: fake, client: client} do
        var!(client) = client

        {result, _killed} = kill_mid_request(fake, unquote(api), fn -> unquote(call) end)

        assert result == {:error, :timeout}
        assert count(fake, unquote(api)) == 1
        assert Process.alive?(client)
      end
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

  test "a produce held past its request deadline gets :timeout, then liveness closes its connection, without a resend" do
    %{fake: fake, client: client, host: host} = start_pinned(connection: [liveness_slack: 100])
    with_request_timeout(100)
    watch_close(host)
    FakeTransport.respond(fake, :produce, produce_reply())
    FakeTransport.hold(fake, :produce)

    assert API.produce(client, @topic, 0, [%{value: "v"}]) == {:error, :timeout}
    assert_receive {:pin_closed, %{port: 9092, reason: :liveness}}, 2_000
    assert count(fake, :produce) == 1
  end

  describe "reconnect backoff with 100 concurrent callers" do
    for {name, kind} <- [
          {"that fail authentication", :auth_failed},
          {"whose OAUTHBEARER token the broker refuses", :token_refused},
          {"the broker accepts and closes after ApiVersions", :handshake_closed}
        ] do
      test "spaces connect attempts #{name}" do
        %{fake: fake, client: client, host: host} = start_for_backoff([auth: oauthbearer()], window: 400)
        script(fake, unquote(kind))

        assert_backoff(client, {host, 9092}, fn _i -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
      end
    end

    # One request at a time, so each caller meets its own connect attempt as when the client served them in turn.
    test "spaces connect attempts whose handshake crashes the connection" do
      raising = :atomics.new(1, [])
      provider = fn -> if :atomics.get(raising, 1) == 1, do: raise("token provider down"), else: {:ok, "t"} end
      %{client: client, host: host} = start_for_backoff(auth: oauthbearer(provider), request_concurrency: :serial)
      :atomics.put(raising, 1, 1)

      assert_backoff(client, {host, 9092}, fn _i -> API.produce(client, @topic, 0, [%{value: "v"}]) end)
    end
  end

  # Serial callers wait out each other's retry backoff, so one caller per partition, produce and heartbeat suffices.
  test "reconnect backoff spaces connect attempts of every role to an unreachable broker" do
    %{client: client, host: host, gate: gate} =
      start_for_backoff([request_concurrency: :serial], topics: %{"wide" => List.duplicate(2, 20), @topic => [2, 2]})

    PinGate.close(gate)

    call = fn i ->
      case rem(i, 22) do
        20 -> API.produce(client, @topic, 0, [%{value: "v"}])
        21 -> API.heartbeat(client, "g", "m", 1)
        partition -> API.fetch(client, "wide", partition, 0, max_wait_time: 10)
      end
    end

    assert_backoff(client, {host, 9093}, call, 22)

    roles = for {{^host, 9093, role}, _pid} <- :ets.tab2list(:sys.get_state(client).conns), do: role
    assert Enum.sort(roles) == [:coordinator, :data | for(partition <- 0..19, do: {:fetch, "wide", partition})]
  end

  test "a request after the backoff interval connects with exactly one attempt" do
    %{fake: fake, client: client, host: host, gate: gate} = start_pinned(connection: [reconnect_window: 200])
    FakeTransport.respond(fake, :produce, produce_reply())
    watch_close(host)
    drop_connection(fake, host)
    PinGate.close(gate)
    {:error, _} = API.produce(client, @topic, 0, [%{value: "v"}])
    PinGate.open(gate)
    [{_address, next_at, _failures}] = :ets.lookup(:sys.get_state(client).reconnect_backoff, {host, 9092})
    refute_receive :backoff_elapsed, max(next_at - System.monotonic_time(:millisecond), 0) + 1
    watch_attempts({host, 9092})

    assert {:ok, %RecordMetadata{base_offset: 7}} = API.produce(client, @topic, 0, [%{value: "v"}])
    assert length(attempts()) == 1
  end

  describe "after a leader move, the old fetch connection stops within the idle timeout" do
    test "when the old leader is dead" do
      %{fake: fake, client: client, host: host, gate: gate} =
        start_pinned(connection: [idle_timeout: @idle_timeout])

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
        start_pinned(connection: [idle_timeout: @idle_timeout, liveness_slack: 200])

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

    departing = for {{_host, 9093, _role}, pid} <- :ets.tab2list(:sys.get_state(client).conns), do: pid
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

  defp script(fake, :auth_failed) do
    FakeTransport.respond(fake, :sasl_authenticate, %{
      error_code: @sasl_authentication_failed,
      error_message: "denied",
      auth_bytes: ""
    })
  end

  defp script(fake, :token_refused) do
    FakeTransport.respond(fake, :sasl_authenticate, &FakeTransport.oauthbearer_refusal/1)
    # Kafka refuses any request but the %x01 after an error challenge; a close stands in for that.
    FakeTransport.respond(fake, :produce, :close)
  end

  defp script(fake, :handshake_closed), do: FakeTransport.respond(fake, :sasl_handshake, :close)

  # stable_period 0 resets boot failures at once; a window past the second backoff keeps an outage unspent.
  defp start_for_backoff(client_opts, opts \\ []) do
    {window, opts} = Keyword.pop(opts, :window, 25)
    connection = [reconnect_window: window, stable_period: 0]
    pinned = start_pinned([client: client_opts, connection: connection] ++ opts)
    _ = :sys.get_state(metadata_connection(pinned.client, pinned.host))
    pinned
  end

  defp assert_backoff(client, address, call, callers \\ 100) do
    failures = failures(client, address)
    watch_attempts(address)

    1..callers |> Enum.map(fn i -> Task.async(fn -> call.(i) end) end) |> Enum.each(&Task.await(&1, 30_000))
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
    case :ets.lookup(:sys.get_state(client).reconnect_backoff, address) do
      [{^address, _next_at, failures}] -> failures
      [] -> 0
    end
  end

  defp lower_bound(n), do: min(0.8 * @backoff_base_ms * Integer.pow(2, n - 1), 0.8 * @backoff_cap_ms)

  defp fitting(duration, n, elapsed \\ 0, attempts \\ 1) do
    elapsed = elapsed + lower_bound(n)
    if elapsed <= duration, do: fitting(duration, n + 1, elapsed, attempts + 1), else: attempts
  end

  defp to_ms(native), do: System.convert_time_unit(native, :native, :microsecond) / 1_000

  defp watch_attempts({host, port}) do
    forward_telemetry([[:kafka_ex, :connection, :start]], fn _event, %{monotonic_time: at}, metadata ->
      if metadata.host == host and metadata.port == port, do: {:attempt, at}
    end)
  end

  defp attempts(collected \\ []) do
    receive do
      {:attempt, at} -> attempts([at | collected])
    after
      0 -> Enum.sort(collected)
    end
  end

  defp watch_old_connections(client, host) do
    [{_key, old}] = :ets.lookup(:sys.get_state(client).conns, {host, 9092, {:fetch, @topic, 0}})
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
             fake
             |> FakeTransport.connects()
             |> Enum.filter(&match?({:fetch, _topic, _partition}, &1.role))
             |> List.last()
  end

  # The metadata connection on the same broker must outlive it: idling out is for fetch roles only.
  defp assert_stops_alone({old, old_ref}, {metadata, metadata_ref}) do
    assert_receive {:DOWN, ^old_ref, :process, ^old, :normal}, round(@idle_timeout * 1.2) + 500
    refute_receive {:DOWN, ^metadata_ref, :process, ^metadata, _reason}, round(@idle_timeout * 1.2) + 100
  end
end
