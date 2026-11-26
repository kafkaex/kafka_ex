defmodule KafkaEx.Client.ConnectionStateMachineTest do
  use KafkaEx.TestSupport.ConnectionCase, async: false

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Client.Error
  alias KafkaEx.TestSupport.FakeTransport.Wire
  alias KafkaEx.TestSupport.LeakProbe
  alias KafkaEx.TestSupport.TLSFixtures

  # Claims and attempt events read the ms clock at different points; a slot's timer fires late under load.
  @clock_slack_ms 5
  @timer_slack_ms 50
  @slow_connect_ms 150
  @span_events for name <- [:connection, :auth], phase <- [:start, :stop, :exception], do: [:kafka_ex, name, phase]

  setup_all do
    {:ok, tls: TLSFixtures.chain()}
  end

  defp plain do
    AuthConfig.new(%{mechanism: :plain, username: "conn-secret-user", password: "conn-secret-password"})
  end

  defp attach_spans do
    handler = "conn-spans-#{System.unique_integer([:positive])}"
    :telemetry.attach_many(handler, @span_events, &__MODULE__.forward_span/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)
  end

  def forward_span([:kafka_ex, name, phase], _measurements, metadata, test) do
    send(test, {:span, self(), name, phase, metadata})
  end

  defp drain_spans(acc \\ []) do
    receive do
      {:span, pid, name, phase, metadata} -> drain_spans([{pid, name, phase, metadata} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp watched_fake(_context) do
    %{fake: fake, host: host} = start_fake()
    watch_close(host)
    %{fake: fake, host: host}
  end

  defp keys(metadata), do: metadata |> Map.delete(:telemetry_span_context) |> Map.keys() |> Enum.sort()

  defp drop(fake, host) do
    FakeTransport.close(fake, {host, port()})
    assert_receive {:closed, %{reason: :remote_closed}}, 2_000
  end

  defp write_until_failure(conn, request, left, started \\ nil)

  defp write_until_failure(_conn, _request, 0, _started), do: flunk("the writes never blocked")

  defp write_until_failure(conn, request, left, nil), do: write_until_failure(conn, request, left, now())

  defp write_until_failure(conn, request, left, started) do
    case dispatch(conn, request, reply?: false) do
      {:ok, :no_response, _meta} -> write_until_failure(conn, request, left - 1, started)
      {:error, reason, meta} -> {{:error, reason, meta}, now() - started}
    end
  end

  defp logged?(log, level, text), do: log =~ ~r/\[#{level}\] \[[^\]]*\] #{Regex.escape(text)}\n/

  defp sends_failed(log, host, reason) do
    line = ~r/Sending data to broker #{Regex.escape(inspect(host))}:\d+ failed with #{Regex.escape(inspect(reason))}\n/
    length(Regex.scan(line, log))
  end

  defp drain_attempts(acc \\ []) do
    receive do
      {:attempt, _pid, at} -> drain_attempts([at | acc])
    after
      0 -> Enum.sort(acc)
    end
  end

  defp lower_bound(failures), do: min(80 * Integer.pow(2, failures - 1), 400)
  defp upper_bound(failures), do: min(120 * Integer.pow(2, failures - 1), 500)

  defp gaps(attempts), do: attempts |> Enum.zip(tl(attempts)) |> Enum.map(fn {a, b} -> b - a end)

  # Every attempt runs into the 700 ms setup deadline, longer than any backoff, so attempts follow back to back.
  defp start_never_completing(tls, window) do
    %{fake: fake, host: host} = start_fake(tls: tls.server)
    FakeTransport.silence_tls(fake)
    opts = [use_ssl: true, ssl_options: tls.client_ok, connect_timeout: 700, reconnect_window: window]
    start_connection(fake, host, port(), :data, opts)
  end

  defp timed(fun) do
    started = now()
    {result, _log} = with_log(fun)
    {result, now() - started}
  end

  # The first request waits out its window; the second is answered by the next failed attempt, which spends the outage.
  defp spend_outage(conn, gate) do
    PinGate.close(gate)

    with_log(fn ->
      assert {:error, :not_connected, _meta} = dispatch(conn, request(:produce))
      assert {:error, :not_connected, _meta} = dispatch(conn, request(:produce))
    end)

    drain_attempts()
  end

  defp watch_connection_clock(host) do
    forward_telemetry([[:kafka_ex, :connection, :stop], [:kafka_ex, :connection, :close]], fn
      [:kafka_ex, :connection, :stop], _measurements, _metadata -> {:clock, self(), :connected, now()}
      _close, _measurements, metadata -> if metadata.host == host, do: {:clock, self(), :closed, now()}
    end)
  end

  defp failing(:raise), do: fn -> raise "conn-secret-password" end
  defp failing(:error), do: fn -> :erlang.error({:badmatch, "conn-secret-password"}) end
  defp failing(:throw), do: fn -> throw({:token, "conn-secret-password"}) end

  defp failing(:exit) do
    fn -> exit({:timeout, {GenServer, :call, [self(), {:token, "conn-secret-password"}, 5_000]}}) end
  end

  defp secrets_failing_at(call, kind) do
    calls = :counters.new(1, [])

    fn ->
      :counters.add(calls, 1, 1)
      if :counters.get(calls, 1) == call, do: failing(kind).(), else: %{ssl_options: [], auth: oauthbearer()}
    end
  end

  defp silent(:tls, fake), do: FakeTransport.silence_tls(fake)
  defp silent(api, fake), do: FakeTransport.respond(fake, api, :no_reply)

  # No socket opened before the TLS handshake, so there is none to close.
  defp assert_setup_closed(:tls), do: refute_received({:closed, _})
  defp assert_setup_closed(_step), do: assert_receive({:closed, %{reason: :setup_timeout}}, 1_000)

  defp silent_opts(:tls, tls), do: [use_ssl: true, ssl_options: tls.client_ok]
  defp silent_opts(:api_versions, _tls), do: []
  defp silent_opts(_sasl, _tls), do: [auth: oauthbearer()]

  defp api_versions_reply(:error_code), do: %{error_code: 35, api_keys: []}
  defp api_versions_reply(:truncated), do: fn request -> {:frame, <<request.correlation_id::32-signed, 0>>} end
  defp api_versions_reply(:wrong_id), do: fn request -> {:frame, <<request.correlation_id + 1::32-signed, 0::48>>} end

  defp api_versions_failure(:error_code), do: %Error{error: :unsupported_version, metadata: %{}}
  defp api_versions_failure(_malformed), do: :parse_error

  # Suspended, the Connection already holds the frame when its socket dies, as if killed between the read and the re-arm.
  defp kill_socket_under(conn, raw, release) do
    true = :erlang.suspend_process(conn)
    release.()
    await_frame(conn, raw)
    true = :erlang.port_close(raw)
    true = :erlang.resume_process(conn)
  end

  defp await_frame(conn, raw, deadline \\ now() + 2_000) do
    {:messages, messages} = Process.info(conn, :messages)

    cond do
      Enum.any?(messages, &(is_tuple(&1) and tuple_size(&1) > 2 and elem(&1, 1) == raw)) -> :ok
      now() >= deadline -> flunk("no frame reached the suspended Connection")
      true -> await_frame(conn, raw, deadline)
    end
  end

  defp await_connector(conn, deadline \\ now() + 2_000) do
    {_state, data} = :sys.get_state(conn)

    cond do
      data.connectors != [] -> :ok
      now() >= deadline -> flunk("connect/2 never reached the Connection")
      true -> await_connector(conn, deadline)
    end
  end

  describe "handshake" do
    test "the handshake writes nothing before ApiVersions, then SASL, then the request" do
      %{fake: fake, host: host} = start_fake()
      FakeTransport.respond(fake, :produce, frame("ok"))
      %{conn: conn} = start_connection(fake, host, port(), :data, auth: oauthbearer())

      assert {:ok, reply, %{bytes_sent: sent, bytes_received: received}} = dispatch(conn, request(:produce))

      assert payload(reply) == "ok"
      assert sent > 0 and received == byte_size(reply)

      assert Enum.map(FakeTransport.requests(fake), & &1.api) ==
               [:api_versions, :sasl_handshake, :sasl_authenticate, :produce]
    end

    test "one ApiVersions per connect, also under SASL" do
      %{fake: fake, host: host} = start_fake()
      watch_close(host)
      %{conn: conn} = start_connection(fake, host, port(), :data, auth: oauthbearer())

      for round <- 1..3 do
        assert {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)
        if round < 3, do: drop(fake, host)
      end

      by_conn = fake |> FakeTransport.requests() |> Enum.group_by(& &1.conn, & &1.api)

      assert map_size(by_conn) == 3
      assert Enum.all?(Map.values(by_conn), &(Enum.count(&1, fn api -> api == :api_versions end) == 1))
    end

    test "PLAIN without TLS fails with :plain_requires_tls before any request is written" do
      %{fake: fake, host: host} = start_fake()
      watch_close(host)
      %{conn: conn} = start_connection(fake, host, port(), :data, auth: plain())

      {result, log} = with_log(fn -> ConnectionStateMachine.connect(conn, 5_000) end)

      assert result == {:error, {:auth, :plain_requires_tls}}
      assert FakeTransport.requests(fake) == []
      assert_receive {:closed, %{reason: :auth_error}}, 1_000
      assert log =~ "SASL authentication failed for broker #{inspect(host)}:#{port()}: :plain_requires_tls"
    end

    test "a refused OAUTHBEARER token fails the handshake after the %x01, writes no request and keeps the backoff" do
      %{fake: fake, host: host} = start_fake()
      watch_close(host)
      FakeTransport.respond(fake, :sasl_authenticate, &FakeTransport.oauthbearer_refusal/1)
      %{conn: conn, reconnect_backoff: backoff} = start_connection(fake, host, port(), :data, auth: oauthbearer())
      started = now()

      {result, _log} = with_log(fn -> dispatch(conn, request(:produce), wait?: false) end)

      assert result == {:error, :not_connected, %{bytes_sent: 0, bytes_received: 0}}
      assert_receive {:closed, %{reason: :auth_error}}, 1_000

      assert Enum.map(FakeTransport.requests(fake), & &1.api) ==
               [:api_versions, :sasl_handshake, :sasl_authenticate, :sasl_authenticate]

      assert [{_address, next_at, 1}] = :ets.lookup(backoff, {host, port()})
      assert next_at - started >= 75
    end

    test "fresh credentials are read on every connect" do
      %{fake: fake, host: host} = start_fake()
      watch_close(host)
      calls = :counters.new(1, [])
      provider = fn -> :counters.add(calls, 1, 1) && {:ok, "token"} end
      %{conn: conn} = start_connection(fake, host, port(), :data, auth: oauthbearer(provider))

      assert {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)
      drop(fake, host)
      assert {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)

      assert :counters.get(calls, 1) == 2
    end

    for step <- [:tls, :api_versions, :sasl_handshake, :sasl_authenticate] do
      test "a silent peer at #{step} reaches :reconnecting within the setup deadline", %{tls: tls} do
        %{fake: fake, host: host} = start_fake(tls: tls.server)
        watch_close(host)
        silent(unquote(step), fake)
        opts = [connect_timeout: 200] ++ silent_opts(unquote(step), tls)
        %{conn: conn} = start_connection(fake, host, port(), :data, opts)

        task = Task.async(fn -> with_log(fn -> dispatch(conn, request(:produce)) end) end)

        assert_receive {ref, {{:error, :not_connected, _meta}, _log}} when ref == task.ref, 2_000
        assert_setup_closed(unquote(step))
        assert connection_state(conn) in [:reconnecting, :disconnected]
      end
    end

    test "a connect that completes after the setup deadline times out before writing ApiVersions" do
      %{fake: fake, host: host} = start_fake()
      watch_close(host)
      FakeTransport.delay_connects(fake, 300)
      %{conn: conn} = start_connection(fake, host, port(), :data, connect_timeout: 100)

      assert ConnectionStateMachine.connect(conn, 5_000) == {:error, :setup_timeout}
      assert_receive {:closed, %{reason: :setup_timeout}}, 1_000
      assert FakeTransport.requests(fake) == []
    end

    for kind <- [:error_code, :truncated, :wrong_id] do
      test "an ApiVersions reply with #{kind} fails the connect and closes the socket" do
        %{fake: fake, host: host} = start_fake()
        FakeTransport.subscribe(fake)
        FakeTransport.respond(fake, :api_versions, api_versions_reply(unquote(kind)))
        %{conn: conn} = start_connection(fake, host, port(), :data)

        assert ConnectionStateMachine.connect(conn, 5_000) ==
                 {:error, {:api_versions, api_versions_failure(unquote(kind))}}

        assert_receive {:fake_transport, :closed, _id}, 1_000
        assert connection_state(conn) == :disconnected
      end
    end

    test "a socket that dies between ApiVersions and its re-arm fails the connect, and its exit is ignored" do
      %{fake: fake, host: host} = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :api_versions)
      %{conn: conn} = start_connection(fake, host, port(), :data)
      test = self()
      spawn(fn -> send(test, {:connected, ConnectionStateMachine.connect(conn, 5_000)}) end)
      assert_receive {:fake_transport, :request, %{api: :api_versions, action: :hold}}, 2_000
      [%{socket: %{socket: raw}}] = FakeTransport.connections(fake)

      kill_socket_under(conn, raw, fn -> FakeTransport.release(fake, :api_versions) end)

      assert_receive {:connected, {:error, {:connect, _reason}}}, 2_000
      assert connection_state(conn) == :disconnected
    end

    test "the :connection and :auth spans come from the connection pid, never from the fake" do
      %{fake: fake, host: host} = start_fake()
      attach_spans()
      %{conn: conn} = start_connection(fake, host, port(), :data, auth: oauthbearer())

      assert {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)
      spans = drain_spans()

      assert [
               {^conn, :connection, :start, connection_start},
               {^conn, :auth, :start, auth_start},
               {^conn, :auth, :stop, auth_stop},
               {^conn, :connection, :stop, connection_stop}
             ] = spans

      assert keys(connection_start) == [:host, :port, :ssl]
      assert keys(auth_start) == [:host, :mechanism, :port]
      assert auth_start.mechanism == "OAUTHBEARER"
      assert keys(auth_stop) == []
      assert keys(connection_stop) == [:success]
      assert connection_stop.success == true
      assert connection_stop.telemetry_span_context == connection_start.telemetry_span_context

      {:ok, socket} =
        FakeTransport.connect(%{host: host, port: port(), role: :data}, FakeTransport.ssl_options(fake), false, 1_000)

      FakeTransport.close(socket)
      assert drain_spans() == []
    end
  end

  describe "framing, multiplexing and correlation" do
    setup :watched_fake

    test "out-of-order replies reach the right callers", %{fake: fake, host: host} do
      %{conn: conn} = start_connected(fake, host, :data)
      FakeTransport.respond(fake, :produce, fn request -> {:frame, [<<request.correlation_id + 1::32>>, "for-2"]} end)
      FakeTransport.respond(fake, :heartbeat, fn request -> {:frame, [<<request.correlation_id - 1::32>>, "for-1"]} end)
      FakeTransport.hold(fake, :produce)

      first = async_dispatch(conn, request(:produce))
      await_in_flight(conn, 1)
      second = async_dispatch(conn, request(:heartbeat))
      await_in_flight(conn, 2)
      FakeTransport.release(fake, :produce)

      assert_receive {^first, {:ok, first_reply, _meta}}, 2_000
      assert_receive {^second, {:ok, second_reply, _meta}}, 2_000
      assert {payload(first_reply), payload(second_reply)} == {"for-1", "for-2"}
      refute_receive {:closed, _}, 50
    end

    test "a late reply to a timed-out request is read and dropped, and the socket stays open", %{
      fake: fake,
      host: host
    } do
      %{conn: conn} = start_connection(fake, host, port(), :data)
      FakeTransport.respond(fake, :produce, frame("ok"))
      FakeTransport.hold(fake, :produce)

      {timed_out, log} = with_log(fn -> dispatch(conn, request(:produce), timeout: 100) end)
      FakeTransport.release(fake, :produce)

      assert {:error, :timeout, %{bytes_sent: sent}} = timed_out
      assert sent > 0
      assert log =~ "Receiving data from broker #{inspect(host)}:#{port()} timed out"
      assert {:ok, reply, _meta} = dispatch(conn, request(:produce))
      assert payload(reply) == "ok"
      assert [_one_connection] = fake |> requests(:produce) |> Enum.map(& &1.conn) |> Enum.uniq()
      refute_receive {:closed, _}, 50
    end

    test "a reply to a written request carries its wire correlation id, also when it times out", %{
      fake: fake,
      host: host
    } do
      %{conn: conn} = start_connection(fake, host, port(), :data)
      correlate_from(conn, 41)
      FakeTransport.respond(fake, :produce, frame("ok"))

      assert {:ok, _reply, %{correlation_id: answered}} = dispatch(conn, request(:produce))
      FakeTransport.hold(fake, :produce)
      {timed_out, _log} = with_log(fn -> dispatch(conn, request(:produce), timeout: 100) end)

      assert {:error, :timeout, %{correlation_id: abandoned}} = timed_out
      assert [answered, abandoned] == Enum.map(requests(fake, :produce), & &1.correlation_id)
      assert answered == 41
    end

    test "an id is not reused while its late reply can arrive, also after the counter wraps onto it", %{
      fake: fake,
      host: host
    } do
      %{conn: conn} = start_connection(fake, host, port(), :data)
      correlate_from(conn, 2_147_483_646)
      FakeTransport.respond(fake, :produce, frame("ok"))
      FakeTransport.hold(fake, :produce)

      {{:error, :timeout, _meta}, _log} = with_log(fn -> dispatch(conn, request(:produce), timeout: 100) end)
      later = for _ <- 1..2, do: async_dispatch(conn, request(:produce))
      await_in_flight(conn, 3)
      correlate_from(conn, 2_147_483_646)
      wrapped = async_dispatch(conn, request(:produce))
      await_in_flight(conn, 4)
      FakeTransport.release(fake, :produce)

      for ref <- later ++ [wrapped], do: assert_receive({^ref, {:ok, _reply, _meta}}, 2_000)
      assert Enum.map(requests(fake, :produce), & &1.correlation_id) == [2_147_483_646, 2_147_483_647, 0, 1]
    end

    test "an unknown correlation id closes the socket and every waiting sibling gets :timeout", %{
      fake: fake,
      host: host
    } do
      %{conn: conn} = start_connection(fake, host, port(), :data)
      FakeTransport.respond(fake, :produce, fn request -> {:frame, <<request.correlation_id + 1000::32>>} end)
      FakeTransport.respond(fake, :heartbeat, frame("hb"))
      FakeTransport.hold(fake, :produce)
      {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)

      produce = async_dispatch(conn, request(:produce))
      await_in_flight(conn, 1)
      sibling = async_dispatch(conn, request(:heartbeat))
      await_in_flight(conn, 2)
      FakeTransport.release(fake, :produce)

      assert_receive {:closed, %{reason: :correlation_mismatch}}, 2_000
      assert_receive {^produce, {:error, :timeout, _meta}}, 1_000
      assert_receive {^sibling, {:error, :timeout, _meta}}, 1_000
    end

    test "a reply shorter than a correlation id closes the socket and its caller gets :timeout", %{
      fake: fake,
      host: host
    } do
      FakeTransport.respond(fake, :produce, {:frame, <<1, 2>>})
      %{conn: conn} = start_connected(fake, host, :data)

      assert {:error, :timeout, _meta} = dispatch(conn, request(:produce))
      assert_receive {:closed, %{reason: :correlation_mismatch}}, 1_000
    end

    test "a reply read just before its socket died reaches its caller, then the connection drops", %{
      fake: fake,
      host: host
    } do
      FakeTransport.respond(fake, :produce, frame("ok"))
      FakeTransport.hold(fake, :produce)
      %{conn: conn} = start_connected(fake, host, :data)
      {:connected, %{socket: %{socket: raw}}} = :sys.get_state(conn)
      ref = async_dispatch(conn, request(:produce))
      await_in_flight(conn, 1)

      kill_socket_under(conn, raw, fn -> FakeTransport.release(fake, :produce) end)

      assert_receive {^ref, {:ok, reply, _meta}}, 2_000
      assert payload(reply) == "ok"
      assert_receive {:closed, %{reason: :remote_closed}}, 1_000
      assert connection_state(conn) == :disconnected
    end

    test "a reply over TLS reaches its caller", %{tls: tls} do
      %{fake: fake, host: host} = start_fake(tls: tls.server)
      FakeTransport.respond(fake, :produce, frame("tls"))
      %{conn: conn} = start_connection(fake, host, port(), :data, use_ssl: true, ssl_options: tls.client_ok)

      assert {:ok, reply, _meta} = dispatch(conn, request(:produce))
      assert payload(reply) == "tls"
    end
  end

  describe "liveness and long-poll" do
    setup :watched_fake

    for {role, api} <- [data: :produce, fetch: :fetch, coordinator: :heartbeat] do
      test "a never-reply broker on #{role}: the socket closes at the liveness bound", %{fake: fake, host: host} do
        FakeTransport.respond(fake, unquote(api), :no_reply)
        %{conn: conn} = start_connected(fake, host, unquote(role), liveness_slack: 100)
        started = now()

        {result, _log} = with_log(fn -> dispatch(conn, request(unquote(api)), timeout: 100) end)

        assert {:error, :timeout, _meta} = result
        assert now() - started >= 100
        assert_receive {:closed, %{reason: :liveness}}, 1_000
        assert now() - started >= 200
      end
    end

    test "a later request with an earlier liveness bound closes the socket at that bound", %{fake: fake, host: host} do
      FakeTransport.respond(fake, :fetch, :no_reply)
      %{conn: conn} = start_connected(fake, host, :fetch, liveness_slack: 100)
      _long = async_dispatch(conn, request(:fetch), long_poll: 1_000, timeout: 1_000)
      KafkaEx.TestHelpers.wait_for(fn -> length(requests(fake, :fetch)) == 1 end, 10)
      started = now()

      {result, _log} = with_log(fn -> dispatch(conn, request(:fetch), timeout: 100) end)

      assert {:error, :timeout, _meta} = result
      assert_receive {:closed, %{reason: :liveness}}, 1_000
      assert (now() - started) in 200..1_000
    end

    test "a healthy long-poll is never closed", %{fake: fake, host: host} do
      FakeTransport.respond(fake, :fetch, frame("records"))
      FakeTransport.hold(fake, :fetch)
      %{conn: conn} = start_connected(fake, host, :fetch, liveness_slack: 100)

      ref = async_dispatch(conn, request(:fetch), long_poll: 300, timeout: 400)
      refute_receive {:closed, _}, 250
      FakeTransport.release(fake, :fetch)

      assert_receive {^ref, {:ok, reply, _meta}}, 1_000
      assert payload(reply) == "records"
    end

    test "a long-poll answered after its caller gave up keeps the socket until its long-poll bound", %{
      fake: fake,
      host: host
    } do
      FakeTransport.respond(fake, :fetch, frame("records"))
      FakeTransport.hold(fake, :fetch)
      %{conn: conn} = start_connected(fake, host, :fetch, liveness_slack: 100)

      {result, _log} = with_log(fn -> dispatch(conn, request(:fetch), long_poll: 300, timeout: 100) end)
      refute_receive {:closed, _}, 250
      FakeTransport.release(fake, :fetch)

      assert {:error, :timeout, _meta} = result
      assert {:ok, _reply, _meta} = dispatch(conn, request(:fetch))
      refute_receive {:closed, _}, 50
    end

    test "abandoned ids count as outstanding and pause the idle clock", %{fake: fake, host: host} do
      FakeTransport.hold(fake, :fetch)
      opts = [idle_timeout: 200, liveness_slack: 1_000]
      %{conn: conn} = start_connection(fake, host, port(), {:fetch, "t", 0}, opts)
      monitor = Process.monitor(conn)
      {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)

      {{:error, :timeout, _meta}, _log} = with_log(fn -> dispatch(conn, request(:fetch), timeout: 100) end)

      refute_receive {:closed, _}, 400
      refute_received {:DOWN, ^monitor, _, _, _}
      assert_receive {:closed, %{reason: :liveness}}, 1_000
      assert_receive {:DOWN, ^monitor, :process, ^conn, :normal}, 1_000
    end
  end

  describe "await, then write" do
    setup do
      %{fake: fake, host: host} = start_fake()
      watch_close(host)
      %{conn: conn} = start_connection(fake, host, port(), :data)
      %{fake: fake, host: host, conn: conn}
    end

    test "await answers once the connection is up and writes nothing", %{fake: fake, conn: conn} do
      assert ConnectionStateMachine.await(conn, build_dispatch(request(:produce)), 5_000) == :ok
      assert connection_state(conn) == :connected
      assert requests(fake, :produce) == []
    end

    test "connect on a connected connection answers its versions and opens nothing", %{fake: fake, conn: conn} do
      assert {:ok, versions} = ConnectionStateMachine.connect(conn, 5_000)

      assert ConnectionStateMachine.connect(conn, 5_000) == {:ok, versions}
      assert length(FakeTransport.connects(fake)) == 1
    end

    test "a write whose deadline passed after its await is refused unwritten, and the connection stays up", %{
      fake: fake,
      conn: conn
    } do
      dispatch = build_dispatch(request(:produce))
      assert ConnectionStateMachine.await(conn, dispatch, 5_000) == :ok

      assert ConnectionStateMachine.write(conn, %{dispatch | deadline: now() - 1}, 5_000) ==
               {:error, :timeout, %{bytes_sent: 0, bytes_received: 0}}

      assert requests(fake, :produce) == []
      assert connection_state(conn) == :connected
    end

    test "a write after the connection dropped is a written :closed, with no reconnect", %{
      fake: fake,
      host: host,
      conn: conn
    } do
      dispatch = build_dispatch(request(:produce))
      assert ConnectionStateMachine.await(conn, dispatch, 5_000) == :ok
      watch_attempts(host)
      drop(fake, host)

      assert ConnectionStateMachine.write(conn, dispatch, 5_000) ==
               {:error, :closed, %{bytes_sent: 0, bytes_received: 0}}

      refute_receive {:attempt, _pid, _at}, 200
      assert requests(fake, :produce) == []
    end
  end

  describe "write path" do
    setup :watched_fake

    for transport <- [:tcp, :ssl] do
      test "a peer that stops reading over #{transport}: the connection closes within send_timeout and replies to every caller",
           %{tls: tls} do
        %{fake: fake, host: host} = start_fake(tls: tls.server)
        watch_close(host)
        FakeTransport.subscribe(fake)
        FakeTransport.respond(fake, :produce, :stall)

        tls_opts = if unquote(transport) == :ssl, do: [use_ssl: true, ssl_options: tls.client_ok ++ [sndbuf: 4096]]
        opts = [send_timeout: 200] ++ (tls_opts || [ssl_options: [sndbuf: 4096]])
        %{conn: conn} = start_connection(fake, host, port(), :data, opts)

        sibling = async_dispatch(conn, request(:produce), timeout: 5_000)
        assert_receive {:fake_transport, :request, %{api: :produce, action: :stall}}, 2_000

        big = request(:produce, acks: 0, messages: [%{value: :binary.copy(<<0>>, 4 * 1024 * 1024)}])
        {{sent, elapsed}, log} = with_log(fn -> write_until_failure(conn, big, 8) end)

        assert {:error, :timeout, %{bytes_sent: bytes}} = sent
        assert bytes > 4 * 1024 * 1024
        assert elapsed >= 200
        assert_receive {^sibling, {:error, :timeout, %{bytes_sent: sibling_bytes}}}, 1_000
        assert sibling_bytes < 1_024
        assert_receive {:closed, %{reason: :send_error}}, 1_000
        assert log =~ "Asynchronously sending data to broker #{inspect(host)}:#{port()} failed with :timeout"
      end
    end

    test "a socket gets the client's request timeout as its send timeout once, at connect, with no 5 s cap", %{
      fake: fake,
      host: host
    } do
      FakeTransport.respond(fake, :produce, frame("ok"))
      %{conn: conn} = start_connected(fake, host, :data)
      {:connected, %{socket: %{socket: raw}}} = :sys.get_state(conn)

      assert :inet.getopts(raw, [:send_timeout]) == {:ok, [send_timeout: KafkaEx.Config.request_timeout()]}
      assert KafkaEx.Config.request_timeout() > 5_000

      assert {:ok, _reply, _meta} = dispatch(conn, request(:produce), deadline: now() + 2_000)
      assert :inet.getopts(raw, [:send_timeout]) == {:ok, [send_timeout: KafkaEx.Config.request_timeout()]}
    end

    test "a request that cannot be encoded is refused unwritten, and its sibling is still answered", %{
      fake: fake,
      host: host
    } do
      %{conn: conn} = start_connection(fake, host, port(), :data)
      FakeTransport.respond(fake, :produce, frame("ok"))
      FakeTransport.hold(fake, :produce)
      sibling = async_dispatch(conn, request(:produce))
      await_in_flight(conn, 1)

      assert {:error, {:exception, :error, _reason, [_ | _]}, %{bytes_sent: 0, bytes_received: 0}} =
               dispatch(conn, request(:produce, messages: [%{value: 123}]))

      FakeTransport.release(fake, :produce)

      assert_receive {^sibling, {:ok, reply, _meta}}, 2_000
      assert payload(reply) == "ok"
      assert length(requests(fake, :produce)) == 1
      refute_receive {:closed, _}, 50
    end

    for reason <- [:econnreset, :closed] do
      test "a partial #{reason} send counts as written and gives written siblings its reason, not :timeout", %{
        fake: fake,
        host: host
      } do
        FakeTransport.subscribe(fake)
        FakeTransport.hold(fake, :heartbeat)
        %{conn: conn} = start_connected(fake, host, :coordinator)

        sibling = async_dispatch(conn, request(:heartbeat))
        assert_receive {:fake_transport, :request, %{api: :heartbeat, action: :hold}}, 2_000
        FakeTransport.fail_send(fake, :produce, 3, unquote(reason))

        {result, log} = with_log(fn -> dispatch(conn, request(:produce)) end)

        assert {:error, unquote(reason), %{bytes_sent: bytes}} = result
        assert bytes > 3
        assert sends_failed(log, host, unquote(reason)) == 1
        assert_receive {^sibling, {:error, unquote(reason), _meta}}, 1_000
        assert_receive {:closed, %{reason: :send_error}}, 1_000
      end
    end

    test "acks 0 completes on write and registers nothing", %{fake: fake, host: host} do
      %{conn: conn} = start_connection(fake, host, port(), :data)

      assert {:ok, :no_response, %{bytes_sent: bytes, bytes_received: 0}} =
               dispatch(conn, request(:produce, acks: 0), reply?: false)

      assert bytes > 0
      {:connected, data} = :sys.get_state(conn)
      assert data.in_flight == %{}
    end

    test "a request whose deadline passed while it waited is never written", %{fake: fake, host: host} do
      %{conn: conn} = start_connected(fake, host, :data)

      assert {:error, :timeout, %{bytes_sent: 0}} = dispatch(conn, request(:produce), deadline: now() - 1)
      assert requests(fake, :produce) == []
    end

    test "a version the connection did not negotiate is refused unwritten", %{fake: fake, host: host} do
      %{conn: conn} = start_connection(fake, host, port(), :data)

      assert {:error, {:unsupported_version, versions}, %{bytes_sent: 0}} =
               dispatch(conn, request(:produce, api_version: 2))

      assert versions[0] == {3, 3}
      assert requests(fake, :produce) == []
    end

    test "an API the broker does not advertise is refused unwritten", %{fake: fake, host: host} do
      advertised = Map.new(%{api_versions: 0..0, produce: 3..3}, fn {api, range} -> {Wire.api_key(api), range} end)
      FakeTransport.respond(fake, :api_versions, Wire.api_versions_fields(advertised))
      %{conn: conn} = start_connection(fake, host, port(), :data)

      assert {:error, {:unsupported_version, versions}, %{bytes_sent: 0}} = dispatch(conn, request(:heartbeat))
      refute Map.has_key?(versions, Wire.api_key(:heartbeat))
      assert requests(fake, :heartbeat) == []
    end
  end

  describe "close initiator and transport reasons" do
    setup :watched_fake

    test "a peer close with a caller waiting reads as a failed receive: :closed, :recv_error and an error line", %{
      fake: fake,
      host: host
    } do
      FakeTransport.respond(fake, :produce, :close)
      %{conn: conn} = start_connected(fake, host, :data)

      {result, log} = with_log([level: :debug], fn -> dispatch(conn, request(:produce)) end)

      assert {:error, :closed, %{bytes_sent: bytes}} = result
      assert bytes > 0
      assert_receive {:closed, %{reason: :recv_error}}, 1_000
      refute_receive {:closed, _}, 50
      assert logged?(log, :error, "Receiving data from broker #{inspect(host)}:#{port()} failed with :closed")
      refute log =~ "closed connection"
    end

    test "a tcp_error with a caller waiting returns its reason, emits :recv_error and logs the failed receive", %{
      fake: fake,
      host: host
    } do
      FakeTransport.hold(fake, :produce)
      %{conn: conn} = start_connected(fake, host, :data)
      ref = async_dispatch(conn, request(:produce))
      await_in_flight(conn, 1)
      log = inject_tcp_error(conn)

      assert_receive {^ref, {:error, :econnreset, _meta}}, 1_000
      assert_receive {:closed, %{reason: :recv_error}}, 1_000
      refute_receive {:closed, _}, 50
      assert logged?(log, :error, "Receiving data from broker #{inspect(host)}:#{port()} failed with :econnreset")
      refute log =~ "closed connection"
    end

    test "a peer close with no caller waiting emits :remote_closed and logs at debug", %{fake: fake, host: host} do
      FakeTransport.respond(fake, :produce, :no_reply)
      %{conn: conn} = start_connected(fake, host, :data)
      {{:error, :timeout, _meta}, _log} = with_log(fn -> dispatch(conn, request(:produce), timeout: 50) end)

      {_, log} =
        with_log([level: :debug], fn ->
          FakeTransport.close(fake, {host, port()})
          assert_receive {:closed, %{reason: :remote_closed}}, 1_000
        end)

      refute_receive {:closed, _}, 50
      assert logged?(log, :debug, "#{host}:#{port()} closed connection")
      refute log =~ "Receiving data"
    end

    test "once a request named its broker, a close with no caller waiting logs that broker", %{fake: fake, host: host} do
      FakeTransport.respond(fake, :produce, frame("ok"))
      %{conn: conn} = start_connection(fake, host, port(), :data)
      assert {:ok, _reply, _meta} = dispatch(conn, request(:produce), node_id: 1)

      {_, log} =
        with_log([level: :debug], fn ->
          FakeTransport.close(fake, {host, port()})
          assert_receive {:closed, %{reason: :remote_closed}}, 1_000
        end)

      assert logged?(log, :debug, "broker 1 (#{host}:#{port()}) closed connection")

      assert {:ok, _reply, _meta} = dispatch(conn, request(:produce), node_id: 1)
      log = inject_tcp_error(conn)

      assert logged?(log, :warning, "broker 1 (#{host}:#{port()}) closed connection: :econnreset")
    end

    test "a tcp_error with no caller waiting emits :recv_error and logs a warning", %{fake: fake, host: host} do
      %{conn: conn} = start_connected(fake, host, :data)
      log = inject_tcp_error(conn)

      assert_receive {:closed, %{reason: :recv_error}}, 1_000
      refute_receive {:closed, _}, 50
      assert logged?(log, :warning, "#{host}:#{port()} closed connection: :econnreset")
      refute log =~ "Receiving data"
    end

    test "a socket whose port dies silently drops the connection and answers its caller with :closed", %{
      fake: fake,
      host: host
    } do
      FakeTransport.hold(fake, :produce)
      %{conn: conn} = start_connected(fake, host, :data)
      ref = async_dispatch(conn, request(:produce))
      await_in_flight(conn, 1)
      {:connected, %{socket: %{socket: raw}}} = :sys.get_state(conn)

      with_log(fn ->
        :erlang.port_close(raw)
        assert_receive {^ref, {:error, :closed, _meta}}, 1_000
      end)

      assert_receive {:closed, %{reason: :recv_error}}, 1_000
      assert connection_state(conn) == :disconnected
    end

    test "a stale socket's message is ignored", %{fake: fake, host: host} do
      FakeTransport.respond(fake, :produce, frame("ok"))
      %{conn: conn} = start_connected(fake, host, :data)
      {:connected, %{socket: old}} = :sys.get_state(conn)
      FakeTransport.close(fake, {host, port()})
      assert_receive {:closed, %{reason: :remote_closed}}, 1_000
      {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)

      FakeTransport.inject(%{socket: old, owner: conn}, {:data, <<12_345::32, "stale">>})
      FakeTransport.inject(%{socket: old, owner: conn}, :closed)

      assert {:ok, _reply, _meta} = dispatch(conn, request(:produce))
      assert connection_state(conn) == :connected
      refute_received {:closed, _}
    end

    test "on a client-initiated close, written siblings get :timeout: a commit sibling and a produce sibling", %{
      fake: fake,
      host: host
    } do
      FakeTransport.respond(fake, :offset_commit, :no_reply)
      FakeTransport.respond(fake, :produce, :no_reply)
      %{conn: conn} = start_connection(fake, host, port(), :coordinator)
      monitor = Process.monitor(conn)
      {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)

      commit = async_dispatch(conn, request(:offset_commit))
      produce = async_dispatch(conn, request(:produce))
      await_in_flight(conn, 2)
      ConnectionStateMachine.stop(conn, :metadata_update)

      assert_receive {^commit, {:error, :timeout, _meta}}, 1_000
      assert_receive {^produce, {:error, :timeout, _meta}}, 1_000
      assert_receive {:closed, %{reason: :metadata_update}}, 1_000
      assert_receive {:DOWN, ^monitor, :process, ^conn, :normal}, 1_000
    end
  end

  describe "reconnect backoff" do
    setup do
      %{fake: fake, host: host} = start_fake()
      watch_attempts(host)
      %{fake: fake, host: host, gate: PinGate.new()}
    end

    test "with a dead broker, connect attempts per backoff interval stay at most one across 22 connections", %{
      fake: fake,
      host: host,
      gate: gate
    } do
      PinGate.close(gate)
      backoff = ConnectionStateMachine.reconnect_backoff_table()
      roles = for i <- 1..22, do: Enum.at([:metadata, :data, :coordinator, :fetch], rem(i, 4))
      opts = [gate: gate, reconnect_backoff: backoff, reconnect_window: 600]
      conns = for role <- roles, do: start_connection(fake, host, port(), role, opts).conn

      {refs, _log} = with_log(fn -> for conn <- conns, do: async_dispatch(conn, request(:produce)) end)

      for ref <- refs, do: assert_receive({^ref, {:error, :not_connected, _meta}}, 1_500)
      refute_receive :no_message_is_expected_while_trailing_attempts_land, 600
      assert [_ | _] = attempts = drain_attempts()
      gaps = attempts |> Enum.zip(tl(attempts)) |> Enum.map(fn {a, b} -> b - a end)

      assert length(attempts) <= ceil(:math.log2(600 / 80)) + 2

      for {gap, failures} <- Enum.with_index(gaps, 1) do
        assert gap >= 0.75 * lower_bound(failures), "gap #{failures} was #{gap} ms: #{inspect(gaps)}"
      end
    end

    test "a lone connection to a dead broker retries at gaps doubling from about 100 ms up to about 500 ms", %{
      fake: fake,
      host: host,
      gate: gate
    } do
      PinGate.close(gate)
      %{conn: conn} = start_connection(fake, host, port(), :data, gate: gate, reconnect_window: 2_000)

      {result, _log} = with_log(fn -> dispatch(conn, request(:produce)) end)

      assert {:error, :not_connected, _meta} = result
      assert [_, _, _, _ | _] = gaps = gaps(drain_attempts())

      for {gap, failures} <- Enum.with_index(gaps, 1) do
        assert gap in (lower_bound(failures) - @clock_slack_ms)..(upper_bound(failures) + @timer_slack_ms),
               "gaps #{inspect(gaps)}"
      end
    end

    test "once a connection connects, another one to that broker connects at once", %{fake: fake, host: host} do
      backoff = ConnectionStateMachine.reconnect_backoff_table()

      for role <- [:data, :coordinator] do
        %{conn: conn} = start_connection(fake, host, port(), role, reconnect_backoff: backoff)
        assert {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)
      end

      assert [first, second] = drain_attempts()
      assert second - first < lower_bound(1)
    end

    test "connections dropped together close the gate once, so the next attempt waits one first-failure gap", %{
      fake: fake,
      host: host,
      gate: gate
    } do
      backoff = ConnectionStateMachine.reconnect_backoff_table()
      opts = [gate: gate, reconnect_backoff: backoff, reconnect_window: 700, stable_period: 0]
      roles = [:data, :coordinator, :metadata, {:fetch, "t", 0}, {:fetch, "t", 1}]
      conns = for role <- roles, do: start_connected(fake, host, role, opts).conn
      PinGate.close(gate)
      FakeTransport.close(fake, {host, port()})
      drain_attempts()

      {refs, _log} = with_log(fn -> for conn <- conns, do: async_dispatch(conn, request(:produce)) end)

      for ref <- refs, do: assert_receive({^ref, {:error, :not_connected, _meta}}, 1_500)
      [first | _] = attempts = drain_attempts()
      {burst, [next | _] = paced} = Enum.split_with(attempts, &(&1 - first < lower_bound(1)))
      assert burst != []
      assert next - first <= upper_bound(1) + @timer_slack_ms
      for {gap, failures} <- Enum.with_index(gaps(paced), 2), do: assert(gap >= lower_bound(failures) - @clock_slack_ms)
    end

    test "a connection stable for stable_period resets the failure count", %{fake: fake, host: host, gate: gate} do
      watch_close(host)
      %{conn: conn} = start_connection(fake, host, port(), :data, gate: gate, reconnect_window: 600, stable_period: 50)
      PinGate.close(gate)
      {{:error, :not_connected, _meta}, _log} = with_log(fn -> dispatch(conn, request(:produce)) end)
      assert [_, _, _ | _] = drain_attempts()
      PinGate.open(gate)
      {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)
      refute_receive :stable_period_elapses, 150
      PinGate.close(gate)
      drop(fake, host)
      drain_attempts()

      {{:error, :not_connected, _meta}, _log} = with_log(fn -> dispatch(conn, request(:produce)) end)

      assert [first, second | _] = drain_attempts()
      assert second - first <= upper_bound(1) + @timer_slack_ms
    end

    test "a first connect waits on another connection's reservation only until that one connects", %{
      fake: fake,
      host: host,
      gate: gate
    } do
      backoff = ConnectionStateMachine.reconnect_backoff_table()
      opts = [gate: gate, reconnect_backoff: backoff, reconnect_window: 600]
      %{conn: holder} = start_connection(fake, host, port(), :data, opts)
      PinGate.close(gate)
      {{:error, :not_connected, _meta}, _log} = with_log(fn -> dispatch(holder, request(:produce)) end)
      assert [_, _, _ | _] = drain_attempts()
      PinGate.open(gate)
      FakeTransport.delay_connects(fake, @slow_connect_ms)
      holding = Task.async(fn -> ConnectionStateMachine.connect(holder, 5_000) end)
      assert_receive {:attempt, ^holder, reserved_at}, 1_000

      %{conn: waiter} = start_connection(fake, host, port(), :coordinator, reconnect_backoff: backoff)

      assert {:ok, _versions} = ConnectionStateMachine.connect(waiter, 5_000)
      assert {:ok, _versions} = Task.await(holding)
      assert_received {:attempt, ^waiter, attempted_at}
      assert (attempted_at - reserved_at) in @slow_connect_ms..(lower_bound(4) - @timer_slack_ms)
    end

    test "a broker in backoff does not delay a first connect to another broker", %{fake: fake, host: host, gate: gate} do
      PinGate.close(gate)
      backoff = ConnectionStateMachine.reconnect_backoff_table()
      %{conn: dead} = start_connection(fake, host, port(), :data, gate: gate, reconnect_backoff: backoff)
      {{:error, _reason}, _log} = with_log(fn -> ConnectionStateMachine.connect(dead, 5_000) end)
      %{conn: other} = start_connection(fake, host, port() + 1, :data, gate: gate, reconnect_backoff: backoff)
      asked = now()

      {{:error, _reason}, _log} = with_log(fn -> ConnectionStateMachine.connect(other, 5_000) end)

      assert_received {:attempt, ^other, attempted_at}
      assert attempted_at - asked < lower_bound(1)
    end

    test "a connection that has attempted ignores a backoff wake and keeps its backoff", %{
      fake: fake,
      host: host,
      gate: gate
    } do
      PinGate.close(gate)
      backoff = ConnectionStateMachine.reconnect_backoff_table()

      %{conn: conn} =
        start_connection(fake, host, port(), :data, gate: gate, reconnect_backoff: backoff, reconnect_window: 5_000)

      {_ref, _log} = with_log(fn -> async_dispatch(conn, request(:produce)) end)
      assert_receive {:attempt, ^conn, _at}, 1_000
      assert {:reconnecting, _data} = :sys.get_state(conn)
      PinGate.open(gate)
      # An open gate makes a mishandled wake attempt at once; no connection can open it while this one holds the slot.
      :ets.insert(backoff, {{host, port()}, :open, 1})
      send(conn, :reconnect_slot)

      assert {:reconnecting, _data} = :sys.get_state(conn)
      refute_received {:attempt, ^conn, _at}
    end

    test "a connection that waited on another's reservation drops its wake row once it attempts", %{
      fake: fake,
      host: host,
      gate: gate
    } do
      PinGate.close(gate)
      backoff = ConnectionStateMachine.reconnect_backoff_table()
      %{conn: holder} = start_connection(fake, host, port(), :data, gate: gate, reconnect_backoff: backoff)
      {{:error, _reason}, _log} = with_log(fn -> ConnectionStateMachine.connect(holder, 5_000) end)
      %{conn: conn} = start_connection(fake, host, port(), :coordinator, gate: gate, reconnect_backoff: backoff)

      {result, _log} = with_log(fn -> ConnectionStateMachine.connect(conn, 5_000) end)

      assert {:error, _reason} = result
      assert [_holder, _conn] = drain_attempts()
      assert :ets.match_object(backoff, {{:first_connect, :_, :_}}) == []
    end

    test "a request that arrives after the backoff interval attempts at once", %{fake: fake, host: host, gate: gate} do
      FakeTransport.respond(fake, :produce, frame("ok"))
      PinGate.close(gate)
      %{conn: conn} = start_connection(fake, host, port(), :data, gate: gate)

      {result, _log} = with_log(fn -> dispatch(conn, request(:produce), wait?: false) end)
      assert {:error, :not_connected, _meta} = result
      assert_receive {:attempt, ^conn, _at}, 1_000
      refute_receive {:attempt, ^conn, _at}, 150
      PinGate.open(gate)
      asked = now()

      assert {:ok, _reply, _meta} = dispatch(conn, request(:produce))
      assert_received {:attempt, ^conn, at}
      assert at - asked < 150
      refute_received {:attempt, ^conn, _at}
    end

    for gate_state <- [:closed, :open] do
      test "on a long-dead broker a new request returns :not_connected within its window, gate #{gate_state}", %{
        fake: fake,
        host: host,
        gate: gate
      } do
        if unquote(gate_state) == :closed, do: PinGate.close(gate)
        backoff = ConnectionStateMachine.reconnect_backoff_table()
        :ets.insert(backoff, {{host, port()}, now() + 5_000, 8})

        %{conn: conn} =
          start_connection(fake, host, port(), :data, gate: gate, reconnect_backoff: backoff, reconnect_window: 600)

        ref = async_dispatch(conn, request(:produce))

        refute_receive {^ref, _reply}, 450
        assert_receive {^ref, {:error, :not_connected, _meta}}, 450
        refute_received {:attempt, _pid, _at}
      end
    end

    test "once an outage has cost a window, a request while the next slot is pending is answered at once", %{
      fake: fake,
      host: host,
      gate: gate
    } do
      %{conn: conn} = start_connection(fake, host, port(), :data, gate: gate, reconnect_window: 600)
      spend_outage(conn, gate)
      asked = now()

      {result, _log} = with_log(fn -> dispatch(conn, request(:produce)) end)

      assert {:error, :not_connected, _meta} = result
      assert now() - asked < 250
      refute_received {:attempt, ^conn, _at}
    end

    test "once an outage has cost a window, a request on a free slot gets that one attempt", %{
      fake: fake,
      host: host,
      gate: gate
    } do
      FakeTransport.respond(fake, :produce, frame("ok"))
      backoff = ConnectionStateMachine.reconnect_backoff_table()

      %{conn: conn} =
        start_connection(fake, host, port(), :data, gate: gate, reconnect_backoff: backoff, reconnect_window: 600)

      spend_outage(conn, gate)
      [{_address, next_at, _failures}] = :ets.lookup(backoff, {host, port()})
      refute_receive :slot_is_free, max(next_at - now(), 0) + 1
      PinGate.open(gate)

      assert {:ok, reply, _meta} = dispatch(conn, request(:produce))
      assert payload(reply) == "ok"
      assert [_one] = drain_attempts()
    end

    test "a waiter's window holds while connect attempts that never complete outlast the backoff", %{tls: tls} do
      %{conn: conn} = start_never_completing(tls, 1_000)

      {result, elapsed} = timed(fn -> dispatch(conn, request(:produce)) end)

      assert {:error, :not_connected, _meta} = result
      assert elapsed >= 1_000 and elapsed < 1_000 + 700 + 500
    end

    test "once spent, a request whose single attempt outlasts the backoff is answered when that attempt fails", %{
      tls: tls
    } do
      %{conn: conn} = start_never_completing(tls, 1_500)
      {{:error, :not_connected, _meta}, _elapsed} = timed(fn -> dispatch(conn, request(:produce)) end)

      {result, elapsed} = timed(fn -> dispatch(conn, request(:produce)) end)

      assert {:error, :not_connected, _meta} = result
      assert elapsed < 700 + 600
    end

    test "in a spent outage, a waiter behind a pending connect still gets its window, and the outage stays spent",
         %{fake: fake, host: host, gate: gate} do
      backoff = ConnectionStateMachine.reconnect_backoff_table()

      %{conn: conn} =
        start_connection(fake, host, port(), :data, gate: gate, reconnect_backoff: backoff, reconnect_window: 600)

      spend_outage(conn, gate)
      # Another connection to this broker pushed the shared slot out past the window.
      :ets.insert(backoff, {{host, port()}, now() + 3_000, 8})
      spawn(fn -> capture_log(fn -> ConnectionStateMachine.connect(conn, 5_000) end) end)
      await_connector(conn)
      asked = now()

      {result, _log} = with_log(fn -> dispatch(conn, request(:produce)) end)

      assert {:error, :not_connected, _meta} = result
      assert (now() - asked) in 600..1_500
      assert {:reconnecting, %{outage: :spent, connectors: [_pending]}} = :sys.get_state(conn)
    end

    test "a fetch connection answered at once in a spent outage still idles out", %{fake: fake, host: host, gate: gate} do
      opts = [gate: gate, reconnect_window: 600, idle_timeout: 300]
      %{conn: conn} = start_connection(fake, host, port(), {:fetch, "t", 0}, opts)
      monitor = Process.monitor(conn)
      spend_outage(conn, gate)

      {result, _log} = with_log(fn -> dispatch(conn, request(:produce)) end)

      assert {:error, :not_connected, _meta} = result
      assert_receive {:DOWN, ^monitor, :process, ^conn, :normal}, round(300 * 1.2) + 200
    end

    test "a connect ends the outage, so the next outage waits a whole window again", %{
      fake: fake,
      host: host,
      gate: gate
    } do
      FakeTransport.respond(fake, :produce, frame("ok"))
      backoff = ConnectionStateMachine.reconnect_backoff_table()

      %{conn: conn} =
        start_connection(fake, host, port(), :data, gate: gate, reconnect_backoff: backoff, reconnect_window: 600)

      spend_outage(conn, gate)
      [{_address, next_at, _failures}] = :ets.lookup(backoff, {host, port()})
      refute_receive :slot_is_free, max(next_at - now(), 0) + 1
      PinGate.open(gate)
      {:ok, _reply, _meta} = dispatch(conn, request(:produce))
      watch_close(host)
      drop(fake, host)
      PinGate.close(gate)

      ref = async_dispatch(conn, request(:produce))

      refute_receive {^ref, _reply}, 500
      assert_receive {^ref, {:error, :not_connected, _meta}}, 300
    end

    test "a waiter is answered by a later successful attempt inside its window", %{fake: fake, host: host, gate: gate} do
      FakeTransport.respond(fake, :produce, frame("ok"))
      PinGate.close_for(gate, 300)
      %{conn: conn} = start_connection(fake, host, port(), :data, gate: gate, reconnect_window: 1_500)

      {result, _log} = with_log(fn -> dispatch(conn, request(:produce)) end)

      assert {:ok, reply, _meta} = result
      assert payload(reply) == "ok"
      assert length(drain_attempts()) >= 2
    end

    test "a crashing handshake still paces the next connection", %{fake: fake, host: host} do
      backoff = ConnectionStateMachine.reconnect_backoff_table()
      auth = oauthbearer(fn -> raise "token source down" end)
      %{conn: crashing} = start_connection(fake, host, port(), :data, auth: auth, reconnect_backoff: backoff)
      monitor = Process.monitor(crashing)
      asked = now()

      log = capture_log(fn -> catch_exit(ConnectionStateMachine.connect(crashing, 5_000)) end)

      assert_receive {:DOWN, ^monitor, :process, ^crashing, {:handshake_failed, RuntimeError}}, 1_000
      refute log =~ "token source down"
      assert [{_key, next_at, 1}] = :ets.lookup(backoff, {host, port()})
      assert next_at - asked >= 75
    end
  end

  describe "lifecycle and security" do
    setup :watched_fake

    test "fetch idle close is jittered and stops the process", %{fake: fake, host: host} do
      watch_connection_clock(host)
      %{conn: conn} = start_connection(fake, host, port(), {:fetch, "t", 0}, idle_timeout: 200)
      monitor = Process.monitor(conn)
      {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)

      assert_receive {:DOWN, ^monitor, :process, ^conn, :normal}, 2_000
      assert_received {:closed, %{reason: :idle}}
      assert_received {:clock, ^conn, :connected, connected_at}
      assert_received {:clock, ^conn, :closed, closed_at}
      assert (closed_at - connected_at) in 160..(240 + 100)
    end

    test "data and coordinator roles never idle out", %{fake: fake, host: host} do
      monitors =
        for role <- [:data, :coordinator] do
          %{conn: conn} = start_connected(fake, host, role, idle_timeout: 200)
          Process.monitor(conn)
        end

      refute_receive {:DOWN, _monitor, _, _, _}, 400
      for monitor <- monitors, do: Process.demonitor(monitor)
    end

    test "a dead old leader's fetch connection stops within the idle timeout", %{fake: fake, host: host} do
      gate = PinGate.new()
      %{conn: conn} = start_connection(fake, host, port(), {:fetch, "t", 0}, gate: gate, idle_timeout: 200)
      monitor = Process.monitor(conn)
      {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)
      PinGate.close(gate)

      FakeTransport.close(fake, {host, port()})

      assert_receive {:DOWN, ^monitor, :process, ^conn, :normal}, round(200 * 1.2) + 200
    end

    test "a write whose caller died while it waited in the mailbox is not sent", %{fake: fake, host: host} do
      FakeTransport.respond(fake, :produce, frame("ok"))
      %{conn: conn} = start_connected(fake, host, :data)
      dispatch = build_dispatch(request(:produce))
      :ok = ConnectionStateMachine.await(conn, dispatch, 1_000)

      :ok = :sys.suspend(conn)
      caller = spawn(fn -> ConnectionStateMachine.write(conn, dispatch, 5_000) end)
      KafkaEx.TestHelpers.wait_for(fn -> Process.info(conn, :message_queue_len) == {:message_queue_len, 1} end, 10)
      Process.exit(caller, :kill)
      refute Process.alive?(caller)
      :ok = :sys.resume(conn)

      assert {:ok, _reply, _meta} = dispatch(conn, request(:produce))
      assert length(requests(fake, :produce)) == 1
    end

    test "terminate closes the socket with :shutdown", %{fake: fake, host: host} do
      supervisor = start_supervised!(DynamicSupervisor)
      args = args(fake, host, port(), :data, ConnectionStateMachine.reconnect_backoff_table(), [])
      {:ok, conn} = DynamicSupervisor.start_child(supervisor, ConnectionStateMachine.child_spec(args))
      {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)

      :ok = DynamicSupervisor.terminate_child(supervisor, conn)

      assert_receive {:closed, %{reason: :shutdown}}, 1_000
    end

    test "a connection stops when its owner dies, closing with :shutdown, or with :init_error for a {:shutdown, :init_error} exit",
         %{fake: fake, host: host} do
      for {exit_reason, close} <- [{:kill, :shutdown}, {{:shutdown, :init_error}, :init_error}] do
        owner = spawn(fn -> receive do: (:never -> :ok) end)

        args = %{
          args(fake, host, port(), :metadata, ConnectionStateMachine.reconnect_backoff_table(), [])
          | client: owner
        }

        conn = start_supervised!(%{ConnectionStateMachine.child_spec(args) | id: make_ref()})
        monitor = Process.monitor(conn)
        {:ok, _versions} = ConnectionStateMachine.connect(conn, 5_000)

        Process.exit(owner, exit_reason)

        assert_receive {:closed, %{reason: ^close}}, 1_000
        assert_receive {:DOWN, ^monitor, :process, ^conn, :normal}, 1_000
      end
    end

    test "format_status redacts the secrets fun and queued requests", %{fake: fake, host: host} do
      gate = PinGate.new()
      PinGate.close(gate)
      %{conn: conn} = start_connection(fake, host, port(), :data, gate: gate)
      _ref = async_dispatch(conn, request(:produce, topic: "conn-secret-topic"))
      await_waiting(conn, 1)

      {:status, ^conn, _module, [_dictionary, _running, _parent, _debug, items]} = :sys.get_status(conn)
      [{~c"State", {:reconnecting, data}}] = items |> Keyword.get_values(:data) |> List.last()
      text = inspect(data, limit: :infinity, printable_limit: :infinity)

      assert data.secrets == :redacted
      assert text =~ "Kayrock.Produce.V3.Request"
      refute text =~ "#Function"
      refute text =~ "conn-secret-topic"
    end

    test "init does no I/O", %{fake: fake, host: host} do
      watch_attempts(host)
      FakeTransport.respond(fake, :produce, frame("ok"))
      %{conn: conn} = start_connection(fake, host, port(), :data)

      refute_receive {:attempt, ^conn, _at}, 100
      assert FakeTransport.connects(fake) == []
      assert {:ok, _reply, _meta} = dispatch(conn, request(:produce))
      assert_received {:attempt, ^conn, _at}
    end
  end

  describe "handshake step failures" do
    setup do
      %{fake: fake, host: host} = start_fake()
      %{fake: fake, host: host}
    end

    for {call, closed} <- [{1, [:connection]}, {2, [:connection]}, {3, [:auth, :connection]}] do
      test "a step that raises at secrets call #{call} still ends every span it opened", %{fake: fake, host: host} do
        attach_spans()
        secrets = secrets_failing_at(unquote(call), :raise)
        opts = [secrets: fn -> Map.update!(secrets.(), :ssl_options, &FakeTransport.ssl_options(fake, &1)) end]
        %{conn: conn} = start_connection(fake, host, port(), :data, opts)

        capture_log(fn -> catch_exit(ConnectionStateMachine.connect(conn, 5_000)) end)

        spans = drain_spans()
        started = for {^conn, name, :start, _meta} <- spans, do: name
        ended = for {^conn, name, phase, _meta} <- spans, phase in [:stop, :exception], do: name

        assert Enum.sort(started) == Enum.sort(unquote(closed))
        assert ended == unquote(closed)
      end
    end

    for kind <- [:raise, :error, :throw, :exit] do
      test "a #{kind} in a handshake step leaves only {:handshake_failed, _} and no secret", %{fake: fake, host: host} do
        attach_spans()
        %{conn: conn} = start_connection(fake, host, port(), :data, secrets: failing(unquote(kind)))
        monitor = Process.monitor(conn)

        log = capture_log([level: :debug], fn -> catch_exit(ConnectionStateMachine.connect(conn, 5_000)) end)

        assert_receive {:DOWN, ^monitor, :process, ^conn, {:handshake_failed, reason}}, 1_000
        assert reason in [RuntimeError, MatchError, :throw, :exit]
        telemetry = for {_pid, _name, _phase, meta} <- drain_spans(), do: {:telemetry, inspect(meta, limit: :infinity)}
        assert LeakProbe.exposures([{:log, log} | telemetry], ["conn-secret-password"]) == MapSet.new()
      end
    end
  end
end
