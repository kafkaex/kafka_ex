defmodule KafkaEx.Security.CredentialLeakTest do
  @moduledoc "SCRAM is covered for its username, password and client-first message only: the fake cannot complete the exchange."
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import KafkaEx.TestHelpers, only: [opaque: 1]

  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Client
  alias KafkaEx.Telemetry
  alias KafkaEx.TestSupport.FakeTransport
  alias KafkaEx.TestSupport.LeakProbe
  alias KafkaEx.TestSupport.TLSFixtures

  @moduletag :leak

  @port 9092
  @wait 5_000

  @mechanisms [:plain, :scram, :oauthbearer, :msk_iam]
  @cases [
    :healthy_boot,
    :crash_mid_sasl,
    :crash_after_connect,
    :malformed_reply,
    :peer_close,
    :recv_silence,
    :stacktrace_args
  ]

  @skip_reason %{
    {:scram, :healthy_boot} => "the fake cannot complete SCRAM",
    {:scram, :crash_after_connect} => "the fake cannot complete SCRAM",
    {:scram, :stacktrace_args} =>
      "client-first uses only the username and the password is first used after server-first, which the fake cannot send",
    {:msk_iam, :stacktrace_args} => "a raising credential source is covered by the OAUTHBEARER token provider"
  }

  @matrix for m <- @mechanisms, c <- @cases, not Map.has_key?(@skip_reason, {m, c}), do: {m, c}

  @baseline MapSet.new([
              {:msk_iam, :state_p, :client, :access_key_id},
              {:msk_iam, :state_p, :client, :secret_access_key},
              {:msk_iam, :state_p, :client, :session_token},
              {:msk_iam, :status_p, :client, :access_key_id},
              {:msk_iam, :status_p, :client, :secret_access_key},
              {:msk_iam, :status_p, :client, :session_token},
              {:oauthbearer, :stacktrace_args, :log, :token},
              {:oauthbearer, :stacktrace_args, :start_error, :token},
              {:oauthbearer, :stacktrace_args, :telemetry, :token},
              # Logger cuts the state before auth on Elixir 1.19, but prints it on 1.18 / OTP 27.
              {:plain, :crash_after_connect, :log, :username},
              {:plain, :stacktrace_args, :log, :password},
              {:plain, :stacktrace_args, :start_error, :password},
              {:plain, :stacktrace_args, :telemetry, :password},
              {:plain, :state_inspect, :client, :username},
              {:plain, :state_p, :client, :password},
              {:plain, :state_p, :client, :username},
              {:plain, :status_inspect, :client, :username},
              {:plain, :status_p, :client, :password},
              {:plain, :status_p, :client, :username},
              # The client state holds the auth config for every mechanism; SCRAM's is unobservable here only because its boot fails on the fake.
              {:scram, :state_inspect, :client, :username},
              {:scram, :state_p, :client, :password},
              {:scram, :state_p, :client, :username},
              {:scram, :status_inspect, :client, :username},
              {:scram, :status_p, :client, :password},
              {:scram, :status_p, :client, :username}
            ])

  # A handshake raise happens in the connection, which reports only the exception's module.
  @retired_by_connection MapSet.new([
                           {:oauthbearer, :stacktrace_args, :log, :token},
                           {:oauthbearer, :stacktrace_args, :start_error, :token},
                           {:oauthbearer, :stacktrace_args, :telemetry, :token},
                           {:plain, :stacktrace_args, :log, :password},
                           {:plain, :stacktrace_args, :start_error, :password},
                           {:plain, :stacktrace_args, :telemetry, :password}
                         ])

  setup_all do
    {:ok, tls: TLSFixtures.chain()}
  end

  setup do
    sleep = Application.get_env(:kafka_ex, :sleep_for_reconnect)
    Application.put_env(:kafka_ex, :sleep_for_reconnect, 0)

    restore_reports = include_crash_reports()
    handler = "credential-leak-#{inspect(make_ref())}"

    :ok =
      :telemetry.attach_many(
        handler,
        Telemetry.connection_events() ++ Telemetry.auth_events(),
        &__MODULE__.forward/4,
        self()
      )

    on_exit(fn ->
      :telemetry.detach(handler)
      restore_reports.()
      Application.put_env(:kafka_ex, :sleep_for_reconnect, sleep)
    end)
  end

  # Elixir 1.15+ gates SASL reports in a primary filter, Elixir 1.14 in the Logger handler.
  defp include_crash_reports do
    filters = :logger.get_primary_config().filters

    case Keyword.fetch(filters, :logger_translator) do
      {:ok, {translate, config}} ->
        :ok =
          :logger.set_primary_config(
            :filters,
            Keyword.put(filters, :logger_translator, {translate, %{config | sasl: true}})
          )

        fn -> :logger.set_primary_config(:filters, filters) end

      :error ->
        {:ok, %{config: %{sasl: sasl}}} = :logger.get_handler_config(Logger)
        :ok = :logger.update_handler_config(Logger, :config, %{sasl: true})
        fn -> :logger.update_handler_config(Logger, :config, %{sasl: sasl}) end
    end
  end

  def forward(_event, _measurements, metadata, test) do
    send(test, {:leak_telemetry, to_string(metadata[:host]), metadata})
  end

  test "no credential reaches a log, a start error, telemetry or a process state beyond the baseline", %{tls: tls} do
    labels =
      @matrix |> Enum.map(fn {mechanism, kase} -> measure(mechanism, kase, tls) end) |> Enum.reduce(&MapSet.union/2)

    {connection_side, client_side} = Enum.split_with(labels, &match?({_mechanism, _surface, :connection, _kind}, &1))
    client_side = MapSet.new(client_side)

    assert MapSet.subset?(client_side, @baseline),
           "new exposures: #{inspect(MapSet.difference(client_side, @baseline))}"

    assert MapSet.new(connection_side) == MapSet.new()
    assert MapSet.disjoint?(client_side, @retired_by_connection)
  end

  defp measure(mechanism, kase, tls) do
    host = "leak-#{mechanism}-#{kase}"
    fake = start_supervised!({FakeTransport, brokers: [{1, host, @port}], tls: tls.server}, id: make_ref())
    :ok = FakeTransport.subscribe(fake)
    script(fake, kase)

    run = %{mechanism: mechanism, kase: kase, host: host, fake: fake}
    {{start, probes}, log} = with_log([level: :debug], fn -> drive(run, client_opts(run, tls)) end)
    telemetry = drain_telemetry(host)
    texts = [{{mechanism, kase, :log}, log}] ++ start_texts(run, start) ++ telemetry_texts(run, telemetry) ++ probes

    auth_bytes = sent_auth_bytes(fake)
    check_controls(run, auth_bytes, probes, log, telemetry)
    LeakProbe.exposures(texts, secrets(mechanism) ++ Enum.map(auth_bytes, &{:auth_bytes, &1}))
  end

  defp script(fake, :crash_mid_sasl), do: FakeTransport.hold(fake, :sasl_authenticate)
  defp script(fake, :malformed_reply), do: FakeTransport.respond(fake, :sasl_authenticate, {:frame, <<1>>})
  defp script(fake, :peer_close), do: FakeTransport.respond(fake, :sasl_authenticate, :close)
  defp script(fake, :recv_silence), do: FakeTransport.respond(fake, :sasl_authenticate, :no_reply)
  defp script(_fake, _kase), do: :ok

  defp client_opts(%{mechanism: mechanism, kase: kase, host: host, fake: fake}, tls) do
    tls_opts = if mechanism == :plain, do: [use_ssl: true, ssl_options: FakeTransport.ssl_options(fake, tls.client_ok)]
    deadline = if kase == :recv_silence, do: [connection: [connect_timeout: 300]]

    [
      uris: [{host, @port}],
      consumer_group: :no_consumer_group,
      transport: FakeTransport,
      ssl_options: FakeTransport.ssl_options(fake),
      metadata_update_interval: 3_600_000,
      auth: auth(mechanism, kase)
    ]
    |> Keyword.merge(tls_opts || [])
    |> Keyword.merge(deadline || [])
  end

  defp drive(%{kase: kase} = run, opts) do
    test = self()
    ref = make_ref()

    starter =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        send(test, {:started, ref, Client.start_link(opts, :no_name)})

        receive do
          :stop -> :ok
        end
      end)

    run = Map.merge(run, %{starter: starter, ref: ref})
    {start, probes} = inject(kase, run)
    teardown(run, start)
    {start, probes}
  end

  defp inject(:crash_mid_sasl, run) do
    await_authenticate(run)
    run |> owners() |> Enum.each(&stop(&1, fn pid -> Process.exit(pid, :kill) end))
    start = await_start(run)
    {start, probe(run, start)}
  end

  defp inject(:crash_after_connect, run) do
    assert {:ok, _client} = start = await_start(run)
    run |> owners() |> Enum.each(&stop(&1, fn pid -> :sys.terminate(pid, :leak_probe_crash) end))
    {start, probe(run, start)}
  end

  defp inject(:recv_silence, run) do
    await_authenticate(run)
    probes = probe(run, :pending)
    run |> owners() |> Enum.each(&stop(&1, fn pid -> Process.exit(pid, :kill) end))
    {await_start(run), probes}
  end

  defp inject(:stacktrace_args, run) do
    start = await_start(run)
    {start, probe(run, start)}
  end

  defp inject(_kase, run) do
    await_authenticate(run)
    start = await_start(run)
    {start, probe(run, start)}
  end

  defp await_authenticate(%{host: host}) do
    assert_receive {:fake_transport, :request, %{api: :sasl_authenticate, host: ^host}}, @wait
  end

  defp await_start(%{ref: ref}) do
    receive do
      {:started, ^ref, result} -> result
    after
      @wait -> flunk("start_link did not return within #{@wait} ms")
    end
  end

  defp owners(%{fake: fake}), do: fake |> FakeTransport.connects() |> Enum.map(& &1.owner) |> Enum.uniq()

  defp stop(pid, how) do
    ref = Process.monitor(pid)
    if Process.alive?(pid), do: how.(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, @wait
  end

  defp probe(%{mechanism: mechanism, starter: starter} = run, start) do
    sides =
      for pid <- Enum.uniq(client_pid(start) ++ owners(run)), side = side(pid, starter), into: %{}, do: {pid, side}

    pids = Map.keys(sides)

    for {pid, surface, text} <- LeakProbe.surfaces(pids), do: {{mechanism, surface, sides[pid]}, text}
  end

  defp client_pid({:ok, pid}), do: [pid]
  defp client_pid(_start), do: []

  defp side(pid, starter) do
    case Process.info(pid, :parent) do
      {:parent, ^starter} -> :client
      {:parent, _other} -> :connection
      nil -> nil
    end
  end

  defp teardown(%{starter: starter} = run, start) do
    with [pid] <- client_pid(start), true <- Process.alive?(pid) do
      stop(pid, &GenServer.stop(&1, :shutdown, @wait))
    end

    Enum.each([starter | owners(run)], &stop(&1, fn pid -> Process.exit(pid, :kill) end))
  end

  defp start_texts(%{mechanism: mechanism, kase: kase}, start) do
    for text <- LeakProbe.render(start), do: {{mechanism, kase, :start_error}, text}
  end

  defp telemetry_texts(%{mechanism: mechanism, kase: kase}, telemetry) do
    for metadata <- telemetry, do: {{mechanism, kase, :telemetry}, hd(LeakProbe.render(metadata))}
  end

  defp drain_telemetry(host) do
    receive do
      {:leak_telemetry, ^host, metadata} -> [metadata | drain_telemetry(host)]
    after
      0 -> []
    end
  end

  defp sent_auth_bytes(fake) do
    for %{api: :sasl_authenticate, frame: frame} <- FakeTransport.requests(fake) do
      <<36::16, version::16, _corr::32, id_size::16, _id::binary-size(id_size), size::32, bytes::binary-size(size)>> =
        frame

      assert version in [0, 1]
      bytes
    end
  end

  defp check_controls(%{mechanism: mechanism, kase: :stacktrace_args}, _auth_bytes, _probes, _log, telemetry) do
    assert Enum.any?(telemetry, &match?(%{reason: {:handshake_failed, module}} when is_atom(module), &1)),
           "#{mechanism} stacktrace_args: the credential did not raise"
  end

  defp check_controls(%{mechanism: mechanism, kase: kase}, auth_bytes, probes, log, _telemetry) do
    assert auth_bytes != [], "#{mechanism} #{kase}: no SaslAuthenticate bytes"
    if mechanism == :plain, do: assert(Enum.uniq(auth_bytes) == [plain_payload()])
    if kase == :healthy_boot, do: assert(Enum.all?(probes, fn {_label, text} -> is_binary(text) end))

    if kase == :healthy_boot do
      assert Enum.any?(probes, &match?({{_, _, :connection}, text} when is_binary(text), &1))
    end

    if kase == :crash_after_connect, do: assert(log =~ "Ancestors:" and log =~ ":leak_probe_crash")
  end

  defp auth(:plain, :stacktrace_args) do
    AuthConfig.new(%{
      mechanism: :plain,
      username: sentinel(:plain, "USER"),
      password: {:sentinel, sentinel(:plain, "PW")}
    })
  end

  defp auth(:plain, _kase) do
    AuthConfig.new(%{mechanism: :plain, username: sentinel(:plain, "USER"), password: sentinel(:plain, "PW")})
  end

  defp auth(:scram, _kase) do
    AuthConfig.new(%{
      mechanism: :scram,
      username: sentinel(:scram, "USER"),
      password: sentinel(:scram, "PW"),
      mechanism_opts: %{algo: :sha256}
    })
  end

  defp auth(:oauthbearer, :stacktrace_args), do: oauthbearer(fn -> {:ok, String.trim(opaque(sentinel_token()))} end)
  defp auth(:oauthbearer, _kase), do: oauthbearer(fn -> {:ok, sentinel(:oauthbearer, "TOK")} end)

  defp auth(:msk_iam, _kase) do
    AuthConfig.new(%{
      mechanism: :msk_iam,
      mechanism_opts: %{
        region: "eu-west-1",
        access_key_id: sentinel(:msk_iam, "AKID"),
        secret_access_key: sentinel(:msk_iam, "SECRET"),
        session_token: sentinel(:msk_iam, "STOKEN")
      }
    })
  end

  defp oauthbearer(provider) do
    AuthConfig.new(%{mechanism: :oauthbearer, mechanism_opts: %{token_provider: provider}})
  end

  defp sentinel_token, do: {:sentinel, sentinel(:oauthbearer, "TOK")}

  defp secrets(:plain) do
    [username: sentinel(:plain, "USER"), password: sentinel(:plain, "PW"), plain_payload: plain_payload()]
  end

  defp secrets(:scram), do: [username: sentinel(:scram, "USER"), password: sentinel(:scram, "PW")]

  defp secrets(:oauthbearer) do
    [token: sentinel(:oauthbearer, "TOK"), bearer: "auth=Bearer " <> sentinel(:oauthbearer, "TOK")]
  end

  defp secrets(:msk_iam) do
    [
      access_key_id: sentinel(:msk_iam, "AKID"),
      secret_access_key: sentinel(:msk_iam, "SECRET"),
      session_token: sentinel(:msk_iam, "STOKEN")
    ]
  end

  defp plain_payload, do: <<0, sentinel(:plain, "USER")::binary, 0, sentinel(:plain, "PW")::binary>>

  defp sentinel(mechanism, kind), do: "#{kind}-#{mechanism}-7c41e9"
end
