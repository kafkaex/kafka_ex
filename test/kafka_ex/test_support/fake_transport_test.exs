defmodule KafkaEx.TestSupport.FakeTransportTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias KafkaEx.API
  alias KafkaEx.Auth.Config, as: AuthConfig
  alias KafkaEx.Client
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Network.Socket
  alias KafkaEx.TestSupport.FakeTransport
  alias KafkaEx.TestSupport.FakeTransport.Wire
  alias KafkaEx.TestSupport.TLSFixtures

  @heartbeat 12

  setup_all do
    {:ok, tls: TLSFixtures.chain()}
  end

  defp metadata_v0(correlation_id), do: <<3::16, 0::16, correlation_id::32, 4::16, "test", 0::32>>
  defp api_versions_v0(correlation_id), do: <<18::16, 0::16, correlation_id::32, 4::16, "test">>

  defp decode(module, iodata), do: module.deserialize(IO.iodata_to_binary(iodata))

  defp oauth,
    do: AuthConfig.new(%{mechanism: :oauthbearer, mechanism_opts: %{token_provider: fn -> {:ok, "jwt"} end}})

  defp sasl(:plain), do: AuthConfig.new(%{mechanism: :plain, username: "u", password: "p"})

  defp sasl(algo),
    do: AuthConfig.new(%{mechanism: :scram, username: "u", password: "p", mechanism_opts: %{algo: algo}})

  defp unique_host, do: "fake-#{System.unique_integer([:positive])}"

  defp peer(fake, id), do: :sys.get_state(fake).connections[id].peer

  defp conn_apis(fake, id), do: for(%{conn: ^id, api: api} <- FakeTransport.requests(fake), do: api)

  defp bad_script(:raises), do: fn _request -> raise "boom" end
  defp bad_script(:returns_unsupported), do: fn _request -> :maybe end
  defp bad_script(:is_unencodable), do: %{brokers: [%{node_id: "seven", host: "x", port: 1}], topics: []}

  defp start_fake(opts \\ []), do: start_supervised!({FakeTransport, opts}, id: make_ref())

  defp connect(fake, host, port, opts \\ []) do
    FakeTransport.create_socket(
      host,
      port,
      FakeTransport.ssl_options(fake, Keyword.get(opts, :ssl_options, [])),
      Keyword.get(opts, :use_ssl, false),
      Keyword.get(opts, :auth)
    )
  end

  defp attach(event, host) do
    test_pid = self()
    handler = "fake-transport-#{inspect(make_ref())}"

    :telemetry.attach(
      handler,
      event,
      fn
        _event, _measurements, %{host: ^host} = metadata, _ -> send(test_pid, {:telemetry, event, metadata})
        _event, _measurements, _metadata, _ -> :ok
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp attach_here(event) do
    test_pid = self()
    handler = "fake-transport-#{inspect(make_ref())}"

    :telemetry.attach(
      handler,
      event,
      fn event, _measurements, metadata, _ ->
        if self() == test_pid, do: send(test_pid, {:telemetry, event, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  describe "tls" do
    test "use_ssl connects over a real TLS loopback the client counts as connected", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      socket = connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_ok)

      assert %Socket{ssl: true} = socket
      assert Broker.connected?(%Broker{host: "a", port: 1, socket: socket})

      assert <<1::32-signed, _::binary>> =
               FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000)
    end

    test "the client's TLS options are applied", %{tls: tls} do
      fake = start_fake(tls: tls.server)

      log =
        capture_log(fn -> assert connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_wrong_ca) == nil end)

      assert log =~ "Could not connect to broker"
    end

    test "a broker-side close arrives as ssl_closed", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      %Socket{socket: raw} = connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_ok)

      FakeTransport.close(fake, {"a", 1})

      assert_receive {:ssl_closed, ^raw}, 1_000
    end

    test "use_ssl without :tls server options fails loudly" do
      fake = start_fake()

      assert_raise ArgumentError, ~r/:tls/, fn -> connect(fake, "a", 1, use_ssl: true) end
    end

    test "use_ssl without :tls server options emits no connection exception" do
      attach_here([:kafka_ex, :connection, :exception])
      fake = start_fake()

      assert_raise ArgumentError, fn -> connect(fake, "a", 1, use_ssl: true) end

      refute_received {:telemetry, [:kafka_ex, :connection, :exception], _}
    end

    test "the creating process closes an ssl socket and the broker side sees it", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      socket = connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_ok)
      ref = Process.monitor(peer(fake, 1))

      assert FakeTransport.close_socket(%{host: "a", port: 1}, socket, :shutdown) == :ok

      assert_receive {:DOWN, ^ref, :process, _, :normal}, 1_000
    end

    test "a fake with :tls server options still accepts a plain connect", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      socket = connect(fake, "a", 1)

      assert %Socket{ssl: false} = socket

      assert <<1::32-signed, _::binary>> =
               FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000)
    end

    test "a TLS socket closed from a non-owner process raises", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      socket = connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_ok)

      task = Task.async(fn -> catch_error(FakeTransport.close_socket(%{host: "a", port: 1}, socket, :shutdown)) end)

      assert %ArgumentError{message: "FakeTransport socket closed from a non-owner process"} = Task.await(task)
    end
  end

  describe "sasl" do
    test "the handshake is answered on every connection before the socket turns active" do
      fake = start_fake()
      %Socket{socket: first} = connect(fake, "a", 1, auth: oauth())
      %Socket{socket: _second} = connect(fake, "a", 1, auth: oauth())

      assert :inet.getopts(first, [:active]) == {:ok, [active: true]}

      assert [
               {1, :api_versions},
               {1, :sasl_handshake},
               {1, :sasl_authenticate},
               {2, :api_versions},
               {2, :sasl_handshake},
               {2, :sasl_authenticate}
             ] = Enum.map(FakeTransport.requests(fake), &{&1.conn, &1.api})
    end

    test "PLAIN authenticates over TLS", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      auth = AuthConfig.new(%{mechanism: :plain, username: "u", password: "p"})

      assert %Socket{ssl: true} = connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_ok, auth: auth)
      assert conn_apis(fake, 1) == [:api_versions, :sasl_handshake, :sasl_authenticate]
    end

    for {mechanism, auth, use_ssl} <- [
          {"PLAIN over TLS", :plain, true},
          {"SCRAM-SHA-256", :sha256, false},
          {"SCRAM-SHA-512", :sha512, false}
        ] do
      test "a scripted SaslAuthenticate error fails #{mechanism}", %{tls: tls} do
        host = unique_host()
        attach([:kafka_ex, :connection, :close], host)
        fake = start_fake(tls: tls.server)
        FakeTransport.respond(fake, :sasl_authenticate, %{error_code: 58, error_message: "denied", auth_bytes: ""})
        opts = [use_ssl: unquote(use_ssl), ssl_options: tls.client_ok, auth: sasl(unquote(auth))]

        log = capture_log(fn -> assert connect(fake, host, 1, opts) == nil end)

        assert log =~
                 ~s(SASL authentication failed for broker "#{host}":1}: {:auth_failed, :sasl_authentication_failed})

        assert_receive {:telemetry, _, %{reason: :auth_error}}
      end
    end

    test "a scripted SaslAuthenticate error fails the connection" do
      host = unique_host()
      attach([:kafka_ex, :connection, :close], host)
      fake = start_fake()
      FakeTransport.respond(fake, :sasl_authenticate, %{error_code: 58, error_message: "denied", auth_bytes: ""})

      log = capture_log(fn -> assert connect(fake, host, 1, auth: oauth()) == nil end)

      assert log =~
               ~s(SASL authentication failed for broker "#{host}":1}: {:auth_failed, :sasl_authentication_failed})

      assert_receive {:telemetry, _, %{reason: :auth_error}}
    end

    test "a scripted SaslHandshake error fails the connection" do
      host = unique_host()
      attach([:kafka_ex, :connection, :close], host)
      fake = start_fake()
      FakeTransport.respond(fake, :sasl_handshake, %{error_code: 33, mechanisms: []})

      log = capture_log(fn -> assert connect(fake, host, 1, auth: oauth()) == nil end)

      assert log =~
               ~s(SASL authentication failed for broker "#{host}":1}: {:handshake_failed, :unsupported_sasl_mechanism})

      assert_receive {:telemetry, _, %{reason: :auth_error}}
    end

    test "connect and auth emit the NetworkClient spans" do
      for kind <- [:connection, :auth], phase <- [:start, :stop], do: attach_here([:kafka_ex, kind, phase])
      fake = start_fake(brokers: [{1, "a", 1}])

      connect(fake, "a", 1, auth: oauth())

      assert_received {:telemetry, [:kafka_ex, :connection, :start], %{port: 1, ssl: false}}
      assert_received {:telemetry, [:kafka_ex, :auth, :start], %{mechanism: "OAUTHBEARER"}}
      assert_received {:telemetry, [:kafka_ex, :auth, :stop], _}
      assert_received {:telemetry, [:kafka_ex, :connection, :stop], %{success: true}}

      capture_log(fn -> connect(fake, "c", 1) end)

      assert_received {:telemetry, [:kafka_ex, :connection, :stop], %{success: false, error: :econnrefused}}
    end

    test "a client boots over TLS with PLAIN", %{tls: tls} do
      fake = start_fake(tls: tls.server, brokers: [{1, "a", 9092}])
      auth = AuthConfig.new(%{mechanism: :plain, username: "u", password: "p"})

      {:ok, client} =
        Client.start_link(
          [
            uris: [{"a", 9092}],
            consumer_group: :no_consumer_group,
            transport: FakeTransport,
            use_ssl: true,
            ssl_options: FakeTransport.ssl_options(fake, tls.client_ok),
            auth: auth
          ],
          :no_name
        )

      assert {:ok, %{brokers: %{1 => broker}}} = API.cluster_metadata(client)
      assert Broker.connected?(broker)
      assert [:api_versions, :sasl_handshake, :sasl_authenticate | _] = conn_apis(fake, 1)
    end
  end

  describe "wire" do
    test "an encoded response decodes with Kayrock and carries the correlation id" do
      fields = Wire.metadata_fields([{1, "a", 9092}], %{"t" => [1]})

      assert {%Kayrock.Metadata.V0.Response{correlation_id: 7, brokers: [%{node_id: 1, host: "a", port: 9092}]}, ""} =
               decode(Kayrock.Metadata.V0.Response, Wire.response(3, 0, 7, fields))

      assert {%Kayrock.Metadata.V9.Response{correlation_id: 8, topics: [%{name: "t"}]}, ""} =
               decode(Kayrock.Metadata.V9.Response, Wire.response(3, 9, 8, fields))

      assert {%Kayrock.ApiVersions.V3.Response{correlation_id: 9, api_keys: [%{api_key: 18, max_version: 3}]}, ""} =
               decode(Kayrock.ApiVersions.V3.Response, Wire.response(18, 3, 9, Wire.api_versions_fields(%{18 => 0..3})))
    end
  end

  describe "controller" do
    test "a client in its own process is served and observed from the test" do
      fake = start_fake(brokers: [{1, "a", 9092}, {2, "b", 9093}], topics: %{"t" => [1, 2]})
      FakeTransport.subscribe(fake)

      {:ok, client} =
        Client.start_link(
          [
            uris: [{"a", 9092}],
            consumer_group: :no_consumer_group,
            transport: FakeTransport,
            ssl_options: FakeTransport.ssl_options(fake)
          ],
          :no_name
        )

      assert {:ok, %{brokers: brokers}} = API.cluster_metadata(client)

      assert brokers |> Map.values() |> Enum.map(&{&1.node_id, &1.host, &1.port}) |> Enum.sort() ==
               [{1, "a", 9092}, {2, "b", 9093}]

      assert_receive {:fake_transport, :request, %{api: :metadata, host: "a", port: 9092, node_id: 1}}
    end

    test "connect options are recorded for every attempt" do
      fake = start_fake(brokers: [{1, "a", 9092}])
      connect(fake, "a", 9092)
      capture_log(fn -> connect(fake, "c", 1) end)

      assert [
               %{host: "a", port: 9092, use_ssl: false, ssl_options: [], auth: nil, owner: owner},
               %{host: "c", port: 1}
             ] = FakeTransport.connects(fake)

      assert owner == self()
    end

    test "an address outside the broker list is refused like an unreachable broker" do
      fake = start_fake(brokers: [{1, "a", 9092}])

      log = capture_log(fn -> assert connect(fake, "c", 1) == nil end)

      assert log =~ ~s(Could not connect to broker "c":1 because of error :econnrefused)
    end

    test "the request log keeps arrival order with connection and header" do
      fake = start_fake(brokers: [{1, "a", 9092}])
      socket = connect(fake, "a", 9092)
      broker = %{host: "a", port: 9092, socket: socket}

      FakeTransport.send_sync_request(broker, metadata_v0(1), 1_000)
      FakeTransport.send_sync_request(broker, metadata_v0(2), 1_000)

      assert [
               %{
                 api: :metadata,
                 api_key: 3,
                 api_version: 0,
                 correlation_id: 1,
                 client_id: "test",
                 conn: id,
                 action: :reply
               },
               %{correlation_id: 2, conn: id}
             ] = FakeTransport.requests(fake)
    end

    test "connections are listed by id, closed ones included, without the peer" do
      fake = start_fake()
      connect(fake, "a", 1)
      connect(fake, "b", 2)

      FakeTransport.close(fake, {"a", 1})

      assert [%{id: 1, open?: false} = first, %{id: 2, open?: true} = second] = FakeTransport.connections(fake)
      refute Map.has_key?(first, :peer) or Map.has_key?(second, :peer)
    end

    test "the :api_versions option changes the ApiVersions reply" do
      fake = start_fake(api_versions: %{metadata: 0..9})
      socket = connect(fake, "a", 1)

      reply = FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, api_versions_v0(1), 1_000)

      assert {%Kayrock.ApiVersions.V0.Response{api_keys: api_keys}, ""} =
               decode(Kayrock.ApiVersions.V0.Response, reply)

      assert %{min_version: 0, max_version: 9} = Enum.find(api_keys, &(&1.api_key == 3))
    end

    test "a stray message to a peer is ignored" do
      fake = start_fake()
      socket = connect(fake, "a", 1)

      send(peer(fake, 1), :stray)

      assert <<1::32-signed, _::binary>> =
               FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000)
    end

    for bad <- [:raises, :returns_unsupported, :is_unencodable] do
      test "a reply script that #{bad} closes only its connection" do
        fake = start_fake()
        FakeTransport.respond(fake, :metadata, bad_script(unquote(bad)))
        socket = connect(fake, "a", 1)

        log =
          capture_log(fn ->
            assert FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000) ==
                     {:error, :closed}
          end)

        assert log =~ "FakeTransport could not answer :metadata: ** ("
        assert [%{api: :metadata, action: :close}] = FakeTransport.requests(fake)

        other = connect(fake, "a", 1)

        assert <<2::32-signed, _::binary>> =
                 FakeTransport.send_sync_request(%{host: "a", port: 1, socket: other}, api_versions_v0(2), 1_000)
      end
    end

    test "a stray message to the fake is ignored" do
      fake = start_fake()
      connect(fake, "a", 1)

      send(fake, :stray)

      assert [%{id: 1, open?: true}] = FakeTransport.connections(fake)
    end

    test "a frame too short for a request header closes only its connection" do
      fake = start_fake()
      socket = connect(fake, "a", 1)

      log =
        capture_log(fn ->
          assert FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, <<3::16>>, 1_000) ==
                   {:error, :closed}
        end)

      assert log =~ "FakeTransport could not parse a 2-byte request"
      other = connect(fake, "a", 1)

      assert <<2::32-signed, _::binary>> =
               FakeTransport.send_sync_request(%{host: "a", port: 1, socket: other}, api_versions_v0(2), 1_000)
    end

    test "a connect after the fake stopped is refused like an unreachable broker" do
      fake = start_fake()
      GenServer.stop(fake)

      log = capture_log(fn -> assert connect(fake, "a", 1) == nil end)

      assert log =~ ~s(Could not connect to broker "a":1 because of error :econnrefused)
    end

    test "a fake that crashes during a connect exits the caller" do
      fake = spawn(fn -> receive do: ({:"$gen_call", _from, _request} -> exit(:boom)) end)

      assert {:boom, _call} = catch_exit(connect(fake, "a", 1))
    end

    test "two fakes share no state" do
      first = start_fake()
      second = start_fake()
      socket = connect(first, "a", 1)

      FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000)

      assert [%{correlation_id: 1}] = FakeTransport.requests(first)
      assert FakeTransport.requests(second) == []
      assert FakeTransport.connects(second) == []
    end
  end

  describe "sends" do
    test "a reply comes back over the socket with the request's correlation id" do
      host = unique_host()
      socket = FakeTransport.create_socket(host, 9)

      :ok = Socket.setopts(socket, active: false)
      :ok = Socket.send(socket, metadata_v0(42))

      assert {:ok, <<42::32-signed, 1::32-signed, 1::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)

      assert <<43::32-signed, _::binary>> =
               FakeTransport.send_sync_request(%{host: host, port: 9, socket: socket}, metadata_v0(43), 1_000)
    end

    test "a send without a socket is not connected" do
      assert FakeTransport.send_sync_request(%{host: "h", port: 1, socket: nil}, metadata_v0(1), 10) ==
               {:error, :not_connected}

      assert FakeTransport.send_async_request(%{host: "h", port: 1, socket: nil}, metadata_v0(1)) ==
               {:error, :not_connected}
    end

    test "a send on a closed socket fails and emits the send_error close" do
      host = unique_host()
      attach([:kafka_ex, :connection, :close], host)
      socket = FakeTransport.create_socket(host, 9)
      Socket.close(socket)

      log =
        capture_log(fn ->
          assert FakeTransport.send_sync_request(%{host: host, port: 9, socket: socket}, metadata_v0(1), 100) ==
                   {:error, :closed}
        end)

      assert_receive {:telemetry, _, %{reason: :send_error}}
      assert log =~ "Setting socket options for broker"
    end
  end

  describe "scripting" do
    test "a fields map is encoded for the request's version and correlation id" do
      fake = start_fake()
      FakeTransport.respond(fake, :metadata, %{brokers: [%{node_id: 7, host: "x", port: 1}], topics: []})
      socket = connect(fake, "a", 1)

      assert <<5::32-signed, 1::32-signed, 7::32-signed, _::binary>> =
               FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(5), 1_000)
    end

    test "a reply function sees the request and may return a raw frame" do
      fake = start_fake()

      FakeTransport.respond(fake, :metadata, fn request ->
        {:frame, <<request.correlation_id + 1::32, 0::32, 0::32>>}
      end)

      socket = connect(fake, "a", 1)

      assert FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(5), 1_000) ==
               <<6::32, 0::32, 0::32>>
    end

    test "a raw frame may be iodata" do
      fake = start_fake()

      FakeTransport.respond(fake, :metadata, fn request ->
        {:frame, [<<request.correlation_id::32>>, [<<0::32>>], 0, 0]}
      end)

      socket = connect(fake, "a", 1)

      assert FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(5), 1_000) ==
               <<5::32, 0::32, 0, 0>>
    end

    test "one script answers every request of its API" do
      fake = start_fake()
      FakeTransport.respond(fake, :metadata, %{brokers: [], topics: []})
      socket = connect(fake, "a", 1)
      broker = %{host: "a", port: 1, socket: socket}

      for id <- 1..3 do
        assert FakeTransport.send_sync_request(broker, metadata_v0(id), 1_000) == <<id::32, 0::32, 0::32>>
      end
    end

    test "a held request is answered on release" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :metadata)
      socket = connect(fake, "a", 1)
      :ok = Socket.setopts(socket, active: false)

      :ok = Socket.send(socket, metadata_v0(1))

      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :hold}}, 1_000
      assert Socket.recv(socket, 0, 50) == {:error, :timeout}
      FakeTransport.release(fake, :metadata)
      assert {:ok, <<1::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)
    end

    test "a hold mutes its connection until release" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :metadata)
      socket = connect(fake, "a", 1)
      :ok = Socket.setopts(socket, active: false)

      :ok = Socket.send(socket, metadata_v0(1))
      :ok = Socket.send(socket, api_versions_v0(2))

      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :hold}}, 1_000
      refute_receive {:fake_transport, :request, %{correlation_id: 2}}, 100
      FakeTransport.release(fake, :metadata)
      assert {:ok, <<1::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)
      assert {:ok, <<2::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)
    end

    test "a hold leaves other connections flowing" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :metadata)
      held = connect(fake, "a", 1)
      :ok = Socket.setopts(held, active: false)
      :ok = Socket.send(held, metadata_v0(1))
      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :hold}}, 1_000

      other = connect(fake, "a", 1)

      assert <<2::32-signed, _::binary>> =
               FakeTransport.send_sync_request(%{host: "a", port: 1, socket: other}, api_versions_v0(2), 1_000)

      FakeTransport.release(fake, :metadata)
      assert {:ok, <<1::32-signed, _::binary>>} = Socket.recv(held, 0, 1_000)
    end

    test "held requests are released in arrival order" do
      test_pid = self()
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :metadata)

      sockets =
        for id <- 1..3 do
          socket = connect(fake, "a", 1)
          :ok = Socket.setopts(socket, active: false)
          :ok = Socket.send(socket, metadata_v0(id))
          assert_receive {:fake_transport, :request, %{correlation_id: ^id, action: :hold}}, 1_000
          socket
        end

      FakeTransport.respond(fake, :metadata, fn request ->
        send(test_pid, {:answered, request.correlation_id})
        %{brokers: [], topics: []}
      end)

      FakeTransport.release(fake, :metadata)

      answered =
        for _ <- 1..3 do
          assert_receive {:answered, id}
          id
        end

      assert answered == [1, 2, 3]

      for {socket, id} <- Enum.with_index(sockets, 1) do
        assert {:ok, <<^id::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)
      end
    end

    test "a script set while a request is held shapes its reply" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :metadata)
      socket = connect(fake, "a", 1)
      :ok = Socket.setopts(socket, active: false)
      :ok = Socket.send(socket, metadata_v0(1))
      assert_receive {:fake_transport, :request, %{action: :hold}}, 1_000

      FakeTransport.respond(fake, :metadata, %{brokers: [%{node_id: 7, host: "x", port: 1}], topics: []})
      FakeTransport.release(fake, :metadata)

      assert {:ok, <<1::32-signed, 1::32-signed, 7::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)
    end

    test "releasing an API that was never held changes nothing" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :metadata)
      socket = connect(fake, "a", 1)
      :ok = Socket.setopts(socket, active: false)
      :ok = Socket.send(socket, metadata_v0(1))
      assert_receive {:fake_transport, :request, %{action: :hold}}, 1_000

      assert FakeTransport.release(fake, :heartbeat) == :ok

      assert Socket.recv(socket, 0, 50) == {:error, :timeout}
      FakeTransport.release(fake, :metadata)
      assert {:ok, <<1::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)
    end

    test "release/2 after close/2 runs no reply function for the closed connection" do
      test_pid = self()
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :metadata)
      socket = connect(fake, "a", 1)
      :ok = Socket.send(socket, metadata_v0(1))
      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :hold}}, 1_000

      FakeTransport.close(fake, {"a", 1})

      FakeTransport.respond(fake, :metadata, fn request ->
        send(test_pid, {:answered, request.conn})
        %{brokers: [], topics: []}
      end)

      FakeTransport.release(fake, :metadata)

      refute_received {:answered, _}
    end

    test "a client leaving a held connection closes it before any release" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :metadata)
      socket = connect(fake, "a", 1)
      :ok = Socket.send(socket, metadata_v0(1))
      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :hold}}, 1_000
      ref = Process.monitor(peer(fake, 1))

      FakeTransport.close_socket(%{host: "a", port: 1}, socket, :timeout)

      assert_receive {:DOWN, ^ref, :process, _, :normal}, 1_000
      assert [%{id: 1, open?: false}] = FakeTransport.connections(fake)
    end

    test "release/2 runs no reply function for a request whose client left" do
      test_pid = self()
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hold(fake, :metadata)
      socket = connect(fake, "a", 1)
      :ok = Socket.send(socket, metadata_v0(1))
      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :hold}}, 1_000
      ref = Process.monitor(peer(fake, 1))
      FakeTransport.close_socket(%{host: "a", port: 1}, socket, :timeout)
      assert_receive {:DOWN, ^ref, :process, _, :normal}, 1_000

      FakeTransport.respond(fake, :metadata, fn request ->
        send(test_pid, {:answered, request.conn})
        %{brokers: [], topics: []}
      end)

      FakeTransport.release(fake, :metadata)

      refute_received {:answered, _}
    end

    for {family, use_ssl} <- [tcp: false, ssl: true] do
      test "#{family}: subscribers learn of a client leaving a held connection before any release", %{tls: tls} do
        test_pid = self()
        fake = start_fake(tls: tls.server)
        FakeTransport.subscribe(fake)
        FakeTransport.hold(fake, :metadata)
        socket = connect(fake, "a", 1, use_ssl: unquote(use_ssl), ssl_options: tls.client_ok)
        :ok = Socket.send(socket, metadata_v0(1))
        assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :hold}}, 1_000

        FakeTransport.close_socket(%{host: "a", port: 1}, socket, :timeout)
        assert_receive {:fake_transport, :closed, 1}, 1_000

        FakeTransport.respond(fake, :metadata, fn request ->
          send(test_pid, {:answered, request.conn})
          %{brokers: [], topics: []}
        end)

        FakeTransport.release(fake, :metadata)

        refute_received {:answered, _}
        assert [%{id: 1, open?: false}] = FakeTransport.connections(fake)
      end
    end

    test "an unanswered request leaves every later request on its connection unanswered" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.respond(fake, :metadata, :no_reply)
      socket = connect(fake, "a", 1)
      :ok = Socket.setopts(socket, active: false)

      :ok = Socket.send(socket, metadata_v0(1))
      :ok = Socket.send(socket, api_versions_v0(2))

      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :no_reply}}, 1_000
      refute_receive {:fake_transport, :request, %{correlation_id: 2}}, 100
      assert Socket.recv(socket, 0, 50) == {:error, :timeout}
    end

    test "a client leaving a connection with an unanswered request closes it" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.respond(fake, :metadata, :no_reply)
      socket = connect(fake, "a", 1)
      :ok = Socket.send(socket, metadata_v0(1))
      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :no_reply}}, 1_000
      ref = Process.monitor(peer(fake, 1))

      FakeTransport.close_socket(%{host: "a", port: 1}, socket, :timeout)

      assert_receive {:DOWN, ^ref, :process, _, :normal}, 1_000
      assert [%{id: 1, open?: false}] = FakeTransport.connections(fake)
    end

    test "a request that never gets a reply times out like a silent broker" do
      host = unique_host()
      attach([:kafka_ex, :connection, :close], host)
      fake = start_fake()
      FakeTransport.respond(fake, :metadata, :no_reply)
      socket = connect(fake, host, 1)

      capture_log(fn ->
        assert FakeTransport.send_sync_request(%{host: host, port: 1, socket: socket}, metadata_v0(1), 50) ==
                 {:error, :timeout}
      end)

      assert_receive {:telemetry, _, %{reason: :timeout}}
    end

    test "a scripted close drops the connection on that request" do
      fake = start_fake()
      FakeTransport.respond(fake, :metadata, :close)
      socket = connect(fake, "a", 1)

      capture_log(fn ->
        assert FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000) ==
                 {:error, :closed}
      end)
    end

    test "close/2 closes a broker's connections from the broker side" do
      fake = start_fake()
      %Socket{socket: raw} = connect(fake, "a", 1)

      FakeTransport.close(fake, {"a", 1})

      assert_receive {:tcp_closed, ^raw}
    end

    test "close/2 returns after the owner has the closed message" do
      fake = start_fake()
      %Socket{socket: raw} = connect(fake, "a", 1)

      FakeTransport.close(fake, {"a", 1})

      assert_received {:tcp_closed, ^raw}
    end

    test "an unscripted API closes its connection and is logged" do
      fake = start_fake()
      socket = connect(fake, "a", 1)
      heartbeat = <<@heartbeat::16, 0::16, 3::32, 4::16, "test">>

      capture_log(fn ->
        assert FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, heartbeat, 1_000) ==
                 {:error, :closed}
      end)

      assert [%{api: :heartbeat, action: :unscripted}] = FakeTransport.requests(fake)
    end
  end

  describe "messages" do
    for {family, use_ssl} <- [tcp: false, ssl: true] do
      test "#{family}: at most one message per active-once re-arm", %{tls: tls} do
        fake = start_fake(tls: tls.server)
        FakeTransport.subscribe(fake)
        FakeTransport.hold(fake, :metadata)
        socket = connect(fake, "a", 1, use_ssl: unquote(use_ssl), ssl_options: tls.client_ok)
        %Socket{socket: raw} = socket
        :ok = Socket.setopts(socket, active: false)
        :ok = Socket.send(socket, metadata_v0(1))
        :ok = Socket.send(socket, metadata_v0(2))
        assert_receive {:fake_transport, :request, %{correlation_id: 1}}, 1_000

        FakeTransport.release(fake, :metadata)
        assert_receive {:fake_transport, :request, %{correlation_id: 2}}, 1_000
        :ok = Socket.setopts(socket, active: :once)

        assert_receive {unquote(family), ^raw, <<1::32-signed, _::binary>>}, 1_000
        refute_receive {unquote(family), _, _}, 100
        :ok = Socket.setopts(socket, active: :once)
        assert_receive {unquote(family), ^raw, <<2::32-signed, _::binary>>}, 1_000
      end
    end

    test "a stale socket's message can be injected after a reconnect" do
      fake = start_fake()
      old = connect(fake, "a", 1)
      FakeTransport.close_socket(%{host: "a", port: 1}, old, :reconnecting)
      new = connect(fake, "a", 1)

      assert [%{socket: ^old} = stale, %{socket: ^new}] = FakeTransport.connections(fake)

      raw = old.socket
      refute_received {:tcp_closed, ^raw}
      FakeTransport.inject(stale, :closed)
      FakeTransport.inject(stale, {:data, "late"})

      assert_receive {:tcp_closed, ^raw}
      assert_receive {:tcp, ^raw, "late"}
      new_raw = new.socket
      :ok = Socket.send(new, metadata_v0(9))
      assert_receive {:tcp, ^new_raw, <<9::32-signed, _::binary>>}, 1_000
    end
  end

  describe "send faults" do
    test "a hung send blocks the sender until release" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.hang_send(fake, :metadata)

      task =
        Task.async(fn ->
          socket = connect(fake, "a", 1)
          FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000)
        end)

      refute Task.yield(task, 100)
      refute_received {:fake_transport, :request, _}
      FakeTransport.release(fake, :metadata)
      assert <<1::32-signed, _::binary>> = Task.await(task)
    end

    test "a partial send fails like NetworkClient and leaves no complete request" do
      host = unique_host()
      attach([:kafka_ex, :connection, :close], host)
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.fail_send(fake, :metadata, 3, :closed)
      socket = connect(fake, host, 1)

      log =
        capture_log(fn ->
          assert FakeTransport.send_sync_request(%{host: host, port: 1, socket: socket}, metadata_v0(1), 1_000) ==
                   {:error, :closed}
        end)

      assert log =~ ~s(Sending data to broker "#{host}":1 failed with :closed)
      assert_receive {:telemetry, _, %{reason: :send_error}}
      refute Socket.open?(socket)
      refute_receive {:fake_transport, :request, _}, 100
    end

    test "a partial async send fails like NetworkClient" do
      fake = start_fake()
      FakeTransport.fail_send(fake, :metadata, 3, :econnreset)
      socket = connect(fake, "a", 1)

      log =
        capture_log(fn ->
          assert FakeTransport.send_async_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1)) ==
                   {:error, :econnreset}
        end)

      assert log =~ ~s(Asynchronously sending data to broker "a":1 failed with :econnreset)
    end

    test "a partial send on a connection the broker closed still fails like NetworkClient" do
      host = unique_host()
      attach([:kafka_ex, :connection, :close], host)
      fake = start_fake()
      FakeTransport.fail_send(fake, :metadata, 3, :closed)
      %Socket{socket: raw} = socket = connect(fake, host, 1)
      FakeTransport.close(fake, {host, 1})
      assert_receive {:tcp_closed, ^raw}

      log =
        capture_log(fn ->
          assert FakeTransport.send_sync_request(%{host: host, port: 1, socket: socket}, metadata_v0(1), 1_000) ==
                   {:error, :closed}
        end)

      assert log =~ ~s(Sending data to broker "#{host}":1 failed with :closed)
      assert_receive {:telemetry, _, %{reason: :send_error}}
    end

    test "a partial send never writes the whole frame" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.fail_send(fake, :metadata, 1_000, :closed)
      socket = connect(fake, "a", 1)

      capture_log(fn ->
        assert FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000) ==
                 {:error, :closed}
      end)

      refute_receive {:fake_transport, :request, _}, 100
    end

    test "fail_send/4 rejects a negative byte count in the caller" do
      fake = start_fake()

      assert_raise FunctionClauseError, fn -> FakeTransport.fail_send(fake, :metadata, -1, :closed) end
    end

    test "release/2 disarms fail_send/4" do
      fake = start_fake()
      FakeTransport.fail_send(fake, :metadata, 3, :closed)
      FakeTransport.release(fake, :metadata)
      socket = connect(fake, "a", 1)

      assert <<1::32-signed, _::binary>> =
               FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000)
    end

    test "a send after the fake stopped behaves like NetworkClient" do
      fake = start_fake()
      %Socket{socket: raw} = socket = connect(fake, "a", 1)
      GenServer.stop(fake)
      assert_receive {:tcp_closed, ^raw}, 1_000

      capture_log(fn ->
        assert FakeTransport.send_sync_request(%{host: "a", port: 1, socket: socket}, metadata_v0(1), 1_000) ==
                 {:error, :closed}
      end)
    end
  end
end
