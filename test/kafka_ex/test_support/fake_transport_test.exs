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

  defp produce(version, acks, correlation_id) do
    [Kayrock.Produce, :"V#{version}", Request]
    |> Module.concat()
    |> struct(acks: acks, timeout: 1_000, timeout_ms: 1_000, transactional_id: "tx", topic_data: [])
    |> struct(correlation_id: correlation_id, client_id: "test")
    |> Kayrock.Request.serialize()
  end

  defp decode(module, iodata), do: module.deserialize(IO.iodata_to_binary(iodata))

  defp peer(fake, id), do: :sys.get_state(fake).connections[id].peer

  defp conn_apis(fake, id), do: for(%{conn: ^id, api: api} <- FakeTransport.requests(fake), do: api)

  defp bad_script(:raises), do: fn _request -> raise "boom" end
  defp bad_script(:returns_unsupported), do: fn _request -> :maybe end
  defp bad_script(:is_unencodable), do: %{brokers: [%{node_id: "seven", host: "x", port: 1}], topics: []}

  defp start_fake(opts \\ []), do: start_supervised!({FakeTransport, opts}, id: make_ref())

  defp connect(fake, host, port, opts \\ []) do
    case seam_connect(fake, host, port, opts) do
      {:ok, socket} -> socket
      {:error, _reason} -> nil
    end
  end

  defp request(socket, data, timeout) do
    with :ok <- FakeTransport.write(socket, data), {:ok, reply} <- FakeTransport.read(socket, timeout), do: reply
  end

  defp ssl_options(opts), do: if(Keyword.get(opts, :use_ssl), do: Keyword.get(opts, :ssl_options, []), else: [])

  defp seam_connect(fake, host, port, opts \\ []) do
    FakeTransport.connect(
      %{host: host, port: port, role: Keyword.get(opts, :role, :data)},
      FakeTransport.ssl_options(
        fake,
        [:binary, {:packet, 4}, {:active, false}] ++
          Keyword.get(opts, :socket_options, []) ++ ssl_options(opts)
      ),
      Keyword.get(opts, :use_ssl, false),
      Keyword.get(opts, :timeout, 1_000)
    )
  end

  describe "tls" do
    test "use_ssl connects over a real TLS loopback the client counts as connected", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      socket = connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_ok)

      assert %Socket{ssl: true} = socket
      assert Broker.connected?(%Broker{host: "a", port: 1, socket: socket})

      assert <<1::32-signed, _::binary>> =
               request(socket, metadata_v0(1), 1_000)
    end

    test "the client's TLS options are applied", %{tls: tls} do
      fake = start_fake(tls: tls.server)

      assert {:error, _reason} = seam_connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_wrong_ca)
    end

    test "a broker-side close arrives as ssl_closed", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      %Socket{socket: raw} = socket = connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_ok)
      :ok = FakeTransport.activate(socket)

      FakeTransport.close(fake, {"a", 1})

      assert_receive {:ssl_closed, ^raw}, 1_000
    end

    test "use_ssl without :tls server options fails loudly" do
      fake = start_fake()

      assert_raise ArgumentError, ~r/:tls/, fn -> connect(fake, "a", 1, use_ssl: true) end
    end

    test "the creating process closes an ssl socket and the broker side sees it", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      socket = connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_ok)
      ref = Process.monitor(peer(fake, 1))

      assert FakeTransport.close(socket) == :ok

      assert_receive {:DOWN, ^ref, :process, _, :normal}, 1_000
    end

    test "a fake with :tls server options still accepts a plain connect", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      socket = connect(fake, "a", 1)

      assert %Socket{ssl: false} = socket

      assert <<1::32-signed, _::binary>> =
               request(socket, metadata_v0(1), 1_000)
    end
  end

  describe "sasl" do
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
      assert broker.socket == nil
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
               %{host: "a", port: 9092, use_ssl: false, auth: nil, owner: owner},
               %{host: "c", port: 1}
             ] = FakeTransport.connects(fake)

      assert owner == self()
    end

    test "an address outside the broker list is refused like an unreachable broker" do
      fake = start_fake(brokers: [{1, "a", 9092}])

      assert seam_connect(fake, "c", 1) == {:error, :econnrefused}
    end

    test "the request log keeps arrival order with connection and header" do
      fake = start_fake(brokers: [{1, "a", 9092}])
      socket = connect(fake, "a", 9092)

      request(socket, metadata_v0(1), 1_000)
      request(socket, metadata_v0(2), 1_000)

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

      reply = request(socket, api_versions_v0(1), 1_000)

      assert {%Kayrock.ApiVersions.V0.Response{api_keys: api_keys}, ""} =
               decode(Kayrock.ApiVersions.V0.Response, reply)

      assert %{min_version: 0, max_version: 9} = Enum.find(api_keys, &(&1.api_key == 3))
    end

    test "a stray message to a peer is ignored" do
      fake = start_fake()
      socket = connect(fake, "a", 1)

      send(peer(fake, 1), :stray)

      assert <<1::32-signed, _::binary>> =
               request(socket, metadata_v0(1), 1_000)
    end

    for bad <- [:raises, :returns_unsupported, :is_unencodable] do
      test "a reply script that #{bad} closes only its connection" do
        fake = start_fake()
        FakeTransport.respond(fake, :metadata, bad_script(unquote(bad)))
        socket = connect(fake, "a", 1)

        log =
          capture_log(fn ->
            assert request(socket, metadata_v0(1), 1_000) ==
                     {:error, :closed}
          end)

        assert log =~ "FakeTransport could not answer :metadata: ** ("
        assert [%{api: :metadata, action: :close}] = FakeTransport.requests(fake)

        other = connect(fake, "a", 1)

        assert <<2::32-signed, _::binary>> =
                 request(other, api_versions_v0(2), 1_000)
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
          assert request(socket, <<3::16>>, 1_000) ==
                   {:error, :closed}
        end)

      assert log =~ "FakeTransport could not parse a 2-byte request"
      other = connect(fake, "a", 1)

      assert <<2::32-signed, _::binary>> =
               request(other, api_versions_v0(2), 1_000)
    end

    test "a connect after the fake stopped is refused like an unreachable broker" do
      fake = start_fake()
      GenServer.stop(fake)

      assert seam_connect(fake, "a", 1) == {:error, :econnrefused}
    end

    test "a fake that crashes during a connect exits the caller" do
      fake = spawn(fn -> receive do: ({:"$gen_call", _from, _request} -> exit(:boom)) end)

      assert {:boom, _call} = catch_exit(connect(fake, "a", 1))
    end

    test "two fakes share no state" do
      first = start_fake()
      second = start_fake()
      socket = connect(first, "a", 1)

      request(socket, metadata_v0(1), 1_000)

      assert [%{correlation_id: 1}] = FakeTransport.requests(first)
      assert FakeTransport.requests(second) == []
      assert FakeTransport.connects(second) == []
    end
  end

  describe "sends" do
    test "a reply comes back over the socket with the request's correlation id" do
      socket = connect(start_fake(), "a", 9)

      :ok = Socket.setopts(socket, active: false)
      :ok = Socket.send(socket, metadata_v0(42))

      assert {:ok, <<42::32-signed, 1::32-signed, 1::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)

      assert <<43::32-signed, _::binary>> =
               request(socket, metadata_v0(43), 1_000)
    end
  end

  describe "scripting" do
    test "a fields map is encoded for the request's version and correlation id" do
      fake = start_fake()
      FakeTransport.respond(fake, :metadata, %{brokers: [%{node_id: 7, host: "x", port: 1}], topics: []})
      socket = connect(fake, "a", 1)

      assert <<5::32-signed, 1::32-signed, 7::32-signed, _::binary>> =
               request(socket, metadata_v0(5), 1_000)
    end

    test "a reply function sees the request and may return a raw frame" do
      fake = start_fake()

      FakeTransport.respond(fake, :metadata, fn request ->
        {:frame, <<request.correlation_id + 1::32, 0::32, 0::32>>}
      end)

      socket = connect(fake, "a", 1)

      assert request(socket, metadata_v0(5), 1_000) ==
               <<6::32, 0::32, 0::32>>
    end

    test "a raw frame may be iodata" do
      fake = start_fake()

      FakeTransport.respond(fake, :metadata, fn request ->
        {:frame, [<<request.correlation_id::32>>, [<<0::32>>], 0, 0]}
      end)

      socket = connect(fake, "a", 1)

      assert request(socket, metadata_v0(5), 1_000) ==
               <<5::32, 0::32, 0, 0>>
    end

    test "one script answers every request of its API" do
      fake = start_fake()
      FakeTransport.respond(fake, :metadata, %{brokers: [], topics: []})
      socket = connect(fake, "a", 1)

      for id <- 1..3 do
        assert request(socket, metadata_v0(id), 1_000) == <<id::32, 0::32, 0::32>>
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
               request(other, api_versions_v0(2), 1_000)

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

      FakeTransport.close(socket)

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
      FakeTransport.close(socket)
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

        FakeTransport.close(socket)
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

    for version <- [0, 3, 9], script <- [:none, :no_reply, :close] do
      test "v#{version} #{script}: an acks=0 produce gets no reply and its connection keeps answering" do
        fake = start_fake()
        if unquote(script) != :none, do: FakeTransport.respond(fake, :produce, unquote(script))
        socket = connect(fake, "a", 1)
        :ok = Socket.setopts(socket, active: false)

        :ok = Socket.send(socket, produce(unquote(version), 0, 1))
        :ok = Socket.send(socket, metadata_v0(2))

        assert {:ok, <<2::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)

        assert [%{correlation_id: 1, api: :produce, action: :acks0}, %{correlation_id: 2, action: :reply}] =
                 FakeTransport.requests(fake)
      end
    end

    for {version, acks} <- [{3, 1}, {9, -1}] do
      test "v#{version}: an unanswered produce with acks=#{acks} still mutes its connection" do
        fake = start_fake()
        FakeTransport.subscribe(fake)
        FakeTransport.respond(fake, :produce, :no_reply)
        socket = connect(fake, "a", 1)
        :ok = Socket.setopts(socket, active: false)

        :ok = Socket.send(socket, produce(unquote(version), unquote(acks), 1))
        :ok = Socket.send(socket, metadata_v0(2))

        assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :no_reply}}, 1_000
        refute_receive {:fake_transport, :request, %{correlation_id: 2}}, 100
        assert Socket.recv(socket, 0, 50) == {:error, :timeout}
      end
    end

    test "a client leaving a connection with an unanswered request closes it" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.respond(fake, :metadata, :no_reply)
      socket = connect(fake, "a", 1)
      :ok = Socket.send(socket, metadata_v0(1))
      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :no_reply}}, 1_000
      ref = Process.monitor(peer(fake, 1))

      FakeTransport.close(socket)

      assert_receive {:DOWN, ^ref, :process, _, reason}, 5_000
      assert reason == :normal, "peer stopped early: #{inspect(FakeTransport.connections(fake))}"
      assert [%{id: 1, open?: false}] = FakeTransport.connections(fake)
    end

    test "a request that never gets a reply times out like a silent broker" do
      fake = start_fake()
      FakeTransport.respond(fake, :metadata, :no_reply)
      socket = connect(fake, "a", 1)

      assert request(socket, metadata_v0(1), 50) == {:error, :timeout}
    end

    test "a client closing the socket records why the peer stopped" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      socket = connect(fake, "a", 1)

      FakeTransport.close(socket)

      assert_receive {:fake_transport, :closed, 1}, 5_000
      assert [%{close_reason: {:socket_closed, :tcp_closed}}] = FakeTransport.connections(fake)
    end

    test "a scripted close records why the peer stopped" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.respond(fake, :metadata, :close)
      socket = connect(fake, "a", 1)

      capture_log(fn ->
        request(socket, metadata_v0(1), 1_000)
      end)

      assert_receive {:fake_transport, :closed, 1}, 5_000
      assert [%{close_reason: :scripted_close}] = FakeTransport.connections(fake)
    end

    test "a scripted close drops the connection on that request" do
      fake = start_fake()
      FakeTransport.respond(fake, :metadata, :close)
      socket = connect(fake, "a", 1)

      capture_log(fn ->
        assert request(socket, metadata_v0(1), 1_000) ==
                 {:error, :closed}
      end)
    end

    test "close/2 closes a broker's connections from the broker side" do
      fake = start_fake()
      %Socket{socket: raw} = socket = connect(fake, "a", 1)
      :ok = FakeTransport.activate(socket)

      FakeTransport.close(fake, {"a", 1})

      assert_receive {:tcp_closed, ^raw}
    end

    test "close/2 returns after the owner has the closed message" do
      fake = start_fake()
      %Socket{socket: raw} = socket = connect(fake, "a", 1)
      :ok = FakeTransport.activate(socket)

      FakeTransport.close(fake, {"a", 1})

      assert_received {:tcp_closed, ^raw}
    end

    test "an unscripted API closes its connection and is logged" do
      fake = start_fake()
      socket = connect(fake, "a", 1)
      heartbeat = <<@heartbeat::16, 0::16, 3::32, 4::16, "test">>

      capture_log(fn ->
        assert request(socket, heartbeat, 1_000) ==
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
      FakeTransport.close(old)
      new = connect(fake, "a", 1)

      assert [%{socket: ^old} = stale, %{socket: ^new}] = FakeTransport.connections(fake)

      raw = old.socket
      refute_received {:tcp_closed, ^raw}
      FakeTransport.inject(stale, :closed)
      FakeTransport.inject(stale, {:data, "late"})

      assert_receive {:tcp_closed, ^raw}
      assert_receive {:tcp, ^raw, "late"}
      new_raw = new.socket
      :ok = Socket.setopts(new, active: true)
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
          request(socket, metadata_v0(1), 1_000)
        end)

      refute Task.yield(task, 100)
      refute_received {:fake_transport, :request, _}
      FakeTransport.release(fake, :metadata)
      assert <<1::32-signed, _::binary>> = Task.await(task)
    end

    test "a partial send fails like NetworkClient and leaves no complete request" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.fail_send(fake, :metadata, 3, :closed)
      socket = connect(fake, "a", 1)

      assert request(socket, metadata_v0(1), 1_000) == {:error, :closed}
      refute Socket.open?(socket)
      refute_receive {:fake_transport, :request, _}, 100
    end

    test "a partial send on a connection the broker closed still fails like NetworkClient" do
      fake = start_fake()
      FakeTransport.fail_send(fake, :metadata, 3, :closed)
      socket = connect(fake, "a", 1)
      FakeTransport.close(fake, {"a", 1})

      assert request(socket, metadata_v0(1), 1_000) == {:error, :closed}
    end

    test "a partial send never writes the whole frame" do
      fake = start_fake()
      FakeTransport.subscribe(fake)
      FakeTransport.fail_send(fake, :metadata, 1_000, :closed)
      socket = connect(fake, "a", 1)

      assert request(socket, metadata_v0(1), 1_000) == {:error, :closed}

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
               request(socket, metadata_v0(1), 1_000)
    end

    test "a send after the fake stopped behaves like NetworkClient" do
      fake = start_fake()
      socket = connect(fake, "a", 1)
      GenServer.stop(fake)

      assert request(socket, metadata_v0(1), 1_000) == {:error, :closed}
    end
  end

  describe "the low-level seam" do
    test "a frame written on the seam is answered on read" do
      fake = start_fake()
      {:ok, socket} = seam_connect(fake, "a", 1)

      assert FakeTransport.write(socket, metadata_v0(5)) == :ok
      assert {:ok, <<5::32-signed, _::binary>>} = FakeTransport.read(socket, 1_000)
      assert [%{api: :metadata}] = FakeTransport.requests(fake)
    end

    test "an ApiVersions request at any version is answered" do
      fake = start_fake()
      {:ok, socket} = seam_connect(fake, "a", 1)

      :ok = FakeTransport.write(socket, <<18::16, 3::16, 7::32, 4::16, "test", 0, 0, 0>>)
      assert {:ok, <<7::32-signed, _::binary>>} = FakeTransport.read(socket, 1_000)
    end
  end
end
