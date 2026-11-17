defmodule KafkaEx.TestSupport.FakeTransportTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias KafkaEx.API
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

  defp decode(module, iodata), do: module.deserialize(IO.iodata_to_binary(iodata))

  defp unique_host, do: "fake-#{System.unique_integer([:positive])}"

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

      assert_receive {:ssl_closed, ^raw}
    end

    test "use_ssl without :tls server options fails loudly" do
      fake = start_fake()

      assert_raise ArgumentError, ~r/:tls/, fn -> connect(fake, "a", 1, use_ssl: true) end
    end

    test "a TLS socket closed from a non-owner process raises", %{tls: tls} do
      fake = start_fake(tls: tls.server)
      socket = connect(fake, "a", 1, use_ssl: true, ssl_options: tls.client_ok)

      task = Task.async(fn -> catch_error(FakeTransport.close_socket(%{host: "a", port: 1}, socket, :shutdown)) end)

      assert %ArgumentError{message: "FakeTransport socket closed from a non-owner process"} = Task.await(task)
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
    test "a client in its own process is scripted from the test" do
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

      assert_receive {:fake_transport, :request, %{correlation_id: 1, action: :hold}}
      assert Socket.recv(socket, 0, 50) == {:error, :timeout}
      FakeTransport.release(fake, :metadata)
      assert {:ok, <<1::32-signed, _::binary>>} = Socket.recv(socket, 0, 1_000)
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
end
