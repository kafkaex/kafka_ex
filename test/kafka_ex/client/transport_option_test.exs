defmodule KafkaEx.Client.TransportOptionTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.TestSupport.FakeTransport

  defmodule IncompleteTransport do
    def create_socket(_host, _port, _ssl_options, _use_ssl, _auth), do: nil
    def close_socket(_broker, _socket, _reason), do: :ok
    def send_async_request(_broker, _data), do: :ok
  end

  defp start_failure(transport) do
    Process.flag(:trap_exit, true)
    Client.start_link([uris: [{"localhost", 1}], transport: transport], :no_name)
  end

  test "an atom that is not a module is rejected" do
    assert {:error, {%ArgumentError{message: message}, _}} = start_failure(:not_a_module)
    assert message =~ ":transport"
  end

  test "a module missing a transport function is rejected" do
    assert {:error, {%ArgumentError{message: message}, _}} = start_failure(IncompleteTransport)
    assert message =~ ":transport"
  end

  test "the fake accepts a missing socket as a no-op" do
    assert FakeTransport.close_socket(%{host: "h", port: 1}, nil, :reconnecting) == :ok
  end

  test "the fake refuses to close a socket from a non-owner process" do
    socket = FakeTransport.create_socket("localhost", 9)
    test_pid = self()

    {pid, ref} =
      spawn_monitor(fn ->
        send(test_pid, {:result, catch_error(FakeTransport.close_socket(%{host: "h", port: 1}, socket, :reconnecting))})
      end)

    assert_receive {:result, %ArgumentError{message: "FakeTransport socket closed from a non-owner process"}}
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
    assert FakeTransport.close_socket(%{host: "h", port: 1}, socket, :reconnecting) == :ok
  end

  test "the transport defaults to NetworkClient" do
    assert State.static_init([]).transport == NetworkClient
  end

  test "a client boots on the fake transport without closing the bootstrap sockets" do
    handler = "transport-option-#{inspect(make_ref())}"
    test_pid = self()

    :telemetry.attach(
      handler,
      [:kafka_ex, :connection, :close],
      fn _event, _measurements, metadata, _config ->
        {:links, links} = Process.info(self(), :links)
        if test_pid in links, do: send(test_pid, {:closed, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    {:ok, client} =
      Client.start_link(
        [uris: [{"localhost", 9}], consumer_group: :no_consumer_group, transport: FakeTransport],
        :no_name
      )

    assert {:ok, %{brokers: brokers}} = API.cluster_metadata(client)
    assert [%Broker{node_id: 1, host: "localhost", port: 9} = broker] = Map.values(brokers)
    assert Broker.connected?(broker)
    refute_receive {:closed, _}, 100
  end

  test "plain SASL without TLS fails the boot with plain_requires_tls" do
    Process.flag(:trap_exit, true)
    auth = KafkaEx.Auth.Config.new(%{mechanism: :plain, username: "u", password: "p"})

    log =
      capture_log(fn ->
        assert {:error, {%RuntimeError{message: "Brokers sockets are not opened"}, _}} =
                 Client.start_link(
                   [
                     uris: [{"localhost", 9}],
                     consumer_group: :no_consumer_group,
                     transport: FakeTransport,
                     use_ssl: false,
                     auth: auth
                   ],
                   :no_name
                 )
      end)

    assert log =~ "plain_requires_tls"
  end

  defmodule ApiVersionsFailingTransport do
    @api_versions_api_key 18

    defdelegate create_socket(host, port, ssl_options, use_ssl, auth), to: FakeTransport
    defdelegate close_socket(broker, socket, reason), to: FakeTransport
    defdelegate send_async_request(broker, data), to: FakeTransport

    def send_sync_request(broker, data, timeout) do
      <<api_key::16-signed, _::binary>> = IO.iodata_to_binary(data)

      if api_key == @api_versions_api_key,
        do: {:error, :api_versions_down},
        else: FakeTransport.send_sync_request(broker, data, timeout)
    end
  end

  test "a boot that fails after the bootstrap sockets opened closes each once with :init_error" do
    Process.flag(:trap_exit, true)
    test_pid = self()
    handler = "init-error-#{inspect(make_ref())}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :connection, :close],
      fn _event, _measurements, metadata, _config ->
        {:links, links} = Process.info(self(), :links)
        if test_pid in links, do: send(test_pid, {:closed, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    capture_log(fn ->
      assert {:error, _} =
               Client.start_link(
                 [
                   uris: [{"a", 1}, {"b", 2}],
                   consumer_group: :no_consumer_group,
                   transport: ApiVersionsFailingTransport
                 ],
                 :no_name
               )
    end)

    assert_receive {:closed, %{host: "a", port: 1, reason: :init_error}}
    assert_receive {:closed, %{host: "b", port: 2, reason: :init_error}}
    refute_receive {:closed, _}, 100
  end
end
