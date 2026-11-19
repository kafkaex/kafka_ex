defmodule KafkaEx.Client.TransportOptionTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.Connection
  alias KafkaEx.Client.Infra
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.TestSupport.FakeTransport
  alias KafkaEx.TestSupport.PinCase

  defmodule IncompleteTransport do
    def connect(_address, _socket_options, _use_ssl, _timeout), do: {:error, :econnrefused}
    def write(_socket, _data), do: :ok
    def read(_socket, _timeout), do: {:error, :closed}
    def activate(_socket), do: :ok
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

    # Signals from one process arrive in order, so its :result is in the mailbox once its :DOWN is.
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000

    assert_received {:result, %ArgumentError{message: "FakeTransport socket closed from a non-owner process"}}
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
        if test_pid in Process.get(:"$ancestors", []), do: send(test_pid, {:closed, metadata})
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
    assert broker.socket == nil
    refute_receive {:closed, _}, 100

    GenServer.stop(client)
    assert_receive {:closed, %{reason: :shutdown}}, 1_000
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

  test "a boot that fails closes every connection it opened, once, and leaves no process behind" do
    Process.flag(:trap_exit, true)
    %{fake: fake, host: host} = PinCase.start_fake([{1, 9092}, {2, 9093}], api_versions: %{metadata: 12..12})
    FakeTransport.respond(fake, :metadata, &{:frame, PinCase.null_topics_metadata_frame(&1)})
    test_pid = self()
    handler = "init-error-#{inspect(make_ref())}"

    :telemetry.attach(
      handler,
      [:kafka_ex, :connection, :close],
      fn _event, _measurements, metadata, _config ->
        if metadata.host == host, do: send(test_pid, {:closed, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    capture_log(fn ->
      assert {:error, _} =
               Client.start_link(
                 [
                   uris: [{host, 9092}, {host, 9093}],
                   consumer_group: :no_consumer_group,
                   transport: FakeTransport,
                   ssl_options: FakeTransport.ssl_options(fake)
                 ],
                 :no_name
               )
    end)

    assert_receive {:closed, %{port: 9092, reason: :init_error}}, 5_000
    refute_receive {:closed, _}, 100

    connections = fake |> FakeTransport.connects() |> Enum.map(& &1.owner)
    assert connections != []
    refute Enum.any?(connections, &Process.alive?/1)
    assert left_behind() == []
  end

  defp left_behind do
    for pid <- Process.list(),
        {:dictionary, dictionary} <- [Process.info(pid, :dictionary)],
        self() in Keyword.get(dictionary, :"$ancestors", []),
        Keyword.get(dictionary, :"$initial_call") in [
          {Connection, :init, 1},
          {:supervisor, Infra, 1},
          {:supervisor, Supervisor.Default, 1}
        ],
        do: pid
  end
end
