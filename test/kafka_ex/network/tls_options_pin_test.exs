defmodule KafkaEx.Network.TLSOptionsPinTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias KafkaEx.Network.NetworkClient
  alias KafkaEx.Network.Socket
  alias KafkaEx.TestSupport.TLSFixtures

  setup do
    tls = TLSFixtures.chain()
    {:ok, port, stop} = TLSFixtures.start_acceptor(tls.server)
    on_exit(stop)
    %{tls: tls, port: port}
  end

  test "verify_peer with the right CA connects", %{tls: tls, port: port} do
    assert %Socket{ssl: true} =
             socket = NetworkClient.create_socket("localhost", port, tls.client_ok, true)

    Socket.close(socket)
  end

  test "verify_peer with a wrong CA fails, so the ssl options reach the handshake", %{
    tls: tls,
    port: port
  } do
    log =
      capture_log(fn ->
        assert NetworkClient.create_socket("localhost", port, tls.client_wrong_ca, true) == nil
      end)

    assert log =~ "unknown_ca"
  end

  test "the client hands its ssl options to the handshake", %{tls: tls, port: port} do
    Process.flag(:trap_exit, true)

    log =
      capture_log(fn ->
        assert {:error, {%RuntimeError{message: "Brokers sockets are not opened"}, _}} =
                 KafkaEx.Client.start_link(
                   [
                     uris: [{"localhost", port}],
                     consumer_group: :no_consumer_group,
                     use_ssl: true,
                     ssl_options: tls.client_wrong_ca
                   ],
                   :no_name
                 )
      end)

    assert log =~ "unknown_ca"
  end
end
