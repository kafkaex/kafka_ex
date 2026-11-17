defmodule KafkaEx.TestSupport.FakeTransportTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias KafkaEx.Network.Socket
  alias KafkaEx.TestSupport.FakeTransport
  alias KafkaEx.TestSupport.FakeTransport.Wire

  defp metadata_v0(correlation_id), do: <<3::16, 0::16, correlation_id::32, 4::16, "test", 0::32>>

  defp decode(module, iodata), do: module.deserialize(IO.iodata_to_binary(iodata))

  defp unique_host, do: "fake-#{System.unique_integer([:positive])}"

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
end
