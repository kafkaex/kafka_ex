defmodule KafkaEx.Integration.Lifecycle.TelemetryTest do
  use ExUnit.Case, async: true
  @moduletag :lifecycle

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Telemetry

  describe "telemetry spans" do
    test "request span emits start and stop events" do
      ref = make_ref()
      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, ref, event, measurements, metadata})
      end

      :telemetry.attach_many(ref, Telemetry.request_events(), handler, nil)

      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, client} = Client.start_link(args, :no_name)

      # Flush startup events
      flush_messages(ref)

      {:ok, _} = API.metadata(client)

      # Verify start event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :request, :start], start_measurements, start_metadata}, 5000
      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.operation == :metadata
      assert is_integer(start_metadata.api_version)
      assert is_integer(start_metadata.correlation_id)
      assert is_binary(start_metadata.client_id)

      # Verify stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :request, :stop], stop_measurements, _}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0

      :telemetry.detach(ref)
      GenServer.stop(client)
    end

    test "connection span emits start and stop events" do
      ref = make_ref()
      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, ref, event, measurements, metadata})
      end

      :telemetry.attach_many(ref, Telemetry.connection_events(), handler, nil)

      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, client} = Client.start_link(args, :no_name)

      # Verify start event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :connection, :start], start_measurements, start_metadata}, 5000
      assert Map.has_key?(start_measurements, :system_time)
      assert is_binary(start_metadata.host)
      assert is_integer(start_metadata.port)
      assert is_boolean(start_metadata.ssl)

      # Verify stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :connection, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0
      assert stop_metadata.success == true

      :telemetry.detach(ref)
      GenServer.stop(client)
    end
  end

  defp flush_messages(ref) do
    receive do
      {:telemetry, ^ref, _, _, _} -> flush_messages(ref)
    after
      0 -> :ok
    end
  end
end
