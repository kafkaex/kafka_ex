defmodule KafkaEx.TelemetryTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Telemetry

  describe "events/0" do
    test "returns all event names as lists of atoms" do
      events = Telemetry.events()

      assert is_list(events)
      assert length(events) > 0

      Enum.each(events, fn event ->
        assert is_list(event)
        assert hd(event) == :kafka_ex
        Enum.each(event, &assert(is_atom(&1)))
      end)
    end

    test "includes request events" do
      events = Telemetry.events()

      assert [:kafka_ex, :request, :start] in events
      assert [:kafka_ex, :request, :stop] in events
      assert [:kafka_ex, :request, :exception] in events
    end

    test "includes connection events" do
      events = Telemetry.events()

      assert [:kafka_ex, :connection, :start] in events
      assert [:kafka_ex, :connection, :stop] in events
      assert [:kafka_ex, :connection, :exception] in events
    end

    test "includes produce events" do
      events = Telemetry.events()

      assert [:kafka_ex, :produce, :start] in events
      assert [:kafka_ex, :produce, :stop] in events
      assert [:kafka_ex, :produce, :exception] in events
    end

    test "includes fetch events" do
      events = Telemetry.events()

      assert [:kafka_ex, :fetch, :start] in events
      assert [:kafka_ex, :fetch, :stop] in events
      assert [:kafka_ex, :fetch, :exception] in events
    end

    test "includes consumer events" do
      events = Telemetry.events()

      assert [:kafka_ex, :consumer, :commit, :start] in events
      assert [:kafka_ex, :consumer, :commit, :stop] in events
      assert [:kafka_ex, :consumer, :commit, :exception] in events
    end
  end

  describe "request_events/0" do
    test "returns only request events" do
      events = Telemetry.request_events()

      assert length(events) == 3

      Enum.each(events, fn event ->
        assert Enum.at(event, 1) == :request
      end)
    end
  end

  describe "connection_events/0" do
    test "returns only connection events" do
      events = Telemetry.connection_events()

      assert length(events) == 3

      Enum.each(events, fn event ->
        assert Enum.at(event, 1) == :connection
      end)
    end
  end

  describe "produce_events/0" do
    test "returns only produce events" do
      events = Telemetry.produce_events()

      assert length(events) == 3

      Enum.each(events, fn event ->
        assert Enum.at(event, 1) == :produce
      end)
    end
  end

  describe "fetch_events/0" do
    test "returns only fetch events" do
      events = Telemetry.fetch_events()

      assert length(events) == 3

      Enum.each(events, fn event ->
        assert Enum.at(event, 1) == :fetch
      end)
    end
  end

  describe "consumer_events/0" do
    test "returns only consumer events" do
      events = Telemetry.consumer_events()

      assert length(events) == 3
      assert [:kafka_ex, :consumer, :commit, :start] in events
      assert [:kafka_ex, :consumer, :commit, :stop] in events
      assert [:kafka_ex, :consumer, :commit, :exception] in events
    end
  end

  describe "request_metadata/2" do
    test "extracts operation from real Kayrock request struct" do
      # Use real Kayrock request struct
      request = %Kayrock.Metadata.V1.Request{
        client_id: "test_client",
        correlation_id: 42,
        topics: []
      }

      metadata = Telemetry.request_metadata(request, %{host: "localhost", port: 9092})

      assert metadata.operation == :metadata
      assert metadata.api_version == 1
      assert metadata.correlation_id == 42
      assert metadata.client_id == "test_client"
      assert metadata.broker == %{host: "localhost", port: 9092}
    end

    test "extracts operation from different Kayrock request types" do
      produce_request = %Kayrock.Produce.V2.Request{
        client_id: "producer",
        correlation_id: 1,
        acks: 1,
        timeout: 5000,
        topic_data: []
      }

      fetch_request = %Kayrock.Fetch.V3.Request{
        client_id: "consumer",
        correlation_id: 2,
        replica_id: -1,
        max_wait_time: 500,
        min_bytes: 1,
        max_bytes: 1_000_000,
        topics: []
      }

      produce_meta = Telemetry.request_metadata(produce_request, %{})
      fetch_meta = Telemetry.request_metadata(fetch_request, %{})

      assert produce_meta.operation == :produce
      assert produce_meta.api_version == 2

      assert fetch_meta.operation == :fetch
      assert fetch_meta.api_version == 3
    end

    test "handles unknown request struct gracefully" do
      request = %{
        __struct__: SomeUnknown.Module,
        client_id: "test",
        correlation_id: 1
      }

      metadata = Telemetry.request_metadata(request, %{})

      # Should not crash, returns :unknown for operation
      assert is_map(metadata)
      assert metadata.client_id == "test"
      assert metadata.operation == :unknown
      assert metadata.api_version == 0
    end
  end

  describe "connection_metadata/3" do
    test "creates connection metadata" do
      metadata = Telemetry.connection_metadata("localhost", 9092, true)

      assert metadata.host == "localhost"
      assert metadata.port == 9092
      assert metadata.ssl == true
    end

    test "handles charlist host" do
      metadata = Telemetry.connection_metadata(~c"localhost", 9092, false)

      assert metadata.host == "localhost"
      assert metadata.port == 9092
      assert metadata.ssl == false
    end
  end

  describe "produce_metadata/4" do
    test "creates produce metadata" do
      metadata = Telemetry.produce_metadata("test-topic", 0, "test-client", 1)

      assert metadata.topic == "test-topic"
      assert metadata.partition == 0
      assert metadata.client_id == "test-client"
      assert metadata.required_acks == 1
    end
  end

  describe "fetch_metadata/4" do
    test "creates fetch metadata" do
      metadata = Telemetry.fetch_metadata("test-topic", 0, 100, "test-client")

      assert metadata.topic == "test-topic"
      assert metadata.partition == 0
      assert metadata.offset == 100
      assert metadata.client_id == "test-client"
    end
  end

  describe "span/3" do
    test "emits start and stop events" do
      ref = make_ref()
      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry_event, ref, event, measurements, metadata})
      end

      events = [
        [:kafka_ex, :test, :start],
        [:kafka_ex, :test, :stop],
        [:kafka_ex, :test, :exception]
      ]

      :telemetry.attach_many(ref, events, handler, nil)

      result =
        Telemetry.span([:kafka_ex, :test], %{foo: "bar"}, fn ->
          {"result_value", %{extra: "data"}}
        end)

      assert result == "result_value"

      # Should receive start event
      assert_receive {:telemetry_event, ^ref, [:kafka_ex, :test, :start], start_measurements, start_metadata}
      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.foo == "bar"

      # Should receive stop event
      assert_receive {:telemetry_event, ^ref, [:kafka_ex, :test, :stop], stop_measurements, stop_metadata}
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_metadata.extra == "data"

      :telemetry.detach(ref)
    end

    test "emits exception event on error" do
      ref = make_ref()
      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry_event, ref, event, measurements, metadata})
      end

      events = [
        [:kafka_ex, :test, :start],
        [:kafka_ex, :test, :stop],
        [:kafka_ex, :test, :exception]
      ]

      :telemetry.attach_many(ref, events, handler, nil)

      assert_raise RuntimeError, "test error", fn ->
        Telemetry.span([:kafka_ex, :test], %{foo: "bar"}, fn ->
          raise "test error"
        end)
      end

      # Should receive start event
      assert_receive {:telemetry_event, ^ref, [:kafka_ex, :test, :start], _start_measurements, _start_metadata}

      # Should receive exception event (not stop)
      assert_receive {:telemetry_event, ^ref, [:kafka_ex, :test, :exception], exception_measurements,
                      exception_metadata}

      assert Map.has_key?(exception_measurements, :duration)
      assert exception_metadata.kind == :error
      assert %RuntimeError{message: "test error"} = exception_metadata.reason

      :telemetry.detach(ref)
    end
  end

  describe "commit_metadata/4" do
    test "creates commit metadata" do
      metadata = Telemetry.commit_metadata("test-group", "test-client", "test-topic", 3)

      assert metadata.group_id == "test-group"
      assert metadata.client_id == "test-client"
      assert metadata.topic == "test-topic"
      assert metadata.partition_count == 3
    end
  end

end
