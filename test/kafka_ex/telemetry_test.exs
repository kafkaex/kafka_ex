defmodule KafkaEx.TelemetryTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Telemetry
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

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

    test "includes auth events" do
      events = Telemetry.events()

      assert [:kafka_ex, :auth, :start] in events
      assert [:kafka_ex, :auth, :stop] in events
      assert [:kafka_ex, :auth, :exception] in events
    end

    test "includes consumer group events" do
      events = Telemetry.events()

      assert [:kafka_ex, :consumer, :join, :start] in events
      assert [:kafka_ex, :consumer, :join, :stop] in events
      assert [:kafka_ex, :consumer, :join, :exception] in events
      assert [:kafka_ex, :consumer, :sync, :start] in events
      assert [:kafka_ex, :consumer, :sync, :stop] in events
      assert [:kafka_ex, :consumer, :sync, :exception] in events
      assert [:kafka_ex, :consumer, :heartbeat, :start] in events
      assert [:kafka_ex, :consumer, :heartbeat, :stop] in events
      assert [:kafka_ex, :consumer, :heartbeat, :exception] in events
      assert [:kafka_ex, :consumer, :leave, :start] in events
      assert [:kafka_ex, :consumer, :leave, :stop] in events
      assert [:kafka_ex, :consumer, :leave, :exception] in events
      assert [:kafka_ex, :consumer, :rebalance] in events
    end

    test "includes metadata events" do
      events = Telemetry.events()

      assert [:kafka_ex, :metadata, :update, :start] in events
      assert [:kafka_ex, :metadata, :update, :stop] in events
      assert [:kafka_ex, :metadata, :update, :exception] in events
    end

    test "includes consumer process events" do
      events = Telemetry.events()

      assert [:kafka_ex, :consumer, :process, :start] in events
      assert [:kafka_ex, :consumer, :process, :stop] in events
      assert [:kafka_ex, :consumer, :process, :exception] in events
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

      assert length(events) == 4

      Enum.each(events, fn event ->
        assert Enum.at(event, 1) == :connection
      end)
    end

    test "includes close event" do
      events = Telemetry.connection_events()

      assert [:kafka_ex, :connection, :close] in events
    end
  end

  describe "auth_events/0" do
    test "returns only auth events" do
      events = Telemetry.auth_events()

      assert length(events) == 3

      Enum.each(events, fn event ->
        assert Enum.at(event, 1) == :auth
      end)
    end

    test "includes all auth events" do
      events = Telemetry.auth_events()

      assert [:kafka_ex, :auth, :start] in events
      assert [:kafka_ex, :auth, :stop] in events
      assert [:kafka_ex, :auth, :exception] in events
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
    test "returns all consumer events including commit, group, and process events" do
      events = Telemetry.consumer_events()

      # 3 commit + 12 group (join, sync, heartbeat, leave x 3) + 1 rebalance + 3 process = 19
      assert length(events) == 19

      # Commit events
      assert [:kafka_ex, :consumer, :commit, :start] in events
      assert [:kafka_ex, :consumer, :commit, :stop] in events
      assert [:kafka_ex, :consumer, :commit, :exception] in events

      # Group lifecycle events
      assert [:kafka_ex, :consumer, :join, :start] in events
      assert [:kafka_ex, :consumer, :join, :stop] in events
      assert [:kafka_ex, :consumer, :join, :exception] in events
      assert [:kafka_ex, :consumer, :sync, :start] in events
      assert [:kafka_ex, :consumer, :sync, :stop] in events
      assert [:kafka_ex, :consumer, :sync, :exception] in events
      assert [:kafka_ex, :consumer, :heartbeat, :start] in events
      assert [:kafka_ex, :consumer, :heartbeat, :stop] in events
      assert [:kafka_ex, :consumer, :heartbeat, :exception] in events
      assert [:kafka_ex, :consumer, :leave, :start] in events
      assert [:kafka_ex, :consumer, :leave, :stop] in events
      assert [:kafka_ex, :consumer, :leave, :exception] in events
      assert [:kafka_ex, :consumer, :rebalance] in events

      # Process events
      assert [:kafka_ex, :consumer, :process, :start] in events
      assert [:kafka_ex, :consumer, :process, :stop] in events
      assert [:kafka_ex, :consumer, :process, :exception] in events
    end
  end

  describe "consumer_group_events/0" do
    test "returns only consumer group lifecycle events" do
      events = Telemetry.consumer_group_events()

      # 12 group events (join, sync, heartbeat, leave x 3) + 1 rebalance = 13
      assert length(events) == 13

      # Group lifecycle events
      assert [:kafka_ex, :consumer, :join, :start] in events
      assert [:kafka_ex, :consumer, :join, :stop] in events
      assert [:kafka_ex, :consumer, :join, :exception] in events
      assert [:kafka_ex, :consumer, :sync, :start] in events
      assert [:kafka_ex, :consumer, :sync, :stop] in events
      assert [:kafka_ex, :consumer, :sync, :exception] in events
      assert [:kafka_ex, :consumer, :heartbeat, :start] in events
      assert [:kafka_ex, :consumer, :heartbeat, :stop] in events
      assert [:kafka_ex, :consumer, :heartbeat, :exception] in events
      assert [:kafka_ex, :consumer, :leave, :start] in events
      assert [:kafka_ex, :consumer, :leave, :stop] in events
      assert [:kafka_ex, :consumer, :leave, :exception] in events
      assert [:kafka_ex, :consumer, :rebalance] in events
    end
  end

  describe "metadata_events/0" do
    test "returns only metadata events" do
      events = Telemetry.metadata_events()

      assert length(events) == 3

      Enum.each(events, fn event ->
        assert Enum.at(event, 1) == :metadata
      end)
    end

    test "includes all metadata events" do
      events = Telemetry.metadata_events()

      assert [:kafka_ex, :metadata, :update, :start] in events
      assert [:kafka_ex, :metadata, :update, :stop] in events
      assert [:kafka_ex, :metadata, :update, :exception] in events
    end
  end

  describe "consumer_process_events/0" do
    test "returns only consumer process events" do
      events = Telemetry.consumer_process_events()

      assert length(events) == 3

      Enum.each(events, fn event ->
        assert Enum.at(event, 1) == :consumer
        assert Enum.at(event, 2) == :process
      end)
    end

    test "includes all consumer process events" do
      events = Telemetry.consumer_process_events()

      assert [:kafka_ex, :consumer, :process, :start] in events
      assert [:kafka_ex, :consumer, :process, :stop] in events
      assert [:kafka_ex, :consumer, :process, :exception] in events
    end
  end

  describe "request_metadata/2" do
    test "extracts operation from real Kayrock request struct" do
      # Use real Kayrock request struct via fixture helper
      request = Fixtures.build_request(:metadata, 1,
        client_id: "test_client",
        correlation_id: 42,
        topics: []
      )

      metadata = Telemetry.request_metadata(request, %{host: "localhost", port: 9092})

      assert metadata.operation == :metadata
      assert metadata.api_version == 1
      assert metadata.correlation_id == 42
      assert metadata.client_id == "test_client"
      assert metadata.broker == %{host: "localhost", port: 9092}
    end

    test "extracts operation from different Kayrock request types" do
      produce_request = Fixtures.build_request(:produce, 2,
        client_id: "producer",
        correlation_id: 1,
        acks: 1,
        timeout: 5000,
        topic_data: []
      )

      fetch_request = Fixtures.build_request(:fetch, 3,
        client_id: "consumer",
        correlation_id: 2,
        replica_id: -1,
        max_wait_time: 500,
        min_bytes: 1,
        max_bytes: 1_000_000,
        topics: []
      )

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

  describe "auth_metadata/3" do
    test "creates auth metadata" do
      metadata = Telemetry.auth_metadata("localhost", 9092, "PLAIN")

      assert metadata.host == "localhost"
      assert metadata.port == 9092
      assert metadata.mechanism == "PLAIN"
    end

    test "handles charlist host" do
      metadata = Telemetry.auth_metadata(~c"localhost", 9092, "SCRAM-SHA-256")

      assert metadata.host == "localhost"
      assert metadata.port == 9092
      assert metadata.mechanism == "SCRAM-SHA-256"
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

  describe "join_group_metadata/3" do
    test "creates join group metadata" do
      metadata = Telemetry.join_group_metadata("test-group", "member-1", ["topic-a", "topic-b"])

      assert metadata.group_id == "test-group"
      assert metadata.member_id == "member-1"
      assert metadata.topics == ["topic-a", "topic-b"]
    end
  end

  describe "sync_group_metadata/4" do
    test "creates sync group metadata" do
      metadata = Telemetry.sync_group_metadata("test-group", "member-1", 5, true)

      assert metadata.group_id == "test-group"
      assert metadata.member_id == "member-1"
      assert metadata.generation_id == 5
      assert metadata.is_leader == true
    end
  end

  describe "heartbeat_metadata/3" do
    test "creates heartbeat metadata" do
      metadata = Telemetry.heartbeat_metadata("test-group", "member-1", 5)

      assert metadata.group_id == "test-group"
      assert metadata.member_id == "member-1"
      assert metadata.generation_id == 5
    end
  end

  describe "leave_group_metadata/2" do
    test "creates leave group metadata" do
      metadata = Telemetry.leave_group_metadata("test-group", "member-1")

      assert metadata.group_id == "test-group"
      assert metadata.member_id == "member-1"
    end
  end

  describe "metadata_update_metadata/2" do
    test "creates metadata update metadata" do
      metadata = Telemetry.metadata_update_metadata("test-client", ["topic-a", "topic-b"])

      assert metadata.client_id == "test-client"
      assert metadata.topics == ["topic-a", "topic-b"]
    end

    test "handles empty topics list" do
      metadata = Telemetry.metadata_update_metadata("test-client", [])

      assert metadata.client_id == "test-client"
      assert metadata.topics == []
    end
  end

  describe "consumer_process_metadata/4" do
    test "creates consumer process metadata" do
      metadata = Telemetry.consumer_process_metadata("test-group", "test-topic", 2, "MyConsumer")

      assert metadata.group_id == "test-group"
      assert metadata.topic == "test-topic"
      assert metadata.partition == 2
      assert metadata.consumer_module == "MyConsumer"
    end
  end

  describe "emit_rebalance/4" do
    test "emits rebalance event" do
      ref = make_ref()
      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry_event, ref, event, measurements, metadata})
      end

      :telemetry.attach(ref, [:kafka_ex, :consumer, :rebalance], handler, nil)

      Telemetry.emit_rebalance("test-group", "member-1", 5, :heartbeat_timeout)

      assert_receive {:telemetry_event, ^ref, [:kafka_ex, :consumer, :rebalance], measurements, metadata}

      assert measurements.count == 1
      assert metadata.group_id == "test-group"
      assert metadata.member_id == "member-1"
      assert metadata.generation_id == 5
      assert metadata.reason == :heartbeat_timeout

      :telemetry.detach(ref)
    end
  end

  describe "emit_connection_close/3" do
    test "emits connection close event" do
      ref = make_ref()
      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry_event, ref, event, measurements, metadata})
      end

      :telemetry.attach(ref, [:kafka_ex, :connection, :close], handler, nil)

      Telemetry.emit_connection_close("localhost", 9092, :timeout)

      assert_receive {:telemetry_event, ^ref, [:kafka_ex, :connection, :close], measurements, metadata}

      assert measurements.count == 1
      assert metadata.host == "localhost"
      assert metadata.port == 9092
      assert metadata.reason == :timeout

      :telemetry.detach(ref)
    end

    test "converts charlist host to string" do
      ref = make_ref()
      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry_event, ref, event, measurements, metadata})
      end

      :telemetry.attach(ref, [:kafka_ex, :connection, :close], handler, nil)

      Telemetry.emit_connection_close(~c"localhost", 9092, :remote_closed)

      assert_receive {:telemetry_event, ^ref, [:kafka_ex, :connection, :close], _measurements, metadata}

      assert metadata.host == "localhost"
      assert metadata.reason == :remote_closed

      :telemetry.detach(ref)
    end
  end
end
