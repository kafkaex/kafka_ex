defmodule KafkaEx.Integration.Lifecycle.TelemetryTest do
  use ExUnit.Case, async: false
  @moduletag :lifecycle

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Telemetry

  setup do
    ref = make_ref()
    test_pid = self()

    handler = fn event, measurements, metadata, _config ->
      send(test_pid, {:telemetry, ref, event, measurements, metadata})
    end

    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)

    on_exit(fn ->
      :telemetry.detach(ref)

      if Process.alive?(client) do
        GenServer.stop(client)
      end
    end)

    {:ok, ref: ref, handler: handler, client: client}
  end

  describe "telemetry spans" do
    test "request span emits start and stop events", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.request_events(), handler, nil)

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
      assert_receive {:telemetry, ^ref, [:kafka_ex, :request, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0

      # Verify bytes sent and received
      assert is_integer(stop_metadata.bytes_sent)
      assert stop_metadata.bytes_sent > 0
      assert is_integer(stop_metadata.bytes_received)
      assert stop_metadata.bytes_received > 0
    end

    test "request span includes bytes_sent even on error", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.request_events(), handler, nil)

      # Flush startup events
      flush_messages(ref)

      # Try to produce to invalid partition - this will fail
      {:error, _} = API.produce(client, "non-existent-topic", 999, [%{value: "test"}])

      # Find the produce request events (there may be multiple request events)
      # We just need to verify that bytes_sent is present in at least one stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :request, :stop], _stop_measurements, stop_metadata}, 5000
      assert is_integer(stop_metadata.bytes_sent)
      assert stop_metadata.bytes_sent > 0
      assert is_integer(stop_metadata.bytes_received)
      # bytes_received could be 0 (on network error) or > 0 (on Kafka error response)
    end

    test "connection span emits start and stop events", %{ref: ref, handler: handler} do
      :telemetry.attach_many(ref, Telemetry.connection_events(), handler, nil)

      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, new_client} = Client.start_link(args, :no_name)

      on_exit(fn ->
        if Process.alive?(new_client), do: GenServer.stop(new_client)
      end)

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
    end

    test "produce span emits start and stop events", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.produce_events(), handler, nil)

      topic_name = "telemetry-produce-test-#{:rand.uniform(100_000)}"
      {:ok, _} = API.create_topics(client, [[topic: topic_name, num_partitions: 1, replication_factor: 1]], 10_000)
      Process.sleep(500)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "test-message"}])

      # Verify start event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :produce, :start], start_measurements, start_metadata}, 5000
      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.message_count == 1
      assert start_metadata.topic == topic_name
      assert start_metadata.partition == 0
      assert is_binary(start_metadata.client_id)
      assert is_integer(start_metadata.required_acks)

      # Verify stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :produce, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0
      assert stop_metadata.result == :ok
      assert is_integer(stop_metadata.offset)
    end

    test "produce span includes error in stop metadata on failure", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.produce_events(), handler, nil)

      # Produce to non-existent topic (auto-creation disabled scenario or invalid partition)
      {:error, _} = API.produce(client, "telemetry-produce-test", 999, [%{value: "test"}])

      # Verify start event was emitted
      assert_receive {:telemetry, ^ref, [:kafka_ex, :produce, :start], _, _}, 5000

      # Verify stop event includes error info
      assert_receive {:telemetry, ^ref, [:kafka_ex, :produce, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_metadata.result == :error
      assert Map.has_key?(stop_metadata, :error)
    end

    test "fetch span emits start and stop events", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.fetch_events(), handler, nil)

      topic_name = "telemetry-fetch-test-#{:rand.uniform(100_000)}"
      {:ok, _} = API.create_topics(client, [[topic: topic_name, num_partitions: 1, replication_factor: 1]], 10_000)
      Process.sleep(500)

      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "test-message"}])
      offset = produce_result.base_offset

      {:ok, _} = API.fetch(client, topic_name, 0, offset, max_bytes: 100_000)

      # Verify start event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :fetch, :start], start_measurements, start_metadata}, 5000
      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.topic == topic_name
      assert start_metadata.partition == 0
      assert start_metadata.offset == offset
      assert is_binary(start_metadata.client_id)

      # Verify stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :fetch, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0
      assert stop_metadata.result == :ok
      assert stop_metadata.message_count == 1
    end

    test "fetch span includes error in stop metadata on failure", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.fetch_events(), handler, nil)

      # Fetch from non-existent topic/partition
      {:error, _} = API.fetch(client, "non-existent-topic-xyz", 999, 0, max_bytes: 100_000)

      # Verify start event was emitted
      assert_receive {:telemetry, ^ref, [:kafka_ex, :fetch, :start], _, _}, 5000

      # Verify stop event includes error info
      assert_receive {:telemetry, ^ref, [:kafka_ex, :fetch, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_metadata.result == :error
      assert Map.has_key?(stop_metadata, :error)
    end
  end

  describe "consumer telemetry events" do
    test "offset commit span emits start and stop events", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.consumer_events(), handler, nil)

      topic_name = "telemetry-commit-test-#{:rand.uniform(100_000)}"
      group_name = "telemetry-commit-group-#{:rand.uniform(100_000)}"
      {:ok, _} = API.create_topics(client, [[topic: topic_name, num_partitions: 1, replication_factor: 1]], 10_000)
      Process.sleep(500)

      # Commit an offset
      partitions = [%{partition_num: 0, offset: 100}]
      {:ok, _} = API.commit_offset(client, group_name, topic_name, partitions)

      # Verify start event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :commit, :start], start_measurements, start_metadata},
                     5000

      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.group_id == group_name
      assert start_metadata.topic == topic_name
      assert is_binary(start_metadata.client_id)
      assert start_metadata.partition_count == 1

      # Verify stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :commit, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0
      assert stop_metadata.result == :ok
    end
  end

  describe "consumer group telemetry events" do
    test "join group span emits start and stop events", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.consumer_group_events(), handler, nil)

      topic_name = "telemetry-join-test-#{:rand.uniform(100_000)}"
      group_name = "telemetry-join-group-#{:rand.uniform(100_000)}"
      {:ok, _} = API.create_topics(client, [[topic: topic_name, num_partitions: 1, replication_factor: 1]], 10_000)
      Process.sleep(500)

      # Join group
      {:ok, join_result} =
        API.join_group(client, group_name, "",
          topics: [topic_name],
          session_timeout: 30_000,
          rebalance_timeout: 60_000
        )

      # Verify start event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :join, :start], start_measurements, start_metadata}, 5000
      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.group_id == group_name
      assert start_metadata.member_id == ""
      assert start_metadata.topics == [topic_name]

      # Verify stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :join, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0
      assert stop_metadata.result == :ok
      assert stop_metadata.generation_id == join_result.generation_id
      assert is_boolean(stop_metadata.is_leader)

      # Leave group to clean up
      API.leave_group(client, group_name, join_result.member_id)
    end

    test "sync group span emits start and stop events", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.consumer_group_events(), handler, nil)

      topic_name = "telemetry-sync-test-#{:rand.uniform(100_000)}"
      group_name = "telemetry-sync-group-#{:rand.uniform(100_000)}"
      {:ok, _} = API.create_topics(client, [[topic: topic_name, num_partitions: 1, replication_factor: 1]], 10_000)
      Process.sleep(500)

      # Join group first
      {:ok, join_result} =
        API.join_group(client, group_name, "",
          topics: [topic_name],
          session_timeout: 30_000,
          rebalance_timeout: 60_000
        )

      # Flush join events
      flush_messages(ref)

      # Sync group (as leader, provide assignments)
      assignments =
        if join_result.leader_id == join_result.member_id do
          [%{member_id: join_result.member_id, topic_partitions: [{topic_name, [0]}]}]
        else
          []
        end

      {:ok, _sync_result} =
        API.sync_group(client, group_name, join_result.generation_id, join_result.member_id,
          group_assignment: assignments
        )

      # Verify start event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :sync, :start], start_measurements, start_metadata}, 5000
      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.group_id == group_name
      assert start_metadata.member_id == join_result.member_id
      assert start_metadata.generation_id == join_result.generation_id
      assert is_boolean(start_metadata.is_leader)

      # Verify stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :sync, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0
      assert stop_metadata.result == :ok
      assert is_integer(stop_metadata.assigned_partitions)

      # Leave group to clean up
      API.leave_group(client, group_name, join_result.member_id)
    end

    test "heartbeat span emits start and stop events", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.consumer_group_events(), handler, nil)

      topic_name = "telemetry-heartbeat-test-#{:rand.uniform(100_000)}"
      group_name = "telemetry-heartbeat-group-#{:rand.uniform(100_000)}"
      {:ok, _} = API.create_topics(client, [[topic: topic_name, num_partitions: 1, replication_factor: 1]], 10_000)
      Process.sleep(500)

      # Join group first
      {:ok, join_result} =
        API.join_group(client, group_name, "",
          topics: [topic_name],
          session_timeout: 30_000,
          rebalance_timeout: 60_000
        )

      # Sync group
      assignments =
        if join_result.leader_id == join_result.member_id do
          [%{member_id: join_result.member_id, topic_partitions: [{topic_name, [0]}]}]
        else
          []
        end

      {:ok, _} =
        API.sync_group(client, group_name, join_result.generation_id, join_result.member_id,
          group_assignment: assignments
        )

      # Flush join/sync events
      flush_messages(ref)

      # Send heartbeat
      {:ok, _} = API.heartbeat(client, group_name, join_result.member_id, join_result.generation_id)

      # Verify start event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :heartbeat, :start], start_measurements, start_metadata},
                     5000

      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.group_id == group_name
      assert start_metadata.member_id == join_result.member_id
      assert start_metadata.generation_id == join_result.generation_id

      # Verify stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :heartbeat, :stop], stop_measurements, stop_metadata},
                     5000

      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0
      assert stop_metadata.result == :ok

      # Leave group to clean up
      API.leave_group(client, group_name, join_result.member_id)
    end

    test "leave group span emits start and stop events", %{ref: ref, handler: handler, client: client} do
      :telemetry.attach_many(ref, Telemetry.consumer_group_events(), handler, nil)

      topic_name = "telemetry-leave-test-#{:rand.uniform(100_000)}"
      group_name = "telemetry-leave-group-#{:rand.uniform(100_000)}"
      {:ok, _} = API.create_topics(client, [[topic: topic_name, num_partitions: 1, replication_factor: 1]], 10_000)
      Process.sleep(500)

      # Join group first
      {:ok, join_result} =
        API.join_group(client, group_name, "",
          topics: [topic_name],
          session_timeout: 30_000,
          rebalance_timeout: 60_000
        )

      # Flush join events
      flush_messages(ref)

      # Leave group
      {:ok, _} = API.leave_group(client, group_name, join_result.member_id)

      # Verify start event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :leave, :start], start_measurements, start_metadata},
                     5000

      assert Map.has_key?(start_measurements, :system_time)
      assert start_metadata.group_id == group_name
      assert start_metadata.member_id == join_result.member_id

      # Verify stop event
      assert_receive {:telemetry, ^ref, [:kafka_ex, :consumer, :leave, :stop], stop_measurements, stop_metadata}, 5000
      assert Map.has_key?(stop_measurements, :duration)
      assert stop_measurements.duration > 0
      assert stop_metadata.result == :ok
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
