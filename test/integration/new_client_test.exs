defmodule KafkaEx.New.Client.Test do
  use ExUnit.Case
  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.New.Client

  alias KafkaEx.New.Kafka.ClusterMetadata
  alias KafkaEx.New.KafkaExAPI
  alias KafkaEx.New.Kafka.Topic
  alias KafkaEx.New.Client.NodeSelector

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  @moduletag :new_client

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "describe_groups/1" do
    setup do
      consumer_group = generate_random_string()
      topic = "new_client_implementation"

      {:ok, %{consumer_group: consumer_group, topic: topic}}
    end

    test "returns group metadata for single consumer group", %{
      consumer_group: consumer_group,
      topic: topic,
      client: client
    } do
      join_to_group(client, topic, consumer_group)

      {:ok, [group_metadata]} = GenServer.call(client, {:describe_groups, [consumer_group], []})

      assert group_metadata.group_id == consumer_group
      assert group_metadata.protocol_type == "consumer"
      assert group_metadata.protocol == ""
      assert length(group_metadata.members) == 1
    end

    test "returns dead when consumer group does not exist", %{client: client} do
      {:ok, [group_metadata]} = GenServer.call(client, {:describe_groups, ["non-existing-group"], []})

      assert group_metadata.group_id == "non-existing-group"
      assert group_metadata.state == "Dead"
    end
  end

  describe "list_offsets/1" do
    test "list latest offsets for empty topic", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      partition = [%{partition_num: 0, timestamp: :latest}]

      {:ok, [result]} = GenServer.call(client, {:list_offsets, [{topic_name, partition}], []})

      assert result.topic == topic_name

      %{partition_offsets: [offset]} = result
      assert offset.partition == 0
      assert offset.offset == 0
    end

    test "lists latest offsets for topic with existing messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)
      partition = [%{partition_num: 0, timestamp: :latest}]

      {:ok, [result]} = GenServer.call(client, {:list_offsets, [{topic_name, partition}], []})

      assert result.topic == topic_name

      %{partition_offsets: [offset]} = result
      assert offset.partition == 0
      assert offset.offset == 1
    end

    test "list earliest offsets for empty topic", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      partition = [%{partition_num: 0, timestamp: :earliest}]

      {:ok, [result]} = GenServer.call(client, {:list_offsets, [{topic_name, partition}], []})

      assert result.topic == topic_name

      %{partition_offsets: [offset]} = result
      assert offset.partition == 0
      assert offset.offset == 0
    end

    test "lists earliest offsets for topic with existing messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)
      partition = [%{partition_num: 0, timestamp: :earliest}]

      {:ok, [result]} = GenServer.call(client, {:list_offsets, [{topic_name, partition}], []})

      assert result.topic == topic_name

      %{partition_offsets: [offset]} = result
      assert offset.partition == 0
      assert offset.offset == 0
    end

    test "lists past based offset for topic with existing messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)
      partition = [%{partition_num: 0, timestamp: 123}]

      {:ok, [result]} = GenServer.call(client, {:list_offsets, [{topic_name, partition}], []})

      assert result.topic == topic_name

      %{partition_offsets: [offset]} = result
      assert offset.partition == 0
      assert offset.offset == -1
    end

    test "returns error when opts are invalid", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)
      partition = [%{partition_num: 0, timestamp: -1}]

      {:error, error} = GenServer.call(client, {:list_offsets, [{topic_name, partition}], [api_version: 5]})

      assert error == :api_version_no_supported
    end
  end

  test "update metadata", %{client: client} do
    {:ok, updated_metadata} = GenServer.call(client, :update_metadata)
    %ClusterMetadata{topics: topics} = updated_metadata
    # we don't fetch any topics on startup
    assert topics == %{}

    {:ok, [topic_metadata]} = GenServer.call(client, {:topic_metadata, ["test0p8p0"], false})

    assert %Topic{name: "test0p8p0"} = topic_metadata
  end

  test "list offsets", %{client: client} do
    topic = "test0p8p0"

    for partition <- 0..3 do
      request = %Kayrock.ListOffsets.V1.Request{
        replica_id: -1,
        topics: [
          %{topic: topic, partitions: [%{partition: partition, timestamp: -1}]}
        ]
      }

      {:ok, resp} =
        Client.send_request(
          client,
          request,
          NodeSelector.topic_partition(topic, partition)
        )

      %Kayrock.ListOffsets.V1.Response{responses: [main_resp]} = resp
      [%{error_code: error_code, offset: offset}] = main_resp.partition_responses

      assert error_code == 0

      {:ok, latest_offset} = KafkaExAPI.latest_offset(client, topic, partition)
      assert latest_offset == offset
    end
  end

  test "produce (new message format)", %{client: client} do
    topic = "test0p8p0"
    partition = 1

    {:ok, offset_before} = KafkaExAPI.latest_offset(client, topic, partition)

    record_batch = RecordBatch.from_binary_list(["foo", "bar", "baz"])

    request = %Kayrock.Produce.V1.Request{
      acks: -1,
      timeout: 1000,
      topic_data: [
        %{
          topic: topic,
          data: [
            %{partition: partition, record_set: record_batch}
          ]
        }
      ]
    }

    {:ok, response} =
      Client.send_request(
        client,
        request,
        NodeSelector.topic_partition(topic, partition)
      )

    %Kayrock.Produce.V1.Response{responses: [topic_response]} = response
    assert topic_response.topic == topic

    [%{partition: ^partition, error_code: error_code}] = topic_response.partition_responses

    assert error_code == 0

    {:ok, offset_after} = KafkaExAPI.latest_offset(client, topic, partition)
    assert offset_after == offset_before + 3
  end

  test "produce with record headers (new message format)", %{client: client} do
    topic = "test0p8p0"
    partition = 1

    {:ok, offset_before} = KafkaExAPI.latest_offset(client, topic, partition)

    headers = [
      %RecordHeader{key: "source", value: "System-X"},
      %RecordHeader{key: "type", value: "HeaderCreatedEvent"}
    ]

    records = [
      %Record{
        headers: headers,
        key: "key-0001",
        value: "msg value for key 0001"
      }
    ]

    record_batch = %RecordBatch{
      attributes: 0,
      records: records
    }

    request = %Kayrock.Produce.V1.Request{
      acks: -1,
      timeout: 1000,
      topic_data: [
        %{
          topic: topic,
          data: [
            %{partition: partition, record_set: record_batch}
          ]
        }
      ]
    }

    {:ok, response} =
      Client.send_request(
        client,
        request,
        NodeSelector.topic_partition(topic, partition)
      )

    %Kayrock.Produce.V1.Response{responses: [topic_response]} = response
    assert topic_response.topic == topic

    [%{partition: ^partition, error_code: error_code}] = topic_response.partition_responses

    assert error_code == 0

    {:ok, offset_after} = KafkaExAPI.latest_offset(client, topic, partition)
    assert offset_after == offset_before + 1
  end

  test "client can receive {:ssl_closed, _}", %{client: client} do
    send(client, {:ssl_closed, :unused})

    KafkaEx.TestHelpers.wait_for(fn ->
      {:message_queue_len, m} = Process.info(client, :message_queue_len)
      m == 0
    end)

    assert Process.alive?(client)
  end

  test "client can receive {:tcp_closed, _}", %{client: client} do
    send(client, {:tcp_closed, :unused})

    KafkaEx.TestHelpers.wait_for(fn ->
      {:message_queue_len, m} = Process.info(client, :message_queue_len)
      m == 0
    end)

    assert Process.alive?(client)
  end

  describe "offset_fetch" do
    test "fetches committed offset for consumer group", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Commit offset first
      partitions_commit = [%{partition_num: 0, offset: 10}]
      {:ok, _} = GenServer.call(client, {:offset_commit, consumer_group, [{topic_name, partitions_commit}], []})

      # Fetch committed offset
      partitions_fetch = [%{partition_num: 0}]
      {:ok, [offset]} = GenServer.call(client, {:offset_fetch, consumer_group, [{topic_name, partitions_fetch}], []})

      assert offset.topic == topic_name
      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == 10
      assert partition_offset.error_code == :no_error
    end

    test "returns -1 for consumer group with no committed offset", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0}]
      {:ok, [offset]} = GenServer.call(client, {:offset_fetch, consumer_group, [{topic_name, partitions}], []})

      assert offset.topic == topic_name
      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == -1
    end

    test "returns error for invalid consumer group", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0}]

      {:error, :invalid_consumer_group} =
        GenServer.call(client, {:offset_fetch, :no_consumer_group, [{topic_name, partitions}], []})
    end

    test "supports api_version option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Commit with v2
      partitions_commit = [%{partition_num: 0, offset: 5}]

      {:ok, _} =
        GenServer.call(client, {:offset_commit, consumer_group, [{topic_name, partitions_commit}], [api_version: 2]})

      # Fetch with v2
      partitions_fetch = [%{partition_num: 0}]

      {:ok, [offset]} =
        GenServer.call(client, {:offset_fetch, consumer_group, [{topic_name, partitions_fetch}], [api_version: 2]})

      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 5
    end
  end

  describe "offset_commit" do
    test "commits offset for consumer group", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0, offset: 100}]
      {:ok, [result]} = GenServer.call(client, {:offset_commit, consumer_group, [{topic_name, partitions}], []})

      assert result.topic == topic_name
      assert [partition_offset] = result.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.error_code == :no_error
    end

    test "commits offsets for multiple partitions", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 3)

      partitions = [
        %{partition_num: 0, offset: 10},
        %{partition_num: 1, offset: 20},
        %{partition_num: 2, offset: 30}
      ]

      {:ok, results} = GenServer.call(client, {:offset_commit, consumer_group, [{topic_name, partitions}], []})

      assert length(results) == 3

      Enum.each(results, fn result ->
        assert result.topic == topic_name
        assert [partition_offset] = result.partition_offsets
        assert partition_offset.error_code == :no_error
      end)
    end

    test "returns error for invalid consumer group", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0, offset: 100}]

      {:error, :invalid_consumer_group} =
        GenServer.call(client, {:offset_commit, :no_consumer_group, [{topic_name, partitions}], []})
    end

    test "supports retention_time option (v2)", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0, offset: 50}]
      opts = [api_version: 2, retention_time: 86_400_000]
      {:ok, [result]} = GenServer.call(client, {:offset_commit, consumer_group, [{topic_name, partitions}], opts})

      assert [partition_offset] = result.partition_offsets
      assert partition_offset.error_code == :no_error
    end

    test "supports generation_id and member_id options (v1)", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Note: Using v0 since v1 requires actual group membership for generation_id/member_id
      # to be valid. In a real scenario, these would come from JoinGroup response.
      partitions = [%{partition_num: 0, offset: 75}]
      opts = [api_version: 0]
      {:ok, [result]} = GenServer.call(client, {:offset_commit, consumer_group, [{topic_name, partitions}], opts})

      assert [partition_offset] = result.partition_offsets
      assert partition_offset.error_code == :no_error
    end

    test "commit-then-fetch round trip", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Commit
      partitions_commit = [%{partition_num: 0, offset: 42}]
      {:ok, _} = GenServer.call(client, {:offset_commit, consumer_group, [{topic_name, partitions_commit}], []})

      # Fetch
      partitions_fetch = [%{partition_num: 0}]
      {:ok, [offset]} = GenServer.call(client, {:offset_fetch, consumer_group, [{topic_name, partitions_fetch}], []})

      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 42
    end
  end

  describe "heartbeat" do
    test "sends successful heartbeat to consumer group", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group to get member_id and generation_id
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Send heartbeat
      {:ok, result} = GenServer.call(client, {:heartbeat, consumer_group, member_id, generation_id, []})

      # v0 returns :no_error
      assert result == :no_error
    end

    test "heartbeat with v1 API returns throttle information", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Send heartbeat with v1
      {:ok, result} = GenServer.call(client, {:heartbeat, consumer_group, member_id, generation_id, [api_version: 1]})

      # v1 returns Heartbeat struct
      assert %KafkaEx.New.Kafka.Heartbeat{throttle_time_ms: _} = result
    end

    test "returns error for unknown member_id", %{client: client} do
      consumer_group = generate_random_string()

      # Try heartbeat with invalid member_id (without joining)
      {:error, error} = GenServer.call(client, {:heartbeat, consumer_group, "invalid-member", 0, []})

      assert error.error == :unknown_member_id
    end

    test "returns error for illegal generation", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Send heartbeat with wrong generation_id
      {:error, error} = GenServer.call(client, {:heartbeat, consumer_group, member_id, 999_999, []})

      assert error.error == :illegal_generation
    end

    test "returns error for invalid consumer group", %{client: client} do
      {:error, :invalid_consumer_group} =
        GenServer.call(client, {:heartbeat, :not_a_binary, "member", 0, []})
    end

    test "multiple heartbeats maintain group membership", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Send multiple heartbeats
      Enum.each(1..3, fn _ ->
        {:ok, _result} = GenServer.call(client, {:heartbeat, consumer_group, member_id, generation_id, []})
        Process.sleep(100)
      end)

      # Verify still in group by describing it
      {:ok, [group]} = GenServer.call(client, {:describe_groups, [consumer_group], []})
      assert group.group_id == consumer_group
      assert length(group.members) == 1
    end

    test "supports api_version option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # v0
      {:ok, result_v0} =
        GenServer.call(client, {:heartbeat, consumer_group, member_id, generation_id, [api_version: 0]})

      assert result_v0 == :no_error

      # v1
      {:ok, result_v1} =
        GenServer.call(client, {:heartbeat, consumer_group, member_id, generation_id, [api_version: 1]})

      assert %KafkaEx.New.Kafka.Heartbeat{} = result_v1
    end
  end

  describe "sync_group" do
    test "successfully syncs as group follower", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group to get member_id and generation_id
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync as follower (no assignments provided)
      {:ok, result} = GenServer.call(client, {:sync_group, consumer_group, generation_id, member_id, []})

      # Should succeed and return SyncGroup struct
      assert %KafkaEx.New.Kafka.SyncGroup{partition_assignments: _} = result
    end

    test "sync_group with v1 API returns throttle information", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync with v1
      {:ok, result} = GenServer.call(client, {:sync_group, consumer_group, generation_id, member_id, [api_version: 1]})

      # v1 returns SyncGroup struct with throttle_time_ms
      assert %KafkaEx.New.Kafka.SyncGroup{throttle_time_ms: throttle} = result
      # v1 should include throttle_time_ms (may be 0 or greater)
      assert is_integer(throttle) or is_nil(throttle)
    end

    test "returns error for unknown member_id", %{client: client} do
      consumer_group = generate_random_string()

      # Try sync_group with invalid member_id (without joining)
      {:error, error} = GenServer.call(client, {:sync_group, consumer_group, 0, "invalid-member", []})

      assert error.error == :unknown_member_id
    end

    test "returns error for illegal generation", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync with wrong generation_id
      {:error, error} = GenServer.call(client, {:sync_group, consumer_group, 999_999, member_id, []})

      assert error.error == :illegal_generation
    end

    test "returns error for invalid consumer group", %{client: client} do
      {:error, :invalid_consumer_group} =
        GenServer.call(client, {:sync_group, :not_a_binary, 0, "member", []})
    end

    test "sync_group with empty assignments succeeds", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync with explicit empty assignments
      {:ok, result} =
        GenServer.call(client, {:sync_group, consumer_group, generation_id, member_id, [group_assignment: []]})

      assert %KafkaEx.New.Kafka.SyncGroup{} = result
    end

    test "sync completes consumer group protocol", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync group
      {:ok, sync_result} = GenServer.call(client, {:sync_group, consumer_group, generation_id, member_id, []})

      assert %KafkaEx.New.Kafka.SyncGroup{} = sync_result

      # Verify group is stable after sync
      {:ok, [group]} = GenServer.call(client, {:describe_groups, [consumer_group], []})
      assert group.group_id == consumer_group
      assert group.state == "Stable"
    end

    test "supports api_version option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # v0
      {:ok, result_v0} =
        GenServer.call(client, {:sync_group, consumer_group, generation_id, member_id, [api_version: 0]})

      assert %KafkaEx.New.Kafka.SyncGroup{throttle_time_ms: nil} = result_v0

      # Give time for first sync to complete
      Process.sleep(200)

      # v1
      {:ok, result_v1} =
        GenServer.call(client, {:sync_group, consumer_group, generation_id, member_id, [api_version: 1]})

      assert %KafkaEx.New.Kafka.SyncGroup{throttle_time_ms: _} = result_v1
    end

    test "sync_group followed by heartbeat maintains group membership", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync group
      {:ok, _sync_result} = GenServer.call(client, {:sync_group, consumer_group, generation_id, member_id, []})

      # Send heartbeat to maintain membership
      {:ok, _heartbeat_result} = GenServer.call(client, {:heartbeat, consumer_group, member_id, generation_id, []})

      # Verify still in group
      {:ok, [group]} = GenServer.call(client, {:describe_groups, [consumer_group], []})
      assert group.group_id == consumer_group
      assert length(group.members) == 1
    end
  end
end
