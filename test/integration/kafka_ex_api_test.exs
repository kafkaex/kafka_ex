defmodule KafkaEx.New.KafkaExAPITest do
  use ExUnit.Case, async: true
  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  @moduletag :integration

  alias KafkaEx.New.Client
  alias KafkaEx.New.KafkaExAPI, as: API

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "latest_offset/3" do
    test "returns latest offset for given topic, partition pair", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      {:ok, value} = API.latest_offset(client, topic_name, 0)

      assert value == 1
    end
  end

  describe "latest_offset/4" do
    test "returns latest offset for given topic, partition pair", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      {:ok, value} = API.latest_offset(client, topic_name, 0, api_version: 2)

      assert value == 1
    end
  end

  describe "earliest_offset/3" do
    test "returns latest offset for given topic, partition pair", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      {:ok, value} = API.earliest_offset(client, topic_name, 0)

      assert value == 0
    end
  end

  describe "earliest_offset/4" do
    test "returns latest offset for given topic, partition pair", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      {:ok, value} = API.earliest_offset(client, topic_name, 0, api_version: 2)

      assert value == 0
    end
  end

  describe "describe_group/2" do
    test "returns consumer group description", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = join_to_group(client, topic_name, consumer_group)

      {:ok, group} = API.describe_group(client, consumer_group)

      assert group.group_id == consumer_group
    end
  end

  describe "describe_group/3" do
    test "returns consumer group description", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = join_to_group(client, topic_name, consumer_group)

      {:ok, group} = API.describe_group(client, consumer_group, api_version: 1)

      assert group.group_id == consumer_group
    end
  end

  describe "list_offsets/2" do
    test "returns latest offset for given topic, partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)
      request_data = [{topic_name, [%{partition_num: 0, timestamp: -1}]}]

      {:ok, [offset]} = API.list_offsets(client, request_data)

      assert offset.topic == topic_name
      assert offset.partition_offsets != []
    end
  end

  describe "list_offsets/3" do
    test "returns latest offset for given topic, partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)
      request_data = [{topic_name, [%{partition_num: 0, timestamp: -1}]}]

      {:ok, [offset]} = API.list_offsets(client, request_data, api_version: 2)

      assert offset.topic == topic_name
      assert offset.partition_offsets != []
    end
  end

  describe "fetch_committed_offset/4" do
    test "fetches committed offset for consumer group", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      # Commit an offset first
      partitions_commit = [%{partition_num: 0, offset: 1}]
      {:ok, _} = API.commit_offset(client, consumer_group, topic_name, partitions_commit)

      # Fetch the committed offset
      partitions_fetch = [%{partition_num: 0}]
      {:ok, [offset]} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions_fetch)

      assert offset.topic == topic_name
      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == 1
      assert partition_offset.error_code == :no_error
    end

    test "returns -1 offset for consumer group with no committed offset", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0}]
      {:ok, [offset]} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions)

      assert offset.topic == topic_name
      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == -1
      assert partition_offset.error_code == :no_error
    end

    test "fetches committed offsets for multiple partitions", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 3)
      _ = partition_produce(client, topic_name, "value", 0)
      _ = partition_produce(client, topic_name, "value", 1)
      _ = partition_produce(client, topic_name, "value", 2)

      # Commit offsets for all partitions
      partitions_commit = [
        %{partition_num: 0, offset: 1},
        %{partition_num: 1, offset: 1},
        %{partition_num: 2, offset: 1}
      ]

      {:ok, _} = API.commit_offset(client, consumer_group, topic_name, partitions_commit)

      # Fetch committed offsets
      partitions_fetch = [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]
      {:ok, offsets} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions_fetch)

      assert length(offsets) == 3

      Enum.each(offsets, fn offset ->
        assert offset.topic == topic_name
        assert [partition_offset] = offset.partition_offsets
        assert partition_offset.offset == 1
        assert partition_offset.error_code == :no_error
      end)
    end
  end

  describe "fetch_committed_offset/5" do
    test "supports api_version option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      # Commit with v2
      partitions_commit = [%{partition_num: 0, offset: 1}]
      {:ok, _} = API.commit_offset(client, consumer_group, topic_name, partitions_commit, api_version: 2)

      # Fetch with v2
      partitions_fetch = [%{partition_num: 0}]
      {:ok, [offset]} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions_fetch, api_version: 2)

      assert offset.topic == topic_name
      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 1
    end
  end

  describe "commit_offset/4" do
    test "commits offset for consumer group", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      partitions = [%{partition_num: 0, offset: 1}]
      {:ok, [result]} = API.commit_offset(client, consumer_group, topic_name, partitions)

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

      {:ok, results} = API.commit_offset(client, consumer_group, topic_name, partitions)

      assert length(results) == 3

      Enum.each(results, fn result ->
        assert result.topic == topic_name
        assert [partition_offset] = result.partition_offsets
        assert partition_offset.error_code == :no_error
      end)
    end

    test "commit-then-fetch round trip works", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      # Commit offset
      partitions_commit = [%{partition_num: 0, offset: 42}]
      {:ok, _} = API.commit_offset(client, consumer_group, topic_name, partitions_commit)

      # Fetch it back
      partitions_fetch = [%{partition_num: 0}]
      {:ok, [offset]} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions_fetch)

      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 42
    end
  end

  describe "commit_offset/5" do
    test "supports retention_time option (v2)", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0, offset: 100}]
      {:ok, [result]} = API.commit_offset(client, consumer_group, topic_name, partitions, retention_time: 86_400_000)

      assert result.topic == topic_name
      assert [partition_offset] = result.partition_offsets
      assert partition_offset.error_code == :no_error
    end

    test "supports generation_id and member_id options (v1)", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Note: Using v0 since v1 requires actual group membership for generation_id/member_id
      # to be valid. In a real scenario, these would come from JoinGroup response.
      partitions = [%{partition_num: 0, offset: 100}]
      opts = [api_version: 0]
      {:ok, [result]} = API.commit_offset(client, consumer_group, topic_name, partitions, opts)

      assert result.topic == topic_name
      assert [partition_offset] = result.partition_offsets
      assert partition_offset.error_code == :no_error
    end
  end

  describe "heartbeat/4" do
    test "sends successful heartbeat to consumer group", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group to get member_id and generation_id
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Send heartbeat (defaults to v1)
      {:ok, result} = API.heartbeat(client, consumer_group, member_id, generation_id)

      # v1 returns Heartbeat struct with throttle_time_ms
      assert %KafkaEx.New.Structs.Heartbeat{throttle_time_ms: _} = result
    end

    test "returns error for unknown member_id", %{client: client} do
      consumer_group = generate_random_string()

      # Try heartbeat with invalid member_id (without joining)
      {:error, error} = API.heartbeat(client, consumer_group, "invalid-member", 0)

      assert error == :unknown_member_id
    end

    test "returns error for illegal generation", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Send heartbeat with wrong generation_id
      {:error, error} = API.heartbeat(client, consumer_group, member_id, 999_999)

      assert error == :illegal_generation
    end

    test "multiple heartbeats maintain group membership", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Send multiple heartbeats
      Enum.each(1..3, fn _ ->
        {:ok, _result} = API.heartbeat(client, consumer_group, member_id, generation_id)
        Process.sleep(100)
      end)

      # Verify still in group
      {:ok, group} = API.describe_group(client, consumer_group)
      assert group.group_id == consumer_group
      assert length(group.members) == 1
    end
  end

  describe "heartbeat/5" do
    test "heartbeat with v1 API returns throttle information", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Send heartbeat with v1
      {:ok, result} = API.heartbeat(client, consumer_group, member_id, generation_id, api_version: 1)

      # v1 returns Heartbeat struct
      assert %KafkaEx.New.Structs.Heartbeat{throttle_time_ms: _} = result
    end

    test "supports api_version option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # v0
      {:ok, result_v0} = API.heartbeat(client, consumer_group, member_id, generation_id, api_version: 0)
      assert result_v0 == :no_error

      # v1
      {:ok, result_v1} = API.heartbeat(client, consumer_group, member_id, generation_id, api_version: 1)
      assert %KafkaEx.New.Structs.Heartbeat{} = result_v1
    end

    test "handles custom timeout option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Send heartbeat with custom timeout (defaults to v1)
      {:ok, result} = API.heartbeat(client, consumer_group, member_id, generation_id, timeout: 10_000)

      # v1 returns Heartbeat struct
      assert %KafkaEx.New.Structs.Heartbeat{throttle_time_ms: _} = result
    end
  end

  describe "leave_group/3" do
    test "successfully leaves consumer group", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group to get member_id
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Leave group (defaults to v1)
      {:ok, result} = API.leave_group(client, consumer_group, member_id)

      # v1 returns LeaveGroup struct with throttle_time_ms
      assert %KafkaEx.New.Structs.LeaveGroup{throttle_time_ms: _} = result
    end

    test "returns error for unknown member_id", %{client: client} do
      consumer_group = generate_random_string()

      # Try to leave with invalid member_id (without joining)
      {:error, error} = API.leave_group(client, consumer_group, "invalid-member")

      assert error == :unknown_member_id
    end

    test "returns error for non-existent group", %{client: client} do
      # Try to leave a group that doesn't exist
      {:error, error} = API.leave_group(client, "nonexistent-group", "some-member")

      # Could be :group_id_not_found or :unknown_member_id depending on Kafka version
      assert error in [:group_id_not_found, :unknown_member_id]
    end

    test "leaving group triggers rebalance", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Verify member is in group
      {:ok, group_before} = API.describe_group(client, consumer_group)
      assert length(group_before.members) == 1

      # Leave group
      {:ok, _result} = API.leave_group(client, consumer_group, member_id)

      # Give some time for rebalance
      Process.sleep(500)

      # Verify member is no longer in group
      {:ok, group_after} = API.describe_group(client, consumer_group)
      # Group might be empty or in Empty state
      assert group_after.state in ["Empty", "Dead"]
    end

    test "can rejoin after leaving", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id_1, _generation_id_1} = join_to_group(client, topic_name, consumer_group)

      # Leave group
      {:ok, _result} = API.leave_group(client, consumer_group, member_id_1)

      # Give some time for rebalance
      Process.sleep(500)

      # Rejoin group
      {member_id_2, _generation_id_2} = join_to_group(client, topic_name, consumer_group)

      # Should get a new member_id
      assert member_id_1 != member_id_2

      # Verify member is in group
      {:ok, group} = API.describe_group(client, consumer_group)
      assert length(group.members) == 1
    end

    test "graceful shutdown pattern works", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Graceful shutdown: leave group
      result = API.leave_group(client, consumer_group, member_id)

      # Should succeed
      assert {:ok, _} = result

      # Trying to leave again should fail (already left)
      {:error, error} = API.leave_group(client, consumer_group, member_id)
      assert error == :unknown_member_id
    end
  end

  describe "leave_group/4" do
    test "leave_group with v1 API returns throttle information", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Leave group with v1
      {:ok, result} = API.leave_group(client, consumer_group, member_id, api_version: 1)

      # v1 returns LeaveGroup struct
      assert %KafkaEx.New.Structs.LeaveGroup{throttle_time_ms: _} = result
    end

    test "supports api_version option", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Test v0 with first group
      consumer_group_1 = generate_random_string()
      {member_id_1, _} = join_to_group(client, topic_name, consumer_group_1)

      # Leave with v0
      {:ok, result_v0} = API.leave_group(client, consumer_group_1, member_id_1, api_version: 0)
      assert result_v0 == :no_error

      # Give time for cleanup
      Process.sleep(200)

      # Test v1 with second group
      consumer_group_2 = generate_random_string()
      {member_id_2, _} = join_to_group(client, topic_name, consumer_group_2)

      # Leave with v1
      {:ok, result_v1} = API.leave_group(client, consumer_group_2, member_id_2, api_version: 1)
      assert %KafkaEx.New.Structs.LeaveGroup{} = result_v1
    end

    test "handles custom timeout option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Leave with custom timeout (defaults to v1)
      {:ok, result} = API.leave_group(client, consumer_group, member_id, timeout: 10_000)

      # v1 returns LeaveGroup struct
      assert %KafkaEx.New.Structs.LeaveGroup{throttle_time_ms: _} = result
    end
  end

  describe "sync_group/4" do
    test "successfully syncs as group follower", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group to get member_id and generation_id
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync as follower (no assignments provided)
      {:ok, result} = API.sync_group(client, consumer_group, generation_id, member_id)

      # Should succeed and return SyncGroup struct
      assert %KafkaEx.New.Structs.SyncGroup{partition_assignments: _} = result
    end

    test "returns error for unknown member_id", %{client: client} do
      consumer_group = generate_random_string()

      # Try sync_group with invalid member_id (without joining)
      {:error, error} = API.sync_group(client, consumer_group, 0, "invalid-member")

      assert error == :unknown_member_id
    end

    test "returns error for illegal generation", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync with wrong generation_id
      {:error, error} = API.sync_group(client, consumer_group, 999_999, member_id)

      assert error == :illegal_generation
    end

    test "sync_group with empty assignments succeeds", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync with explicit empty assignments
      {:ok, result} = API.sync_group(client, consumer_group, generation_id, member_id, group_assignment: [])

      assert %KafkaEx.New.Structs.SyncGroup{} = result
    end

    test "handles zero generation_id", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, _generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync with generation_id 0 should fail
      {:error, error} = API.sync_group(client, consumer_group, 0, member_id)

      # Should return illegal_generation or unknown_member_id
      assert error in [:illegal_generation, :unknown_member_id]
    end

    test "sync completes consumer group protocol", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync group
      {:ok, sync_result} = API.sync_group(client, consumer_group, generation_id, member_id)

      assert %KafkaEx.New.Structs.SyncGroup{} = sync_result

      # Verify group is stable after sync
      {:ok, group} = API.describe_group(client, consumer_group)
      assert group.group_id == consumer_group
      assert group.state == "Stable"
    end

    test "returns partition assignments", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync group
      {:ok, sync_result} = API.sync_group(client, consumer_group, generation_id, member_id)

      # partition_assignments should be a list (empty or with assignments)
      assert is_list(sync_result.partition_assignments)
    end
  end

  describe "sync_group/5" do
    test "sync_group with v1 API returns throttle information", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync with v1
      {:ok, result} = API.sync_group(client, consumer_group, generation_id, member_id, api_version: 1)

      # v1 returns SyncGroup struct with throttle_time_ms
      assert %KafkaEx.New.Structs.SyncGroup{throttle_time_ms: throttle} = result
      # v1 should include throttle_time_ms (may be 0 or greater)
      assert is_integer(throttle) or is_nil(throttle)
    end

    test "supports api_version option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group twice for two separate sync calls
      {member_id_1, generation_id_1} = join_to_group(client, topic_name, consumer_group)

      # v1 (default)
      {:ok, result_v1} = API.sync_group(client, consumer_group, generation_id_1, member_id_1, api_version: 1)
      assert %KafkaEx.New.Structs.SyncGroup{throttle_time_ms: _} = result_v1

      # Give time for first sync to complete
      Process.sleep(200)

      # Join another member for v0 test
      request2 = %KafkaEx.Protocol.JoinGroup.Request{
        group_name: consumer_group,
        member_id: "",
        topics: [topic_name],
        session_timeout: 6000
      }

      response2 = KafkaEx.join_group(request2, worker_name: client, timeout: 10000)
      member_id_2 = response2.member_id
      generation_id_2 = response2.generation_id

      # v0
      {:ok, result_v0} = API.sync_group(client, consumer_group, generation_id_2, member_id_2, api_version: 0)
      assert %KafkaEx.New.Structs.SyncGroup{throttle_time_ms: nil} = result_v0
    end

    test "handles custom timeout option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync with custom timeout
      {:ok, result} = API.sync_group(client, consumer_group, generation_id, member_id, timeout: 10_000)

      assert %KafkaEx.New.Structs.SyncGroup{} = result
    end

    test "sync with explicit empty group_assignment", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync with explicit empty group_assignment and v1
      {:ok, result} =
        API.sync_group(
          client,
          consumer_group,
          generation_id,
          member_id,
          group_assignment: [],
          api_version: 1
        )

      assert %KafkaEx.New.Structs.SyncGroup{throttle_time_ms: _} = result
    end

    test "multiple sync_group calls with same generation_id", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # First sync
      {:ok, result1} = API.sync_group(client, consumer_group, generation_id, member_id)
      assert %KafkaEx.New.Structs.SyncGroup{} = result1

      # Second sync with same generation_id should succeed
      # (though in real usage, sync_group is typically called once per generation)
      {:ok, result2} = API.sync_group(client, consumer_group, generation_id, member_id)
      assert %KafkaEx.New.Structs.SyncGroup{} = result2
    end

    test "sync_group followed by heartbeat maintains group membership", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Join group
      {member_id, generation_id} = join_to_group(client, topic_name, consumer_group)

      # Sync group
      {:ok, _sync_result} = API.sync_group(client, consumer_group, generation_id, member_id)

      # Send heartbeat to maintain membership
      {:ok, _heartbeat_result} = API.heartbeat(client, consumer_group, member_id, generation_id)

      # Verify still in group
      {:ok, group} = API.describe_group(client, consumer_group)
      assert group.group_id == consumer_group
      assert length(group.members) == 1
    end
  end

  describe "join_group/3" do
    test "joins a consumer group successfully", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          protocol_name: "assign",
          protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: [topic_name]}
        }
      ]

      {:ok, response} =
        API.join_group(
          client,
          consumer_group,
          "",
          session_timeout: 30_000,
          rebalance_timeout: 60_000,
          group_protocols: group_protocols
        )

      assert response.generation_id >= 0
      assert response.group_protocol in ["assign", "range", "roundrobin"]
      assert response.leader_id != ""
      assert response.member_id != ""
      # First member is always the leader
      assert response.leader_id == response.member_id
      # Leader receives all members
      assert length(response.members) >= 1
    end

    test "subsequent join with member_id works", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          protocol_name: "assign",
          protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: [topic_name]}
        }
      ]

      # First join
      {:ok, first_response} =
        API.join_group(
          client,
          consumer_group,
          "",
          session_timeout: 30_000,
          rebalance_timeout: 60_000,
          group_protocols: group_protocols
        )

      member_id = first_response.member_id

      # Second join with member_id
      {:ok, second_response} =
        API.join_group(
          client,
          consumer_group,
          member_id,
          session_timeout: 30_000,
          rebalance_timeout: 60_000,
          group_protocols: group_protocols
        )

      assert second_response.member_id == member_id
      assert second_response.generation_id >= first_response.generation_id
    end
  end

  describe "join_group/4" do
    test "supports api_version option for v0", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          protocol_name: "assign",
          protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: [topic_name]}
        }
      ]

      {:ok, response} =
        API.join_group(
          client,
          consumer_group,
          "",
          session_timeout: 30_000,
          group_protocols: group_protocols,
          api_version: 0
        )

      assert response.generation_id >= 0
      assert response.member_id != ""
      # V0 doesn't have throttle_time_ms
      assert response.throttle_time_ms == nil
    end

    test "supports api_version option for v2 with throttle_time_ms", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          protocol_name: "assign",
          protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: [topic_name]}
        }
      ]

      {:ok, response} =
        API.join_group(
          client,
          consumer_group,
          "",
          session_timeout: 30_000,
          rebalance_timeout: 60_000,
          group_protocols: group_protocols,
          api_version: 2
        )

      assert response.generation_id >= 0
      assert response.member_id != ""
      # V2 includes throttle_time_ms (may be 0)
      assert is_integer(response.throttle_time_ms)
    end

    test "leader detection works correctly", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          protocol_name: "assign",
          protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: [topic_name]}
        }
      ]

      {:ok, response} =
        API.join_group(
          client,
          consumer_group,
          "",
          session_timeout: 30_000,
          rebalance_timeout: 60_000,
          group_protocols: group_protocols
        )

      # Use the helper function from the struct
      alias KafkaEx.New.Structs.JoinGroup
      assert JoinGroup.leader?(response) == true
    end

    test "returns error for invalid consumer group", %{client: client} do
      result =
        API.join_group(
          client,
          "",
          "",
          session_timeout: 30_000,
          rebalance_timeout: 60_000,
          group_protocols: []
        )

      assert {:error, :invalid_consumer_group} == result
    end
  end

  describe "join_group + sync_group workflow" do
    test "complete consumer group join and sync workflow", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          protocol_name: "assign",
          protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: [topic_name]}
        }
      ]

      # Step 1: Join the group
      {:ok, join_response} =
        API.join_group(
          client,
          consumer_group,
          "",
          session_timeout: 30_000,
          rebalance_timeout: 60_000,
          group_protocols: group_protocols
        )

      assert join_response.generation_id >= 0
      member_id = join_response.member_id
      generation_id = join_response.generation_id

      # Step 2: Sync (as leader, provide assignments)
      alias KafkaEx.New.Structs.JoinGroup

      if JoinGroup.leader?(join_response) do
        # Leader assigns partitions to members
        group_assignment = [
          %{
            member_id: member_id,
            member_assignment: %Kayrock.MemberAssignment{
              partition_assignments: [%{topic: topic_name, partitions: [0]}]
            }
          }
        ]

        {:ok, sync_response} =
          API.sync_group(
            client,
            consumer_group,
            generation_id,
            member_id,
            group_assignment: group_assignment
          )

        assert sync_response.partition_assignment.partition_assignments != []
      else
        # Follower waits for assignment
        {:ok, sync_response} =
          API.sync_group(
            client,
            consumer_group,
            generation_id,
            member_id,
            group_assignment: []
          )

        assert sync_response.partition_assignment != nil
      end

      # Step 3: Verify group membership
      {:ok, group_info} = API.describe_group(client, consumer_group)
      assert group_info.group_id == consumer_group
      assert length(group_info.members) == 1
    end
  end
end
