defmodule KafkaEx.Integration.ConsumerGroup.HighestSupportedVersionTest do
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.Heartbeat
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Messages.LeaveGroup
  alias KafkaEx.Messages.SyncGroup
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {:ok, %{client: pid}}
  end

  # ---------------------------------------------------------------------------
  # Helper: full consumer group join+sync lifecycle
  # ---------------------------------------------------------------------------
  defp join_and_sync_group(client, topic_name, group_id, retries \\ 5) do
    group_protocols = [
      %{
        name: "assign",
        metadata: Fixtures.group_protocol_metadata(topics: [topic_name])
      }
    ]

    join_opts = [
      session_timeout: 30_000,
      rebalance_timeout: 60_000,
      group_protocols: group_protocols
    ]

    case API.join_group(client, group_id, "", join_opts) do
      {:ok, join_result} ->
        member_id = join_result.member_id
        generation_id = join_result.generation_id

        group_assignment = [
          %{
            member_id: member_id,
            assignment:
              Fixtures.member_assignment(
                partition_assignments: [
                  Fixtures.partition_assignment(
                    topic: topic_name,
                    partitions: [0]
                  )
                ]
              )
          }
        ]

        {:ok, _sync_result} =
          API.sync_group(client, group_id, generation_id, member_id, group_assignment: group_assignment)

        {member_id, generation_id}

      {:error, _reason} when retries > 0 ->
        Process.sleep(1_000)
        join_and_sync_group(client, topic_name, group_id, retries - 1)

      {:error, reason} ->
        raise "join_and_sync_group failed after retries: #{inspect(reason)}"
    end
  end

  defp retry_join_group(client, group_id, join_opts, retries \\ 5) do
    case API.join_group(client, group_id, "", join_opts) do
      {:ok, _} = success ->
        success

      {:error, _reason} when retries > 0 ->
        Process.sleep(1_000)
        retry_join_group(client, group_id, join_opts, retries - 1)

      {:error, reason} ->
        raise "join_group failed after retries: #{inspect(reason)}"
    end
  end

  # ---------------------------------------------------------------------------
  # OffsetFetch V6 (FLEX)
  # ---------------------------------------------------------------------------
  describe "OffsetFetch V6 (highest)" do
    test "fetches committed offsets for a consumer group", %{client: client} do
      topic_name = generate_random_string()
      group_id = "cg-offset-fetch-v6-#{generate_random_string()}"
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0}]

      {:ok, result} = API.fetch_committed_offset(client, group_id, topic_name, partitions, api_version: 6)

      # No offsets committed yet, so offset should be -1
      assert [%{partition_offsets: partition_offsets}] = result
      assert [%{offset: offset}] = partition_offsets
      assert offset == -1
    end
  end

  # ---------------------------------------------------------------------------
  # OffsetCommit V8 (FLEX)
  # ---------------------------------------------------------------------------
  describe "OffsetCommit V8 (highest)" do
    @tag timeout: 120_000
    test "commits offset and verifies with OffsetFetch", %{client: client} do
      topic_name = generate_random_string()
      group_id = "cg-offset-commit-v8-#{generate_random_string()}"
      _ = create_topic(client, topic_name)

      {member_id, generation_id} = join_and_sync_group(client, topic_name, group_id)

      partitions = [%{partition_num: 0, offset: 42}]
      commit_opts = [generation_id: generation_id, member_id: member_id, api_version: 8]

      {:ok, commit_result} = API.commit_offset(client, group_id, topic_name, partitions, commit_opts)

      assert [%{partition_offsets: [%{error_code: :no_error}]}] = commit_result

      # Verify with OffsetFetch
      fetch_partitions = [%{partition_num: 0}]

      {:ok, fetch_result} =
        API.fetch_committed_offset(client, group_id, topic_name, fetch_partitions, api_version: 6)

      assert [%{partition_offsets: [%{offset: committed_offset}]}] = fetch_result
      assert committed_offset == 42

      {:ok, _} = API.leave_group(client, group_id, member_id)
    end
  end

  # ---------------------------------------------------------------------------
  # JoinGroup V3
  # ---------------------------------------------------------------------------
  describe "JoinGroup V3 (highest working)" do
    @tag timeout: 120_000
    test "joins a consumer group and receives member_id", %{client: client} do
      topic_name = generate_random_string()
      group_id = "cg-join-v3-#{generate_random_string()}"
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          name: "assign",
          metadata: Fixtures.group_protocol_metadata(topics: [topic_name])
        }
      ]

      opts = [
        session_timeout: 30_000,
        rebalance_timeout: 60_000,
        group_protocols: group_protocols,
        api_version: 3
      ]

      {:ok, result} = API.join_group(client, group_id, "", opts)

      assert %JoinGroup{} = result
      assert is_binary(result.member_id)
      assert byte_size(result.member_id) > 0
      assert result.generation_id >= 1
      assert JoinGroup.leader?(result)
      assert is_binary(result.group_protocol)

      assert is_integer(result.throttle_time_ms)
      assert result.throttle_time_ms >= 0

      assert length(result.members) >= 1

      # Clean up: sync then leave
      group_assignment = [
        %{
          member_id: result.member_id,
          assignment:
            Fixtures.member_assignment(
              partition_assignments: [
                Fixtures.partition_assignment(topic: topic_name, partitions: [0])
              ]
            )
        }
      ]

      {:ok, _} =
        API.sync_group(client, group_id, result.generation_id, result.member_id, group_assignment: group_assignment)

      {:ok, _} = API.leave_group(client, group_id, result.member_id)
    end
  end

  # ---------------------------------------------------------------------------
  # SyncGroup V3
  # ---------------------------------------------------------------------------
  describe "SyncGroup V3 (highest working)" do
    @tag timeout: 120_000
    test "syncs group as leader and receives partition assignments", %{client: client} do
      topic_name = generate_random_string()
      group_id = "cg-sync-v3-#{generate_random_string()}"
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          name: "assign",
          metadata: Fixtures.group_protocol_metadata(topics: [topic_name])
        }
      ]

      join_opts = [
        session_timeout: 30_000,
        rebalance_timeout: 60_000,
        group_protocols: group_protocols
      ]

      {:ok, join_result} = retry_join_group(client, group_id, join_opts)

      member_id = join_result.member_id
      generation_id = join_result.generation_id

      group_assignment = [
        %{
          member_id: member_id,
          assignment:
            Fixtures.member_assignment(
              partition_assignments: [
                Fixtures.partition_assignment(topic: topic_name, partitions: [0])
              ]
            )
        }
      ]

      {:ok, sync_result} =
        API.sync_group(client, group_id, generation_id, member_id,
          group_assignment: group_assignment,
          api_version: 3
        )

      assert %SyncGroup{} = sync_result
      assert is_list(sync_result.partition_assignments)
      assert length(sync_result.partition_assignments) >= 1

      assert is_integer(sync_result.throttle_time_ms)
      assert sync_result.throttle_time_ms >= 0

      {:ok, _} = API.leave_group(client, group_id, member_id)
    end
  end

  # ---------------------------------------------------------------------------
  # Heartbeat V4 (FLEX)
  # ---------------------------------------------------------------------------
  describe "Heartbeat V4 (highest)" do
    @tag timeout: 120_000
    test "sends heartbeat to active group", %{client: client} do
      topic_name = generate_random_string()
      group_id = "cg-heartbeat-v4-#{generate_random_string()}"
      _ = create_topic(client, topic_name)

      {member_id, generation_id} = join_and_sync_group(client, topic_name, group_id)

      {:ok, result} = API.heartbeat(client, group_id, member_id, generation_id, api_version: 4)

      assert %Heartbeat{} = result

      assert is_integer(result.throttle_time_ms)
      assert result.throttle_time_ms >= 0

      {:ok, _} = API.leave_group(client, group_id, member_id)
    end
  end

  # ---------------------------------------------------------------------------
  # LeaveGroup V2
  # ---------------------------------------------------------------------------
  describe "LeaveGroup V2 (highest working)" do
    @tag timeout: 120_000
    test "leaves group successfully", %{client: client} do
      topic_name = generate_random_string()
      group_id = "cg-leave-v2-#{generate_random_string()}"
      _ = create_topic(client, topic_name)

      {member_id, _generation_id} = join_and_sync_group(client, topic_name, group_id)

      {:ok, result} = API.leave_group(client, group_id, member_id, api_version: 2)

      assert %LeaveGroup{} = result

      assert is_integer(result.throttle_time_ms)
      assert result.throttle_time_ms >= 0

      # Verify group is now empty
      Process.sleep(200)
      {:ok, group_info} = API.describe_group(client, group_id)
      assert group_info.members == []
    end
  end

  # ---------------------------------------------------------------------------
  # DescribeGroups V5 (FLEX)
  # ---------------------------------------------------------------------------
  describe "DescribeGroups V5 (highest)" do
    @tag timeout: 120_000
    test "describes an active consumer group", %{client: client} do
      topic_name = generate_random_string()
      group_id = "cg-describe-v5-#{generate_random_string()}"
      _ = create_topic(client, topic_name)

      {member_id, _generation_id} = join_and_sync_group(client, topic_name, group_id)

      {:ok, result} = API.describe_group(client, group_id, api_version: 5)

      assert %ConsumerGroupDescription{} = result
      assert result.group_id == group_id
      assert result.state == "Stable"
      assert is_binary(result.protocol_type)
      assert is_binary(result.protocol)
      assert length(result.members) == 1

      member = hd(result.members)
      assert member.member_id == member_id

      # V5 includes authorized_operations (may be -2147483648 if not requested)
      assert is_integer(result.authorized_operations) or is_nil(result.authorized_operations)

      {:ok, _} = API.leave_group(client, group_id, member_id)
    end
  end
end
