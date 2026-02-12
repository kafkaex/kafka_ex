defmodule KafkaEx.Integration.ConsumerGroup.LifecycleTest do
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Messages.SyncGroup
  alias KafkaEx.Messages.Heartbeat
  alias KafkaEx.Messages.LeaveGroup
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)

    {:ok, %{client: pid}}
  end

  describe "happy path" do
    test "single consumer full lifecycle: join -> sync -> heartbeat -> fetch -> commit -> leave", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce a message to consume later
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "test-message"}])

      # Step 1: Join the group
      group_protocols = [
        %{
          name: "assign",
          metadata: Fixtures.group_protocol_metadata(topics: [topic_name])
        }
      ]

      opts = [session_timeout: 30_000, rebalance_timeout: 60_000, group_protocols: group_protocols]
      {:ok, join_response} = API.join_group(client, consumer_group, "", opts)

      assert %JoinGroup{} = join_response
      assert join_response.generation_id >= 1
      assert JoinGroup.leader?(join_response)
      member_id = join_response.member_id
      generation_id = join_response.generation_id

      # Step 2: Sync the group (as leader)
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

      {:ok, sync_response} =
        API.sync_group(client, consumer_group, generation_id, member_id, group_assignment: group_assignment)

      assert %SyncGroup{} = sync_response
      assert sync_response.partition_assignments != []

      # Step 3: Send heartbeat
      {:ok, heartbeat_response} = API.heartbeat(client, consumer_group, member_id, generation_id)

      assert %Heartbeat{} = heartbeat_response

      # Step 4: Fetch messages
      {:ok, fetch_response} = API.fetch(client, topic_name, 0, 0)
      assert %KafkaEx.Messages.Fetch{} = fetch_response
      assert length(fetch_response.records) > 0

      # Step 5: Commit offset
      partitions = [%{partition_num: 0, offset: 1}]
      commit_opts = [generation_id: generation_id, member_id: member_id]
      {:ok, commit_response} = API.commit_offset(client, consumer_group, topic_name, partitions, commit_opts)
      assert [%{partition_offsets: [%{error_code: :no_error}]}] = commit_response

      # Step 6: Leave the group
      {:ok, leave_response} = API.leave_group(client, consumer_group, member_id)
      assert %LeaveGroup{} = leave_response

      # Verify group is empty after leaving
      Process.sleep(100)
      {:ok, group_info} = API.describe_group(client, consumer_group)
      assert group_info.members == []
    end
  end

  describe "unhappy path" do
    @tag timeout: 30_000
    test "session timeout removes member when heartbeat not sent", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          name: "assign",
          metadata: Fixtures.group_protocol_metadata(topics: [topic_name])
        }
      ]

      # Join with short session timeout (6 seconds)
      opts = [session_timeout: 6_000, rebalance_timeout: 10_000, group_protocols: group_protocols]
      {:ok, join_response} = API.join_group(client, consumer_group, "", opts)

      member_id = join_response.member_id
      generation_id = join_response.generation_id

      # Complete sync
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

      {:ok, _} = API.sync_group(client, consumer_group, generation_id, member_id, group_assignment: group_assignment)

      # Verify member is in group
      {:ok, group_before} = API.describe_group(client, consumer_group)
      assert length(group_before.members) == 1

      # Wait for session timeout (6s) plus buffer - DON'T send heartbeats
      Process.sleep(8_000)

      # Member should be removed due to timeout
      {:ok, group_after} = API.describe_group(client, consumer_group)
      assert group_after.members == []
    end

    @tag timeout: 30_000
    test "heartbeat with stale generation_id returns illegal_generation", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      group_protocols = [
        %{
          name: "assign",
          metadata: Fixtures.group_protocol_metadata(topics: [topic_name])
        }
      ]

      opts = [session_timeout: 30_000, rebalance_timeout: 60_000, group_protocols: group_protocols]
      {:ok, join_response} = API.join_group(client, consumer_group, "", opts)

      member_id = join_response.member_id
      generation_id = join_response.generation_id

      # Complete sync
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

      {:ok, _} = API.sync_group(client, consumer_group, generation_id, member_id, group_assignment: group_assignment)

      # Heartbeat with stale (old) generation_id should fail
      # Note: generation_id starts at 1, so we use max(0, generation_id - 1) to handle edge case
      # When generation_id is 1, using 0 as stale_generation_id is still invalid
      stale_generation_id = max(0, generation_id - 1)

      result = API.heartbeat(client, consumer_group, member_id, stale_generation_id)

      case result do
        {:error, :illegal_generation} -> :ok
        # Also acceptable - group may be rebalancing
        {:error, :rebalance_in_progress} -> :ok
        {:ok, %Heartbeat{}} -> flunk("Heartbeat with stale generation should fail, but succeeded")
        other -> flunk("Expected illegal_generation error, got: #{inspect(other)}")
      end

      # Clean up
      {:ok, _} = API.leave_group(client, consumer_group, member_id)
    end

    @tag timeout: 30_000
    test "commit with unknown member_id fails", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      # Try to commit with a made-up member_id (not part of any group)
      fake_member_id = "fake-member-#{:rand.uniform(10000)}"
      partitions = [%{partition_num: 0, offset: 10}]
      commit_opts = [generation_id: 1, member_id: fake_member_id]

      case API.commit_offset(client, consumer_group, topic_name, partitions, commit_opts) do
        {:ok, [%{partition_offsets: [%{error_code: error}]}]} when error in [:unknown_member_id, :illegal_generation] ->
          :ok

        {:error, error} when error in [:unknown_member_id, :illegal_generation, :coordinator_not_available] ->
          :ok

        other ->
          # Some error is expected - unknown member shouldn't be able to commit
          assert match?({:error, _}, other) or
                   match?({:ok, [%{partition_offsets: [%{error_code: e}]}]} when e != :no_error, other),
                 "Expected error for unknown member_id, got: #{inspect(other)}"
      end
    end
  end
end
