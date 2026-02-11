defmodule KafkaEx.Client.ResponseParserTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.ResponseParser
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.DeleteTopics
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Messages.Heartbeat
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Messages.JoinGroup.Member
  alias KafkaEx.Messages.LeaveGroup
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.Offset.PartitionOffset
  alias KafkaEx.Messages.SyncGroup
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  describe "offset_fetch_response/1" do
    test "parses successful OffsetFetch v1 response" do
      response = Fixtures.build_response(:offset_fetch, 1,
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "meta", error_code: 0}
            ]
          }
        ]
      )

      assert {:ok, [result]} = ResponseParser.offset_fetch_response(response)

      assert result == %Offset{
               topic: "test-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 100,
                   metadata: "meta",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses OffsetFetch v2 response with error_code" do
      response = Fixtures.build_response(:offset_fetch, 2,
        error_code: 0,
        topics: [
          %{
            name: "topic-a",
            partitions: [
              %{partition_index: 0, committed_offset: 200, metadata: "", error_code: 0},
              %{partition_index: 1, committed_offset: 300, metadata: "data", error_code: 0}
            ]
          }
        ]
      )

      assert {:ok, results} = ResponseParser.offset_fetch_response(response)
      assert length(results) == 2
    end

    test "returns error for failed OffsetFetch response" do
      response = Fixtures.build_response(:offset_fetch, 1,
        topics: [
          %{
            name: "error-topic",
            partitions: [
              %{partition_index: 0, committed_offset: -1, metadata: "", error_code: 15}
            ]
          }
        ]
      )

      assert {:error, error} = ResponseParser.offset_fetch_response(response)
      assert error.error == :coordinator_not_available
    end
  end

  describe "offset_commit_response/1" do
    test "parses successful OffsetCommit v2 response" do
      response = Fixtures.build_response(:offset_commit, 2,
        topics: [
          %{
            name: "commit-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
            ]
          }
        ]
      )

      assert {:ok, [result]} = ResponseParser.offset_commit_response(response)

      assert result == %Offset{
               topic: "commit-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   error_code: :no_error,
                   offset: nil,
                   metadata: nil,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses OffsetCommit v3 response with throttle_time_ms" do
      response = Fixtures.build_response(:offset_commit, 3,
        throttle_time_ms: 100,
        topics: [
          %{
            name: "throttled-topic",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 0}
            ]
          }
        ]
      )

      assert {:ok, results} = ResponseParser.offset_commit_response(response)
      assert length(results) == 2
    end

    test "returns error for failed OffsetCommit response" do
      response = Fixtures.build_response(:offset_commit, 2,
        topics: [
          %{
            name: "error-topic",
            partitions: [
              %{partition_index: 0, error_code: 16}
            ]
          }
        ]
      )

      assert {:error, error} = ResponseParser.offset_commit_response(response)
      assert error.error == :not_coordinator
    end
  end

  describe "heartbeat_response/1" do
    test "parses successful Heartbeat v0 response" do
      response = Fixtures.build_response(:heartbeat, 0, error_code: 0)

      assert {:ok, :no_error} = ResponseParser.heartbeat_response(response)
    end

    test "parses successful Heartbeat v1 response with throttle_time_ms" do
      response = Fixtures.build_response(:heartbeat, 1,
        error_code: 0,
        throttle_time_ms: 100
      )

      assert {:ok, heartbeat} = ResponseParser.heartbeat_response(response)
      assert %Heartbeat{throttle_time_ms: 100} = heartbeat
    end

    test "parses Heartbeat v1 response with zero throttle_time_ms" do
      response = Fixtures.build_response(:heartbeat, 1,
        error_code: 0,
        throttle_time_ms: 0
      )

      assert {:ok, heartbeat} = ResponseParser.heartbeat_response(response)
      assert heartbeat.throttle_time_ms == 0
    end

    test "returns error for unknown_member_id (v0)" do
      response = Fixtures.build_response(:heartbeat, 0, error_code: 25)

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :unknown_member_id
    end

    test "returns error for illegal_generation (v0)" do
      response = Fixtures.build_response(:heartbeat, 0, error_code: 22)

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :illegal_generation
    end

    test "returns error for rebalance_in_progress (v1)" do
      response = Fixtures.build_response(:heartbeat, 1,
        error_code: 27,
        throttle_time_ms: 50
      )

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "returns error for not_coordinator (v1)" do
      response = Fixtures.build_response(:heartbeat, 1,
        error_code: 16,
        throttle_time_ms: 0
      )

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :not_coordinator
    end

    test "returns error for coordinator_not_available (v0)" do
      response = Fixtures.build_response(:heartbeat, 0, error_code: 15)

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :coordinator_not_available
    end

    test "handles generic error with unknown error code (v0)" do
      response = Fixtures.build_response(:heartbeat, 0, error_code: 999)

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :unknown
    end

    test "handles generic error with unknown error code (v1)" do
      response = Fixtures.build_response(:heartbeat, 1,
        error_code: 999,
        throttle_time_ms: 10
      )

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :unknown
    end
  end

  describe "join_group_response/1" do
    test "parses successful JoinGroup v0 response" do
      response = Fixtures.build_response(:join_group, 0,
        error_code: 0,
        generation_id: 5,
        protocol_name: "assign",
        leader: "leader-123",
        member_id: "member-456",
        members: [
          %{member_id: "member-456", metadata: <<1, 2, 3>>},
          %{member_id: "member-789", metadata: <<4, 5, 6>>}
        ]
      )

      assert {:ok, join_group} = ResponseParser.join_group_response(response)

      assert %JoinGroup{
               generation_id: 5,
               group_protocol: "assign",
               leader_id: "leader-123",
               member_id: "member-456",
               throttle_time_ms: nil
             } = join_group

      assert length(join_group.members) == 2
      assert Enum.at(join_group.members, 0).member_id == "member-456"
    end

    test "parses successful JoinGroup v1 response" do
      response = Fixtures.build_response(:join_group, 1,
        error_code: 0,
        generation_id: 10,
        protocol_name: "roundrobin",
        leader: "leader-abc",
        member_id: "member-abc",
        members: []
      )

      assert {:ok, join_group} = ResponseParser.join_group_response(response)

      assert %JoinGroup{
               generation_id: 10,
               group_protocol: "roundrobin",
               leader_id: "leader-abc",
               member_id: "member-abc",
               throttle_time_ms: nil,
               members: []
             } = join_group
    end

    test "parses successful JoinGroup v2 response with throttle_time_ms" do
      response = Fixtures.build_response(:join_group, 2,
        error_code: 0,
        throttle_time_ms: 150,
        generation_id: 3,
        protocol_name: "range",
        leader: "leader-xyz",
        member_id: "member-xyz",
        members: [
          %{member_id: "member-xyz", metadata: <<>>}
        ]
      )

      assert {:ok, join_group} = ResponseParser.join_group_response(response)

      assert %JoinGroup{
               throttle_time_ms: 150,
               generation_id: 3,
               group_protocol: "range",
               leader_id: "leader-xyz",
               member_id: "member-xyz"
             } = join_group

      assert length(join_group.members) == 1
    end

    test "parses JoinGroup v2 response with zero throttle_time_ms" do
      response = Fixtures.build_response(:join_group, 2,
        error_code: 0,
        throttle_time_ms: 0,
        generation_id: 1,
        protocol_name: "assign",
        leader: "leader",
        member_id: "member",
        members: []
      )

      assert {:ok, join_group} = ResponseParser.join_group_response(response)
      assert join_group.throttle_time_ms == 0
    end

    test "parses JoinGroup response with multiple members" do
      members = [
        %{member_id: "member-1", metadata: <<1>>},
        %{member_id: "member-2", metadata: <<2>>},
        %{member_id: "member-3", metadata: <<3>>}
      ]

      response = Fixtures.build_response(:join_group, 0,
        error_code: 0,
        generation_id: 7,
        protocol_name: "sticky",
        leader: "member-1",
        member_id: "member-1",
        members: members
      )

      assert {:ok, join_group} = ResponseParser.join_group_response(response)
      assert length(join_group.members) == 3

      Enum.each(join_group.members, fn member ->
        assert %Member{} = member
        assert member.member_id in ["member-1", "member-2", "member-3"]
      end)
    end

    test "returns error for unknown_member_id (v0)" do
      response = Fixtures.build_response(:join_group, 0,
        error_code: 25,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      )

      assert {:error, error} = ResponseParser.join_group_response(response)
      assert error.error == :unknown_member_id
    end

    test "returns error for illegal_generation (v0)" do
      response = Fixtures.build_response(:join_group, 0,
        error_code: 22,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      )

      assert {:error, error} = ResponseParser.join_group_response(response)
      assert error.error == :illegal_generation
    end

    test "returns error for rebalance_in_progress (v2)" do
      response = Fixtures.build_response(:join_group, 2,
        error_code: 27,
        throttle_time_ms: 50,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      )

      assert {:error, error} = ResponseParser.join_group_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "returns error for not_coordinator (v1)" do
      response = Fixtures.build_response(:join_group, 1,
        error_code: 16,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      )

      assert {:error, error} = ResponseParser.join_group_response(response)
      assert error.error == :not_coordinator
    end

    test "returns error for coordinator_not_available (v0)" do
      response = Fixtures.build_response(:join_group, 0,
        error_code: 15,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      )

      assert {:error, error} = ResponseParser.join_group_response(response)
      assert error.error == :coordinator_not_available
    end

    test "returns error for group_authorization_failed (v2)" do
      response = Fixtures.build_response(:join_group, 2,
        error_code: 30,
        throttle_time_ms: 0,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      )

      assert {:error, error} = ResponseParser.join_group_response(response)
      assert error.error == :group_authorization_failed
    end

    test "handles generic error with unknown error code (v0)" do
      response = Fixtures.build_response(:join_group, 0,
        error_code: 999,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      )

      assert {:error, error} = ResponseParser.join_group_response(response)
      assert error.error == :unknown
    end

    test "handles generic error with unknown error code (v2)" do
      response = Fixtures.build_response(:join_group, 2,
        error_code: 999,
        throttle_time_ms: 10,
        generation_id: 0,
        protocol_name: "",
        leader: "",
        member_id: "",
        members: []
      )

      assert {:error, error} = ResponseParser.join_group_response(response)
      assert error.error == :unknown
    end
  end

  describe "leave_group_response/1" do
    test "parses successful LeaveGroup v0 response" do
      response = Fixtures.build_response(:leave_group, 0, error_code: 0)

      assert {:ok, :no_error} = ResponseParser.leave_group_response(response)
    end

    test "parses successful LeaveGroup v1 response with throttle_time_ms" do
      response = Fixtures.build_response(:leave_group, 1,
        error_code: 0,
        throttle_time_ms: 100
      )

      assert {:ok, leave_group} = ResponseParser.leave_group_response(response)
      assert %LeaveGroup{throttle_time_ms: 100} = leave_group
    end

    test "parses LeaveGroup v1 response with zero throttle_time_ms" do
      response = Fixtures.build_response(:leave_group, 1,
        error_code: 0,
        throttle_time_ms: 0
      )

      assert {:ok, leave_group} = ResponseParser.leave_group_response(response)
      assert leave_group.throttle_time_ms == 0
    end

    test "parses LeaveGroup v1 response with large throttle_time_ms" do
      response = Fixtures.build_response(:leave_group, 1,
        error_code: 0,
        throttle_time_ms: 5000
      )

      assert {:ok, leave_group} = ResponseParser.leave_group_response(response)
      assert leave_group.throttle_time_ms == 5000
    end

    test "returns error for unknown_member_id (v0)" do
      response = Fixtures.build_response(:leave_group, 0, error_code: 25)

      assert {:error, error} = ResponseParser.leave_group_response(response)
      assert error.error == :unknown_member_id
    end

    test "returns error for group_id_not_found (v0)" do
      response = Fixtures.build_response(:leave_group, 0, error_code: 69)

      assert {:error, error} = ResponseParser.leave_group_response(response)
      assert error.error == :group_id_not_found
    end

    test "returns error for rebalance_in_progress (v1)" do
      response = Fixtures.build_response(:leave_group, 1,
        error_code: 27,
        throttle_time_ms: 50
      )

      assert {:error, error} = ResponseParser.leave_group_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "returns error for not_coordinator (v1)" do
      response = Fixtures.build_response(:leave_group, 1,
        error_code: 16,
        throttle_time_ms: 0
      )

      assert {:error, error} = ResponseParser.leave_group_response(response)
      assert error.error == :not_coordinator
    end

    test "returns error for coordinator_not_available (v0)" do
      response = Fixtures.build_response(:leave_group, 0, error_code: 15)

      assert {:error, error} = ResponseParser.leave_group_response(response)
      assert error.error == :coordinator_not_available
    end

    test "returns error for group_authorization_failed (v0)" do
      response = Fixtures.build_response(:leave_group, 0, error_code: 30)

      assert {:error, error} = ResponseParser.leave_group_response(response)
      assert error.error == :group_authorization_failed
    end

    test "handles generic error with unknown error code (v0)" do
      response = Fixtures.build_response(:leave_group, 0, error_code: 999)

      assert {:error, error} = ResponseParser.leave_group_response(response)
      assert error.error == :unknown
    end

    test "handles generic error with unknown error code (v1)" do
      response = Fixtures.build_response(:leave_group, 1,
        error_code: 999,
        throttle_time_ms: 10
      )

      assert {:error, error} = ResponseParser.leave_group_response(response)
      assert error.error == :unknown
    end
  end

  describe "produce_response/1" do
    alias KafkaEx.Messages.RecordMetadata

    test "parses successful Produce v0 response" do
      response = Fixtures.build_response(:produce, 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 42}
            ]
          }
        ]
      )

      assert {:ok, record_metadata} = ResponseParser.produce_response(response)
      assert %RecordMetadata{} = record_metadata
      assert record_metadata.topic == "test-topic"
      assert record_metadata.partition == 0
      assert record_metadata.base_offset == 42
    end

    test "parses successful Produce v2 response with log_append_time" do
      response = Fixtures.build_response(:produce, 2,
        responses: [
          %{
            topic: "events",
            partition_responses: [
              %{partition: 1, error_code: 0, base_offset: 100, log_append_time: 1_702_000_000_000}
            ]
          }
        ]
      )

      assert {:ok, record_metadata} = ResponseParser.produce_response(response)
      assert record_metadata.topic == "events"
      assert record_metadata.partition == 1
      assert record_metadata.base_offset == 100
      assert record_metadata.log_append_time == 1_702_000_000_000
    end

    test "parses successful Produce v3 response with throttle_time_ms" do
      response = Fixtures.build_response(:produce, 3,
        throttle_time_ms: 50,
        responses: [
          %{
            topic: "transactions",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 500, log_append_time: -1}
            ]
          }
        ]
      )

      assert {:ok, record_metadata} = ResponseParser.produce_response(response)
      assert record_metadata.topic == "transactions"
      assert record_metadata.throttle_time_ms == 50
      assert record_metadata.log_append_time == -1
    end

    test "returns error for failed Produce response" do
      response = Fixtures.build_response(:produce, 0,
        responses: [
          %{
            topic: "error-topic",
            partition_responses: [
              %{partition: 0, error_code: 3, base_offset: -1}
            ]
          }
        ]
      )

      assert {:error, error} = ResponseParser.produce_response(response)
      assert error.error == :unknown_topic_or_partition
    end

    test "returns error for empty responses" do
      response = Fixtures.build_response(:produce, 0, responses: [])

      assert {:error, error} = ResponseParser.produce_response(response)
      assert error.error == :empty_response
    end
  end

  describe "api_versions_response/1" do
    test "parses successful ApiVersions v0 response" do
      response = Fixtures.build_response(:api_versions, 0,
        error_code: 0,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 8},
          %{api_key: 1, min_version: 0, max_version: 11},
          %{api_key: 3, min_version: 0, max_version: 9}
        ]
      )

      assert {:ok, api_versions} = ResponseParser.api_versions_response(response)
      assert %ApiVersions{} = api_versions
      assert is_map(api_versions.api_versions)
      assert api_versions.api_versions[0] == %{min_version: 0, max_version: 8}
      assert api_versions.api_versions[1] == %{min_version: 0, max_version: 11}
    end

    test "parses successful ApiVersions v1 response with throttle_time_ms" do
      response = Fixtures.build_response(:api_versions, 1,
        error_code: 0,
        throttle_time_ms: 100,
        api_keys: [
          %{api_key: 0, min_version: 0, max_version: 9},
          %{api_key: 2, min_version: 0, max_version: 5}
        ]
      )

      assert {:ok, api_versions} = ResponseParser.api_versions_response(response)
      assert %ApiVersions{} = api_versions
      assert api_versions.throttle_time_ms == 100
      assert api_versions.api_versions[2] == %{min_version: 0, max_version: 5}
    end

    test "returns error for failed ApiVersions response" do
      response = Fixtures.build_response(:api_versions, 0,
        error_code: 35,
        api_keys: []
      )

      assert {:error, error} = ResponseParser.api_versions_response(response)
      assert error.error == :unsupported_version
    end
  end

  describe "describe_groups_response/1" do
    test "parses successful DescribeGroups v0 response with empty members" do
      response = Fixtures.build_response(:describe_groups, 0,
        groups: [
          %{
            error_code: 0,
            group_id: "test-group",
            group_state: "Stable",
            protocol_type: "consumer",
            protocol_data: "range",
            members: []
          }
        ]
      )

      assert {:ok, groups} = ResponseParser.describe_groups_response(response)
      assert length(groups) == 1
      assert %ConsumerGroupDescription{} = hd(groups)
      assert hd(groups).group_id == "test-group"
      assert hd(groups).state == "Stable"
    end

    test "parses successful DescribeGroups v0 response with members" do
      response = Fixtures.build_response(:describe_groups, 0,
        groups: [
          %{
            error_code: 0,
            group_id: "test-group",
            group_state: "Stable",
            protocol_type: "consumer",
            protocol_data: "range",
            members: [
              %{
                member_id: "member-1",
                client_id: "client-1",
                client_host: "/127.0.0.1",
                member_metadata: %{version: 0, topics: ["topic1"], user_data: nil},
                member_assignment: %{
                  version: 0,
                  partition_assignments: [%{topic: "topic1", partitions: [0, 1]}],
                  user_data: nil
                }
              }
            ]
          }
        ]
      )

      assert {:ok, groups} = ResponseParser.describe_groups_response(response)
      assert length(groups) == 1
      group = hd(groups)
      assert %ConsumerGroupDescription{} = group
      assert group.group_id == "test-group"
      assert group.state == "Stable"
      assert length(group.members) == 1
    end

    test "parses DescribeGroups v1 response with multiple groups" do
      response = Fixtures.build_response(:describe_groups, 1,
        groups: [
          %{
            error_code: 0,
            group_id: "group-a",
            group_state: "Stable",
            protocol_type: "consumer",
            protocol_data: "roundrobin",
            members: []
          },
          %{
            error_code: 0,
            group_id: "group-b",
            group_state: "Empty",
            protocol_type: "consumer",
            protocol_data: "",
            members: []
          }
        ]
      )

      assert {:ok, groups} = ResponseParser.describe_groups_response(response)
      assert length(groups) == 2
      group_ids = Enum.map(groups, & &1.group_id)
      assert "group-a" in group_ids
      assert "group-b" in group_ids
    end

    test "returns error for failed DescribeGroups response" do
      response = Fixtures.build_response(:describe_groups, 0,
        groups: [
          %{
            error_code: 16,
            group_id: "error-group",
            state: "",
            protocol_type: "",
            protocol: "",
            members: []
          }
        ]
      )

      assert {:error, error_list} = ResponseParser.describe_groups_response(response)
      assert [{"error-group", :not_coordinator}] = error_list
    end
  end

  describe "list_offsets_response/1" do
    test "parses successful ListOffsets v0 response" do
      response = Fixtures.build_response(:list_offsets, 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offsets: [100, 50, 0]}
            ]
          }
        ]
      )

      assert {:ok, offsets} = ResponseParser.list_offsets_response(response)
      assert length(offsets) == 1
      assert %Offset{} = hd(offsets)
      assert hd(offsets).topic == "test-topic"
    end

    test "parses successful ListOffsets v1 response" do
      response = Fixtures.build_response(:list_offsets, 1,
        responses: [
          %{
            topic: "events",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 500, timestamp: -1}
            ]
          }
        ]
      )

      assert {:ok, offsets} = ResponseParser.list_offsets_response(response)
      assert length(offsets) == 1
      assert hd(offsets).topic == "events"
    end

    test "returns error for failed ListOffsets response" do
      response = Fixtures.build_response(:list_offsets, 0,
        responses: [
          %{
            topic: "error-topic",
            partition_responses: [
              %{partition: 0, error_code: 3, offsets: []}
            ]
          }
        ]
      )

      assert {:error, error} = ResponseParser.list_offsets_response(response)
      assert error.error == :unknown_topic_or_partition
    end
  end

  describe "metadata_response/1" do
    test "parses successful Metadata v0 response" do
      response = Fixtures.build_response(:metadata, 0,
        brokers: [
          %{node_id: 0, host: "localhost", port: 9092}
        ],
        topics: [
          %{
            error_code: 0,
            name: "test-topic",
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 0, replica_nodes: [0], isr_nodes: [0]}
            ]
          }
        ]
      )

      assert {:ok, metadata} = ResponseParser.metadata_response(response)
      assert %ClusterMetadata{} = metadata
      assert map_size(metadata.brokers) == 1
      assert metadata.brokers[0].host == "localhost"
      assert map_size(metadata.topics) == 1
      assert metadata.topics["test-topic"].name == "test-topic"
    end

    test "parses Metadata v1 response with controller_id" do
      response = Fixtures.build_response(:metadata, 1,
        brokers: [
          %{node_id: 1, host: "broker1", port: 9092, rack: "rack1"},
          %{node_id: 2, host: "broker2", port: 9092, rack: "rack2"}
        ],
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "events",
            is_internal: false,
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1, 2], isr_nodes: [1, 2]},
              %{error_code: 0, partition_index: 1, leader_id: 2, replica_nodes: [2, 1], isr_nodes: [2, 1]}
            ]
          }
        ]
      )

      assert {:ok, metadata} = ResponseParser.metadata_response(response)
      assert %ClusterMetadata{} = metadata
      assert metadata.controller_id == 1
      assert map_size(metadata.brokers) == 2
      assert metadata.brokers[1].rack == "rack1"
      topic = metadata.topics["events"]
      assert length(topic.partitions) == 2
    end

    test "parses Metadata response with topic errors (filters out errored topics)" do
      response = Fixtures.build_response(:metadata, 0,
        brokers: [%{node_id: 0, host: "localhost", port: 9092}],
        topics: [
          %{
            error_code: 0,
            name: "good-topic",
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 0, replica_nodes: [0], isr_nodes: [0]}
            ]
          },
          %{
            error_code: 3,
            name: "bad-topic",
            partitions: []
          }
        ]
      )

      assert {:ok, metadata} = ResponseParser.metadata_response(response)
      # Errored topics are filtered out
      assert map_size(metadata.topics) == 1
      assert Map.has_key?(metadata.topics, "good-topic")
      refute Map.has_key?(metadata.topics, "bad-topic")
    end
  end

  describe "fetch_response/1" do
    test "parses successful Fetch v0 response" do
      response = Fixtures.build_response(:fetch, 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{
                partition_header: %{partition: 0, error_code: 0, high_watermark: 100},
                record_set: Fixtures.message_set(
                  messages: [
                    %{offset: 0, key: "key1", value: "value1", timestamp: nil, attributes: 0, crc: 123}
                  ]
                )
              }
            ]
          }
        ]
      )

      assert {:ok, fetch} = ResponseParser.fetch_response(response)
      assert %Fetch{} = fetch
      assert fetch.topic == "test-topic"
      assert fetch.partition == 0
      assert fetch.high_watermark == 100
      assert length(fetch.records) == 1
    end

    test "parses successful Fetch v1 response with throttle_time_ms" do
      response = Fixtures.build_response(:fetch, 1,
        throttle_time_ms: 50,
        responses: [
          %{
            topic: "events",
            partition_responses: [
              %{
                partition_header: %{partition: 1, error_code: 0, high_watermark: 500},
                record_set: nil
              }
            ]
          }
        ]
      )

      assert {:ok, fetch} = ResponseParser.fetch_response(response)
      assert %Fetch{} = fetch
      assert fetch.topic == "events"
      assert fetch.partition == 1
      assert fetch.throttle_time_ms == 50
      assert fetch.records == []
    end

    test "returns error for failed Fetch response" do
      response = Fixtures.build_response(:fetch, 0,
        responses: [
          %{
            topic: "error-topic",
            partition_responses: [
              %{
                partition_header: %{partition: 0, error_code: 1, high_watermark: 0},
                record_set: nil
              }
            ]
          }
        ]
      )

      assert {:error, error} = ResponseParser.fetch_response(response)
      assert error.error == :offset_out_of_range
    end

    test "returns error for empty Fetch response" do
      response = Fixtures.build_response(:fetch, 0, responses: [])

      assert {:error, error} = ResponseParser.fetch_response(response)
      assert error.error == :empty_response
    end
  end

  describe "find_coordinator_response/1" do
    test "parses successful FindCoordinator v0 response" do
      response = Fixtures.build_response(:find_coordinator, 0,
        error_code: 0,
        node_id: 1,
        host: "broker1",
        port: 9092
      )

      assert {:ok, result} = ResponseParser.find_coordinator_response(response)
      assert %FindCoordinator{} = result
      assert %Broker{} = result.coordinator
      assert result.coordinator.node_id == 1
      assert result.coordinator.host == "broker1"
      assert result.coordinator.port == 9092
    end

    test "parses successful FindCoordinator v1 response with throttle_time_ms" do
      response = Fixtures.build_response(:find_coordinator, 1,
        error_code: 0,
        throttle_time_ms: 100,
        error_message: nil,
        node_id: 2,
        host: "broker2",
        port: 9093
      )

      assert {:ok, result} = ResponseParser.find_coordinator_response(response)
      assert %FindCoordinator{} = result
      assert result.throttle_time_ms == 100
      assert result.coordinator.node_id == 2
    end

    test "returns error for failed FindCoordinator response" do
      response = Fixtures.build_response(:find_coordinator, 0,
        error_code: 15,
        node_id: nil,
        host: nil,
        port: nil
      )

      assert {:error, error} = ResponseParser.find_coordinator_response(response)
      assert error.error == :coordinator_not_available
    end

    test "returns error for group authorization failed" do
      response = Fixtures.build_response(:find_coordinator, 1,
        error_code: 30,
        throttle_time_ms: 0,
        error_message: "Group authorization failed",
        node_id: nil,
        host: nil,
        port: nil
      )

      assert {:error, error} = ResponseParser.find_coordinator_response(response)
      assert error.error == :group_authorization_failed
    end
  end

  describe "sync_group_response/1" do
    test "parses successful SyncGroup v0 response" do
      response = Fixtures.build_response(:sync_group, 0,
        error_code: 0,
        assignment: Fixtures.member_assignment(
          version: 0,
          partition_assignments: [
            Fixtures.partition_assignment(
              topic: "test-topic",
              partitions: [0, 1, 2]
            )
          ],
          user_data: nil
        )
      )

      assert {:ok, sync_group} = ResponseParser.sync_group_response(response)
      assert %SyncGroup{} = sync_group
      assert length(sync_group.partition_assignments) == 1
      assignment = hd(sync_group.partition_assignments)
      assert assignment.topic == "test-topic"
      assert assignment.partitions == [0, 1, 2]
    end

    test "parses successful SyncGroup v1 response with throttle_time_ms" do
      response = Fixtures.build_response(:sync_group, 1,
        error_code: 0,
        throttle_time_ms: 75,
        assignment: Fixtures.member_assignment(
          version: 0,
          partition_assignments: [
            Fixtures.partition_assignment(
              topic: "events",
              partitions: [0]
            ),
            Fixtures.partition_assignment(
              topic: "commands",
              partitions: [1, 2]
            )
          ],
          user_data: nil
        )
      )

      assert {:ok, sync_group} = ResponseParser.sync_group_response(response)
      assert %SyncGroup{} = sync_group
      assert sync_group.throttle_time_ms == 75
      assert length(sync_group.partition_assignments) == 2
    end

    test "parses SyncGroup response with empty assignment" do
      response = Fixtures.build_response(:sync_group, 0,
        error_code: 0,
        assignment: nil
      )

      assert {:ok, sync_group} = ResponseParser.sync_group_response(response)
      assert %SyncGroup{} = sync_group
      assert sync_group.partition_assignments == []
    end

    test "returns error for rebalance_in_progress" do
      response = Fixtures.build_response(:sync_group, 0,
        error_code: 27,
        assignment: nil
      )

      assert {:error, error} = ResponseParser.sync_group_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "returns error for unknown_member_id" do
      response = Fixtures.build_response(:sync_group, 1,
        error_code: 25,
        throttle_time_ms: 0,
        assignment: nil
      )

      assert {:error, error} = ResponseParser.sync_group_response(response)
      assert error.error == :unknown_member_id
    end
  end

  describe "create_topics_response/1" do
    test "parses successful CreateTopics v0 response" do
      response = Fixtures.build_response(:create_topics, 0,
        topics: [
          %{name: "new-topic", error_code: 0}
        ]
      )

      assert {:ok, result} = ResponseParser.create_topics_response(response)
      assert %CreateTopics{} = result
      assert length(result.topic_results) == 1
      topic_result = hd(result.topic_results)
      assert topic_result.topic == "new-topic"
      assert topic_result.error == :no_error
    end

    test "parses CreateTopics v1 response with error_message" do
      response = Fixtures.build_response(:create_topics, 1,
        topics: [
          %{name: "topic-a", error_code: 0, error_message: nil},
          %{name: "topic-b", error_code: 0, error_message: nil}
        ]
      )

      assert {:ok, result} = ResponseParser.create_topics_response(response)
      assert %CreateTopics{} = result
      assert length(result.topic_results) == 2
    end

    test "parses CreateTopics response with topic already exists error" do
      response = Fixtures.build_response(:create_topics, 0,
        topics: [
          %{name: "existing-topic", error_code: 36}
        ]
      )

      assert {:ok, result} = ResponseParser.create_topics_response(response)
      topic_result = hd(result.topic_results)
      assert topic_result.topic == "existing-topic"
      assert topic_result.error == :topic_already_exists
    end
  end

  describe "delete_topics_response/1" do
    test "parses successful DeleteTopics v0 response" do
      response = Fixtures.build_response(:delete_topics, 0,
        responses: [
          %{name: "deleted-topic", error_code: 0}
        ]
      )

      assert {:ok, result} = ResponseParser.delete_topics_response(response)
      assert %DeleteTopics{} = result
      assert length(result.topic_results) == 1
      topic_result = hd(result.topic_results)
      assert topic_result.topic == "deleted-topic"
      assert topic_result.error == :no_error
    end

    test "parses DeleteTopics v1 response with throttle_time_ms" do
      response = Fixtures.build_response(:delete_topics, 1,
        throttle_time_ms: 50,
        responses: [
          %{name: "topic-1", error_code: 0},
          %{name: "topic-2", error_code: 0}
        ]
      )

      assert {:ok, result} = ResponseParser.delete_topics_response(response)
      assert %DeleteTopics{} = result
      assert result.throttle_time_ms == 50
      assert length(result.topic_results) == 2
    end

    test "parses DeleteTopics response with unknown topic error" do
      response = Fixtures.build_response(:delete_topics, 0,
        responses: [
          %{name: "nonexistent-topic", error_code: 3}
        ]
      )

      assert {:ok, result} = ResponseParser.delete_topics_response(response)
      topic_result = hd(result.topic_results)
      assert topic_result.topic == "nonexistent-topic"
      assert topic_result.error == :unknown_topic_or_partition
    end
  end
end
