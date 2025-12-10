defmodule KafkaEx.New.Client.RequestBuilderTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Client.RequestBuilder

  describe "describe_groups_request/2" do
    test "returns request for DescribeGroups API" do
      state = %KafkaEx.New.Client.State{api_versions: %{15 => {0, 1}}}
      group_names = ["group1", "group2"]

      expected_request = %Kayrock.DescribeGroups.V1.Request{group_ids: group_names}

      {:ok, request} = RequestBuilder.describe_groups_request([group_names: group_names], state)

      assert expected_request == request
    end

    test "returns request with custom API version" do
      state = %KafkaEx.New.Client.State{api_versions: %{15 => {0, 1}}}
      group_names = ["group1", "group2"]

      expected_request = %Kayrock.DescribeGroups.V0.Request{group_ids: group_names}

      {:ok, request} = RequestBuilder.describe_groups_request([group_names: group_names, api_version: 0], state)

      assert expected_request == request
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.New.Client.State{api_versions: %{15 => {0, 1}}}
      group_names = ["group1", "group2"]

      {:error, error_value} = RequestBuilder.describe_groups_request([group_names: group_names, api_version: 3], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "lists_offset_request/2" do
    test "returns request for ListOffsets API" do
      state = %KafkaEx.New.Client.State{api_versions: %{2 => {0, 2}}}
      topic_data = [{"test-topic", [%{partition_num: 1, timestamp: :latest}]}]

      {:ok, request} = RequestBuilder.lists_offset_request([topics: topic_data], state)

      expected_request = %Kayrock.ListOffsets.V1.Request{
        replica_id: -1,
        topics: [%{partitions: [%{timestamp: -1, partition: 1}], topic: "test-topic"}]
      }

      assert expected_request == request
    end

    test "returns request with custom API version" do
      state = %KafkaEx.New.Client.State{api_versions: %{2 => {0, 2}}}
      topic_data = [{"test-topic", [%{partition_num: 1, timestamp: :latest}]}]

      {:ok, request} = RequestBuilder.lists_offset_request([topics: topic_data, api_version: 2], state)

      expected_request = %Kayrock.ListOffsets.V2.Request{
        replica_id: -1,
        isolation_level: 0,
        topics: [%{partitions: [%{timestamp: -1, partition: 1}], topic: "test-topic"}]
      }

      assert expected_request == request
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.New.Client.State{api_versions: %{2 => {0, 2}}}
      topic_data = [{"test-topic", [%{partition_num: 1, timestamp: :latest}]}]

      {:error, error_value} = RequestBuilder.lists_offset_request([topics: topic_data, api_version: 3], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "offset_fetch_request/2" do
    test "returns request for OffsetFetch API with default version" do
      state = %KafkaEx.New.Client.State{api_versions: %{9 => {0, 3}}}
      group_id = "test-group"
      topics = [{"test-topic", [%{partition_num: 0}]}]

      {:ok, request} = RequestBuilder.offset_fetch_request([group_id: group_id, topics: topics], state)

      expected_request = %Kayrock.OffsetFetch.V1.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "test-group",
        topics: [%{topic: "test-topic", partitions: [%{partition: 0}]}]
      }

      assert expected_request == request
    end

    test "returns request with custom API version" do
      state = %KafkaEx.New.Client.State{api_versions: %{9 => {0, 3}}}
      group_id = "consumer-group"
      topics = [{"my-topic", [%{partition_num: 1}, %{partition_num: 2}]}]

      {:ok, request} = RequestBuilder.offset_fetch_request([group_id: group_id, topics: topics, api_version: 2], state)

      expected_request = %Kayrock.OffsetFetch.V2.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "consumer-group",
        topics: [%{topic: "my-topic", partitions: [%{partition: 1}, %{partition: 2}]}]
      }

      assert expected_request == request
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.New.Client.State{api_versions: %{9 => {0, 2}}}
      group_id = "test-group"
      topics = [{"test-topic", [%{partition_num: 0}]}]

      {:error, error_value} =
        RequestBuilder.offset_fetch_request([group_id: group_id, topics: topics, api_version: 5], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "offset_commit_request/2" do
    test "returns request for OffsetCommit API v1 (default)" do
      state = %KafkaEx.New.Client.State{api_versions: %{8 => {0, 3}}}
      group_id = "test-group"
      topics = [{"test-topic", [%{partition_num: 0, offset: 100}]}]

      {:ok, request} = RequestBuilder.offset_commit_request([group_id: group_id, topics: topics], state)

      expected_request = %Kayrock.OffsetCommit.V1.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "test-group",
        generation_id: -1,
        member_id: "",
        topics: [%{topic: "test-topic", partitions: [%{partition: 0, offset: 100, timestamp: -1, metadata: ""}]}]
      }

      assert expected_request == request
    end

    test "returns request with v1 (generation_id and member_id)" do
      state = %KafkaEx.New.Client.State{api_versions: %{8 => {0, 3}}}
      group_id = "consumer-group"
      topics = [{"my-topic", [%{partition_num: 1, offset: 200}]}]

      {:ok, request} =
        RequestBuilder.offset_commit_request(
          [
            group_id: group_id,
            topics: topics,
            api_version: 1,
            generation_id: 5,
            member_id: "member-123"
          ],
          state
        )

      expected_request = %Kayrock.OffsetCommit.V1.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "consumer-group",
        generation_id: 5,
        member_id: "member-123",
        topics: [%{topic: "my-topic", partitions: [%{partition: 1, offset: 200, timestamp: -1, metadata: ""}]}]
      }

      assert expected_request == request
    end

    test "returns request with v2 and custom retention_time" do
      state = %KafkaEx.New.Client.State{api_versions: %{8 => {0, 3}}}
      group_id = "retention-group"
      topics = [{"topic-a", [%{partition_num: 0, offset: 300}]}]

      {:ok, request} =
        RequestBuilder.offset_commit_request(
          [
            group_id: group_id,
            topics: topics,
            api_version: 2,
            retention_time: 86_400_000,
            generation_id: 10,
            member_id: "member-abc"
          ],
          state
        )

      expected_request = %Kayrock.OffsetCommit.V2.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "retention-group",
        generation_id: 10,
        member_id: "member-abc",
        retention_time: 86_400_000,
        topics: [%{topic: "topic-a", partitions: [%{partition: 0, offset: 300, metadata: ""}]}]
      }

      assert expected_request == request
    end

    test "returns request with v0 (no generation_id or member_id)" do
      state = %KafkaEx.New.Client.State{api_versions: %{8 => {0, 3}}}
      group_id = "legacy-group"
      topics = [{"legacy-topic", [%{partition_num: 0, offset: 50}]}]

      {:ok, request} =
        RequestBuilder.offset_commit_request(
          [
            group_id: group_id,
            topics: topics,
            api_version: 0
          ],
          state
        )

      expected_request = %Kayrock.OffsetCommit.V0.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "legacy-group",
        topics: [%{topic: "legacy-topic", partitions: [%{partition: 0, offset: 50, metadata: ""}]}]
      }

      assert expected_request == request
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.New.Client.State{api_versions: %{8 => {0, 2}}}
      group_id = "test-group"
      topics = [{"test-topic", [%{partition_num: 0, offset: 100}]}]

      {:error, error_value} =
        RequestBuilder.offset_commit_request([group_id: group_id, topics: topics, api_version: 5], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "heartbeat_request/2" do
    test "returns request for Heartbeat API v1 (default)" do
      state = %KafkaEx.New.Client.State{api_versions: %{12 => {0, 1}}}
      group_id = "test-group"
      member_id = "consumer-123"
      generation_id = 5

      {:ok, request} =
        RequestBuilder.heartbeat_request(
          [group_id: group_id, member_id: member_id, generation_id: generation_id],
          state
        )

      expected_request = %Kayrock.Heartbeat.V1.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "test-group",
        member_id: "consumer-123",
        generation_id: 5
      }

      assert expected_request == request
    end

    test "returns request for Heartbeat API v0 when explicitly requested" do
      state = %KafkaEx.New.Client.State{api_versions: %{12 => {0, 1}}}
      group_id = "test-group"
      member_id = "consumer-123"
      generation_id = 5

      {:ok, request} =
        RequestBuilder.heartbeat_request(
          [group_id: group_id, member_id: member_id, generation_id: generation_id, api_version: 0],
          state
        )

      expected_request = %Kayrock.Heartbeat.V0.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "test-group",
        member_id: "consumer-123",
        generation_id: 5
      }

      assert expected_request == request
    end

    test "returns request for Heartbeat API v1" do
      state = %KafkaEx.New.Client.State{api_versions: %{12 => {0, 1}}}
      group_id = "consumer-group"
      member_id = "member-abc"
      generation_id = 10

      {:ok, request} =
        RequestBuilder.heartbeat_request(
          [
            group_id: group_id,
            member_id: member_id,
            generation_id: generation_id,
            api_version: 1
          ],
          state
        )

      expected_request = %Kayrock.Heartbeat.V1.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "consumer-group",
        member_id: "member-abc",
        generation_id: 10
      }

      assert expected_request == request
    end

    test "handles generation_id 0" do
      state = %KafkaEx.New.Client.State{api_versions: %{12 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.heartbeat_request(
          [
            group_id: "group",
            member_id: "member",
            generation_id: 0
          ],
          state
        )

      assert request.generation_id == 0
    end

    test "handles empty member_id" do
      state = %KafkaEx.New.Client.State{api_versions: %{12 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.heartbeat_request(
          [
            group_id: "group",
            member_id: "",
            generation_id: 1
          ],
          state
        )

      assert request.member_id == ""
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.New.Client.State{api_versions: %{12 => {0, 1}}}

      {:error, error_value} =
        RequestBuilder.heartbeat_request(
          [
            group_id: "group",
            member_id: "member",
            generation_id: 1,
            api_version: 3
          ],
          state
        )

      assert error_value == :api_version_no_supported
    end

    test "uses correct default api version when not specified" do
      state = %KafkaEx.New.Client.State{api_versions: %{12 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.heartbeat_request(
          [
            group_id: "group",
            member_id: "member",
            generation_id: 1
          ],
          state
        )

      # Should use v1 as default (changed from v0 for better throttle visibility)
      assert match?(%Kayrock.Heartbeat.V1.Request{}, request)
    end

    test "can explicitly request v0 when needed" do
      state = %KafkaEx.New.Client.State{api_versions: %{12 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.heartbeat_request(
          [
            group_id: "group",
            member_id: "member",
            generation_id: 1,
            api_version: 0
          ],
          state
        )

      assert match?(%Kayrock.Heartbeat.V0.Request{}, request)
    end
  end

  describe "leave_group_request/2" do
    test "returns request for LeaveGroup API v1 (default)" do
      state = %KafkaEx.New.Client.State{api_versions: %{13 => {0, 2}}}
      group_id = "test-group"
      member_id = "consumer-123"

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [group_id: group_id, member_id: member_id],
          state
        )

      expected_request = %Kayrock.LeaveGroup.V1.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "test-group",
        member_id: "consumer-123"
      }

      assert expected_request == request
    end

    test "returns request for LeaveGroup API v0 when explicitly requested" do
      state = %KafkaEx.New.Client.State{api_versions: %{13 => {0, 2}}}
      group_id = "test-group"
      member_id = "consumer-123"

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [group_id: group_id, member_id: member_id, api_version: 0],
          state
        )

      expected_request = %Kayrock.LeaveGroup.V0.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "test-group",
        member_id: "consumer-123"
      }

      assert expected_request == request
    end

    test "returns request for LeaveGroup API v1" do
      state = %KafkaEx.New.Client.State{api_versions: %{13 => {0, 2}}}
      group_id = "consumer-group"
      member_id = "member-abc"

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [
            group_id: group_id,
            member_id: member_id,
            api_version: 1
          ],
          state
        )

      expected_request = %Kayrock.LeaveGroup.V1.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "consumer-group",
        member_id: "member-abc"
      }

      assert expected_request == request
    end

    test "handles empty member_id" do
      state = %KafkaEx.New.Client.State{api_versions: %{13 => {0, 2}}}

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [
            group_id: "group",
            member_id: ""
          ],
          state
        )

      assert request.member_id == ""
    end

    test "handles unicode characters in group_id and member_id" do
      state = %KafkaEx.New.Client.State{api_versions: %{13 => {0, 2}}}

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [
            group_id: "group-日本",
            member_id: "member-한국"
          ],
          state
        )

      assert request.group_id == "group-日本"
      assert request.member_id == "member-한국"
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.New.Client.State{api_versions: %{13 => {0, 1}}}

      {:error, error_value} =
        RequestBuilder.leave_group_request(
          [
            group_id: "group",
            member_id: "member",
            api_version: 3
          ],
          state
        )

      assert error_value == :api_version_no_supported
    end

    test "uses correct default api version when not specified" do
      state = %KafkaEx.New.Client.State{api_versions: %{13 => {0, 2}}}

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [
            group_id: "group",
            member_id: "member"
          ],
          state
        )

      # Should use v1 as default (changed from v0 for better throttle visibility)
      assert match?(%Kayrock.LeaveGroup.V1.Request{}, request)
    end

    test "can explicitly request v0 when needed" do
      state = %KafkaEx.New.Client.State{api_versions: %{13 => {0, 2}}}

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [
            group_id: "group",
            member_id: "member",
            api_version: 0
          ],
          state
        )

      assert match?(%Kayrock.LeaveGroup.V0.Request{}, request)
    end
  end

  describe "join_group_request/2" do
    test "returns request for JoinGroup API v1 (default)" do
      state = %KafkaEx.New.Client.State{api_versions: %{11 => {0, 2}}}
      group_id = "test-group"
      member_id = ""
      session_timeout = 30_000
      rebalance_timeout = 60_000

      group_protocols = [
        %{protocol_name: "assign", protocol_metadata: <<0, 1, 2>>}
      ]

      {:ok, request} =
        RequestBuilder.join_group_request(
          [
            group_id: group_id,
            member_id: member_id,
            session_timeout: session_timeout,
            rebalance_timeout: rebalance_timeout,
            group_protocols: group_protocols
          ],
          state
        )

      expected_request = %Kayrock.JoinGroup.V1.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "test-group",
        member_id: "",
        session_timeout: 30_000,
        rebalance_timeout: 60_000,
        protocol_type: "consumer",
        group_protocols: group_protocols
      }

      assert expected_request == request
    end

    test "returns request for JoinGroup API v0 when explicitly requested" do
      state = %KafkaEx.New.Client.State{api_versions: %{11 => {0, 2}}}
      group_id = "legacy-group"
      member_id = "member-123"
      session_timeout = 10_000

      group_protocols = [
        %{protocol_name: "roundrobin", protocol_metadata: <<1, 2, 3>>}
      ]

      {:ok, request} =
        RequestBuilder.join_group_request(
          [
            group_id: group_id,
            member_id: member_id,
            session_timeout: session_timeout,
            group_protocols: group_protocols,
            api_version: 0
          ],
          state
        )

      expected_request = %Kayrock.JoinGroup.V0.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "legacy-group",
        member_id: "member-123",
        session_timeout: 10_000,
        protocol_type: "consumer",
        group_protocols: group_protocols
      }

      assert expected_request == request
      # V0 doesn't have rebalance_timeout
      refute Map.has_key?(request, :rebalance_timeout)
    end

    test "returns request for JoinGroup API v2" do
      state = %KafkaEx.New.Client.State{api_versions: %{11 => {0, 2}}}

      group_protocols = [
        %{protocol_name: "assign", protocol_metadata: <<>>}
      ]

      {:ok, request} =
        RequestBuilder.join_group_request(
          [
            group_id: "my-group",
            member_id: "member-456",
            session_timeout: 45_000,
            rebalance_timeout: 90_000,
            group_protocols: group_protocols,
            api_version: 2
          ],
          state
        )

      expected_request = %Kayrock.JoinGroup.V2.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "my-group",
        member_id: "member-456",
        session_timeout: 45_000,
        rebalance_timeout: 90_000,
        protocol_type: "consumer",
        group_protocols: group_protocols
      }

      assert expected_request == request
    end

    test "uses custom protocol_type when provided" do
      state = %KafkaEx.New.Client.State{api_versions: %{11 => {0, 2}}}

      {:ok, request} =
        RequestBuilder.join_group_request(
          [
            group_id: "group",
            member_id: "",
            session_timeout: 30_000,
            rebalance_timeout: 60_000,
            protocol_type: "custom",
            group_protocols: []
          ],
          state
        )

      assert request.protocol_type == "custom"
    end

    test "defaults to consumer protocol_type" do
      state = %KafkaEx.New.Client.State{api_versions: %{11 => {0, 2}}}

      {:ok, request} =
        RequestBuilder.join_group_request(
          [
            group_id: "group",
            member_id: "",
            session_timeout: 30_000,
            rebalance_timeout: 60_000,
            group_protocols: []
          ],
          state
        )

      assert request.protocol_type == "consumer"
    end

    test "handles multiple group protocols" do
      state = %KafkaEx.New.Client.State{api_versions: %{11 => {0, 2}}}

      group_protocols = [
        %{protocol_name: "roundrobin", protocol_metadata: <<1>>},
        %{protocol_name: "range", protocol_metadata: <<2>>},
        %{protocol_name: "sticky", protocol_metadata: <<3>>}
      ]

      {:ok, request} =
        RequestBuilder.join_group_request(
          [
            group_id: "group",
            member_id: "",
            session_timeout: 30_000,
            rebalance_timeout: 60_000,
            group_protocols: group_protocols
          ],
          state
        )

      assert length(request.group_protocols) == 3
      assert request.group_protocols == group_protocols
    end

    test "returns error when requested API version not supported" do
      state = %KafkaEx.New.Client.State{api_versions: %{11 => {0, 1}}}

      result =
        RequestBuilder.join_group_request(
          [
            group_id: "group",
            member_id: "",
            session_timeout: 30_000,
            rebalance_timeout: 60_000,
            group_protocols: [],
            api_version: 5
          ],
          state
        )

      assert {:error, :api_version_no_supported} == result
    end
  end

  describe "sync_group_request/2" do
    test "returns request for SyncGroup API v1 (default)" do
      state = %KafkaEx.New.Client.State{api_versions: %{14 => {0, 1}}}
      group_id = "test-group"
      member_id = "consumer-123"
      generation_id = 5

      {:ok, request} =
        RequestBuilder.sync_group_request(
          [
            group_id: group_id,
            member_id: member_id,
            generation_id: generation_id
          ],
          state
        )

      expected_request = %Kayrock.SyncGroup.V1.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "test-group",
        member_id: "consumer-123",
        generation_id: 5,
        group_assignment: []
      }

      assert expected_request == request
    end

    test "returns request for SyncGroup API v0 when explicitly requested" do
      state = %KafkaEx.New.Client.State{api_versions: %{14 => {0, 1}}}
      group_id = "legacy-group"
      member_id = "member-abc"
      generation_id = 10

      {:ok, request} =
        RequestBuilder.sync_group_request(
          [
            group_id: group_id,
            member_id: member_id,
            generation_id: generation_id,
            api_version: 0
          ],
          state
        )

      expected_request = %Kayrock.SyncGroup.V0.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "legacy-group",
        member_id: "member-abc",
        generation_id: 10,
        group_assignment: []
      }

      assert expected_request == request
    end

    test "returns request with group_assignment for leader" do
      state = %KafkaEx.New.Client.State{api_versions: %{14 => {0, 1}}}

      assignments = [
        %{member_id: "member-1", member_assignment: <<1, 2, 3>>},
        %{member_id: "member-2", member_assignment: <<4, 5, 6>>}
      ]

      {:ok, request} =
        RequestBuilder.sync_group_request(
          [
            group_id: "consumer-group",
            member_id: "leader-member",
            generation_id: 3,
            group_assignment: assignments
          ],
          state
        )

      assert request.group_assignment == assignments
    end

    test "handles generation_id 0" do
      state = %KafkaEx.New.Client.State{api_versions: %{14 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.sync_group_request(
          [
            group_id: "group",
            member_id: "member",
            generation_id: 0
          ],
          state
        )

      assert request.generation_id == 0
    end

    test "handles empty member_id" do
      state = %KafkaEx.New.Client.State{api_versions: %{14 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.sync_group_request(
          [
            group_id: "group",
            member_id: "",
            generation_id: 1
          ],
          state
        )

      assert request.member_id == ""
    end

    test "handles unicode characters in group_id and member_id" do
      state = %KafkaEx.New.Client.State{api_versions: %{14 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.sync_group_request(
          [
            group_id: "group-🚀",
            member_id: "member-café",
            generation_id: 1
          ],
          state
        )

      assert request.group_id == "group-🚀"
      assert request.member_id == "member-café"
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.New.Client.State{api_versions: %{14 => {0, 1}}}

      {:error, error_value} =
        RequestBuilder.sync_group_request(
          [
            group_id: "group",
            member_id: "member",
            generation_id: 1,
            api_version: 3
          ],
          state
        )

      assert error_value == :api_version_no_supported
    end

    test "uses correct default api version when not specified" do
      state = %KafkaEx.New.Client.State{api_versions: %{14 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.sync_group_request(
          [
            group_id: "group",
            member_id: "member",
            generation_id: 1
          ],
          state
        )

      # Should use v1 as default (for throttle visibility)
      assert match?(%Kayrock.SyncGroup.V1.Request{}, request)
    end
  end
end
