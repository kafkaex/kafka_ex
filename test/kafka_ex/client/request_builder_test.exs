defmodule KafkaEx.Client.RequestBuilderTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  describe "api_versions_request/2" do
    test "returns request for ApiVersions API v0" do
      state = %KafkaEx.Client.State{api_versions: %{18 => {0, 1}}}

      {:ok, request} = RequestBuilder.api_versions_request([api_version: 0], state)

      assert Fixtures.request_type?(request, :api_versions, 0)
    end

    test "returns request for ApiVersions API v1" do
      state = %KafkaEx.Client.State{api_versions: %{18 => {0, 1}}}

      {:ok, request} = RequestBuilder.api_versions_request([api_version: 1], state)

      assert Fixtures.request_type?(request, :api_versions, 1)
    end

    test "uses default version when not specified" do
      state = %KafkaEx.Client.State{api_versions: %{18 => {0, 1}}}

      {:ok, request} = RequestBuilder.api_versions_request([], state)

      # Default is v0 per @default_api_version
      assert Fixtures.request_type?(request, :api_versions, 0)
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{18 => {0, 0}}}

      {:error, error_value} = RequestBuilder.api_versions_request([api_version: 2], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "metadata_request/2" do
    test "returns request for Metadata API with specific topics" do
      state = %KafkaEx.Client.State{api_versions: %{3 => {0, 2}}}
      topics = ["topic1", "topic2"]

      {:ok, request} = RequestBuilder.metadata_request([topics: topics], state)

      assert Fixtures.request_type?(request, :metadata, 1)
      assert request.topics == topics
    end

    test "returns request for Metadata API v0" do
      state = %KafkaEx.Client.State{api_versions: %{3 => {0, 2}}}
      topics = ["test-topic"]

      {:ok, request} = RequestBuilder.metadata_request([topics: topics, api_version: 0], state)

      assert Fixtures.request_type?(request, :metadata, 0)
      assert request.topics == topics
    end

    test "returns request for Metadata API v2" do
      state = %KafkaEx.Client.State{api_versions: %{3 => {0, 2}}}
      topics = ["topic-a", "topic-b"]

      {:ok, request} = RequestBuilder.metadata_request([topics: topics, api_version: 2], state)

      assert Fixtures.request_type?(request, :metadata, 2)
      assert request.topics == topics
    end

    test "returns request for all topics when topics is nil" do
      state = %KafkaEx.Client.State{api_versions: %{3 => {0, 2}}}

      {:ok, request} = RequestBuilder.metadata_request([topics: nil], state)

      # V1+ uses nil for all topics
      assert request.topics == nil
    end

    test "returns request for all topics when topics not provided" do
      state = %KafkaEx.Client.State{api_versions: %{3 => {0, 2}}}

      {:ok, request} = RequestBuilder.metadata_request([], state)

      assert request.topics == nil
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{3 => {0, 1}}}

      {:error, error_value} = RequestBuilder.metadata_request([topics: ["topic"], api_version: 5], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "find_coordinator_request/2" do
    test "returns request for FindCoordinator API v0" do
      state = %KafkaEx.Client.State{api_versions: %{10 => {0, 1}}}
      group_id = "test-group"

      {:ok, request} = RequestBuilder.find_coordinator_request([group_id: group_id, api_version: 0], state)

      assert Fixtures.request_type?(request, :find_coordinator, 0)
      assert request.key == group_id
    end

    test "returns request for FindCoordinator API v1 (default)" do
      state = %KafkaEx.Client.State{api_versions: %{10 => {0, 1}}}
      group_id = "consumer-group"

      {:ok, request} = RequestBuilder.find_coordinator_request([group_id: group_id], state)

      assert Fixtures.request_type?(request, :find_coordinator, 1)
      assert request.key == group_id
      assert request.key_type == 0
    end

    test "returns request for FindCoordinator API v1 with transaction coordinator" do
      state = %KafkaEx.Client.State{api_versions: %{10 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.find_coordinator_request(
          [coordinator_key: "my-transactional-id", coordinator_type: 1],
          state
        )

      assert Fixtures.request_type?(request, :find_coordinator, 1)
      assert request.key == "my-transactional-id"
      assert request.key_type == 1
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{10 => {0, 1}}}

      {:error, error_value} =
        RequestBuilder.find_coordinator_request([group_id: "group", api_version: 3], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "create_topics_request/2" do
    test "returns request for CreateTopics API v0" do
      state = %KafkaEx.Client.State{api_versions: %{19 => {0, 2}}}

      topics = [
        %{topic: "new-topic", num_partitions: 3, replication_factor: 1}
      ]

      {:ok, request} =
        RequestBuilder.create_topics_request(
          [topics: topics, timeout: 30_000, api_version: 0],
          state
        )

      assert Fixtures.request_type?(request, :create_topics, 0)
      assert request.timeout_ms == 30_000
      assert length(request.topics) == 1
    end

    test "returns request for CreateTopics API v1 (default)" do
      state = %KafkaEx.Client.State{api_versions: %{19 => {0, 2}}}

      topics = [
        %{topic: "topic-a", num_partitions: 1, replication_factor: 1},
        %{topic: "topic-b", num_partitions: 2, replication_factor: 2}
      ]

      {:ok, request} =
        RequestBuilder.create_topics_request(
          [topics: topics, timeout: 15_000],
          state
        )

      assert Fixtures.request_type?(request, :create_topics, 1)
      assert request.timeout_ms == 15_000
      assert length(request.topics) == 2
    end

    test "returns request for CreateTopics API v1 with validate_only" do
      state = %KafkaEx.Client.State{api_versions: %{19 => {0, 2}}}

      topics = [%{topic: "validate-topic", num_partitions: 1, replication_factor: 1}]

      {:ok, request} =
        RequestBuilder.create_topics_request(
          [topics: topics, timeout: 10_000, validate_only: true],
          state
        )

      assert Fixtures.request_type?(request, :create_topics, 1)
      assert request.validate_only == true
    end

    test "returns request with topic config entries" do
      state = %KafkaEx.Client.State{api_versions: %{19 => {0, 2}}}

      topics = [
        %{
          topic: "configured-topic",
          num_partitions: 1,
          replication_factor: 1,
          config_entries: [
            %{config_name: "cleanup.policy", config_value: "compact"},
            %{config_name: "retention.ms", config_value: "86400000"}
          ]
        }
      ]

      {:ok, request} =
        RequestBuilder.create_topics_request(
          [topics: topics, timeout: 30_000],
          state
        )

      [topic_config] = request.topics
      assert length(topic_config.configs) == 2
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{19 => {0, 1}}}

      topics = [%{topic: "topic", num_partitions: 1, replication_factor: 1}]

      {:error, error_value} =
        RequestBuilder.create_topics_request(
          [topics: topics, timeout: 30_000, api_version: 5],
          state
        )

      assert error_value == :api_version_no_supported
    end
  end

  describe "delete_topics_request/2" do
    test "returns request for DeleteTopics API v0" do
      state = %KafkaEx.Client.State{api_versions: %{20 => {0, 1}}}
      topics = ["topic-to-delete"]

      {:ok, request} =
        RequestBuilder.delete_topics_request(
          [topics: topics, timeout: 30_000, api_version: 0],
          state
        )

      assert Fixtures.request_type?(request, :delete_topics, 0)
      assert request.topic_names == topics
      assert request.timeout_ms == 30_000
    end

    test "returns request for DeleteTopics API v1 (default)" do
      state = %KafkaEx.Client.State{api_versions: %{20 => {0, 1}}}
      topics = ["delete-me-1", "delete-me-2"]

      {:ok, request} =
        RequestBuilder.delete_topics_request(
          [topics: topics, timeout: 15_000],
          state
        )

      assert Fixtures.request_type?(request, :delete_topics, 1)
      assert request.topic_names == topics
      assert request.timeout_ms == 15_000
    end

    test "handles single topic deletion" do
      state = %KafkaEx.Client.State{api_versions: %{20 => {0, 1}}}

      {:ok, request} =
        RequestBuilder.delete_topics_request(
          [topics: ["single-topic"], timeout: 30_000],
          state
        )

      assert request.topic_names == ["single-topic"]
    end

    test "handles multiple topics deletion" do
      state = %KafkaEx.Client.State{api_versions: %{20 => {0, 1}}}
      topics = ["topic-1", "topic-2", "topic-3", "topic-4"]

      {:ok, request} =
        RequestBuilder.delete_topics_request(
          [topics: topics, timeout: 60_000],
          state
        )

      assert length(request.topic_names) == 4
      assert request.timeout_ms == 60_000
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{20 => {0, 0}}}

      {:error, error_value} =
        RequestBuilder.delete_topics_request(
          [topics: ["topic"], timeout: 30_000, api_version: 3],
          state
        )

      assert error_value == :api_version_no_supported
    end
  end

  describe "describe_groups_request/2" do
    test "returns request for DescribeGroups API" do
      state = %KafkaEx.Client.State{api_versions: %{15 => {0, 1}}}
      group_names = ["group1", "group2"]

      expected_request = Fixtures.build_request(:describe_groups, 1, groups: group_names)

      {:ok, request} = RequestBuilder.describe_groups_request([group_names: group_names], state)

      assert expected_request == request
    end

    test "returns request with custom API version" do
      state = %KafkaEx.Client.State{api_versions: %{15 => {0, 1}}}
      group_names = ["group1", "group2"]

      expected_request = Fixtures.build_request(:describe_groups, 0, groups: group_names)

      {:ok, request} = RequestBuilder.describe_groups_request([group_names: group_names, api_version: 0], state)

      assert expected_request == request
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{15 => {0, 1}}}
      group_names = ["group1", "group2"]

      {:error, error_value} = RequestBuilder.describe_groups_request([group_names: group_names, api_version: 3], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "lists_offset_request/2" do
    test "returns request for ListOffsets API" do
      state = %KafkaEx.Client.State{api_versions: %{2 => {0, 2}}}
      topic_data = [{"test-topic", [%{partition_num: 1, timestamp: :latest}]}]

      {:ok, request} = RequestBuilder.lists_offset_request([topics: topic_data], state)

      expected_request =
        Fixtures.build_request(:list_offsets, 1,
          replica_id: -1,
          topics: [%{partitions: [%{timestamp: -1, partition: 1}], topic: "test-topic"}]
        )

      assert expected_request == request
    end

    test "returns request with custom API version" do
      state = %KafkaEx.Client.State{api_versions: %{2 => {0, 2}}}
      topic_data = [{"test-topic", [%{partition_num: 1, timestamp: :latest}]}]

      {:ok, request} = RequestBuilder.lists_offset_request([topics: topic_data, api_version: 2], state)

      expected_request =
        Fixtures.build_request(:list_offsets, 2,
          replica_id: -1,
          isolation_level: 0,
          topics: [%{partitions: [%{timestamp: -1, partition: 1}], topic: "test-topic"}]
        )

      assert expected_request == request
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{2 => {0, 2}}}
      topic_data = [{"test-topic", [%{partition_num: 1, timestamp: :latest}]}]

      {:error, error_value} = RequestBuilder.lists_offset_request([topics: topic_data, api_version: 3], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "offset_fetch_request/2" do
    test "returns request for OffsetFetch API with default version" do
      state = %KafkaEx.Client.State{api_versions: %{9 => {0, 3}}}
      group_id = "test-group"
      topics = [{"test-topic", [%{partition_num: 0}]}]

      {:ok, request} = RequestBuilder.offset_fetch_request([group_id: group_id, topics: topics], state)

      expected_request =
        Fixtures.build_request(:offset_fetch, 1,
          client_id: nil,
          correlation_id: nil,
          group_id: "test-group",
          topics: [%{name: "test-topic", partition_indexes: [0]}]
        )

      assert expected_request == request
    end

    test "returns request with custom API version" do
      state = %KafkaEx.Client.State{api_versions: %{9 => {0, 3}}}
      group_id = "consumer-group"
      topics = [{"my-topic", [%{partition_num: 1}, %{partition_num: 2}]}]

      {:ok, request} = RequestBuilder.offset_fetch_request([group_id: group_id, topics: topics, api_version: 2], state)

      expected_request =
        Fixtures.build_request(:offset_fetch, 2,
          client_id: nil,
          correlation_id: nil,
          group_id: "consumer-group",
          topics: [%{name: "my-topic", partition_indexes: [1, 2]}]
        )

      assert expected_request == request
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{9 => {0, 2}}}
      group_id = "test-group"
      topics = [{"test-topic", [%{partition_num: 0}]}]

      {:error, error_value} =
        RequestBuilder.offset_fetch_request([group_id: group_id, topics: topics, api_version: 5], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "offset_commit_request/2" do
    test "returns request for OffsetCommit API v1 (default)" do
      state = %KafkaEx.Client.State{api_versions: %{8 => {0, 3}}}
      group_id = "test-group"
      topics = [{"test-topic", [%{partition_num: 0, offset: 100}]}]

      {:ok, request} = RequestBuilder.offset_commit_request([group_id: group_id, topics: topics], state)

      expected_request =
        Fixtures.build_request(:offset_commit, 1,
          client_id: nil,
          correlation_id: nil,
          group_id: "test-group",
          generation_id: -1,
          member_id: "",
          topics: [
            %{
              name: "test-topic",
              partitions: [%{partition_index: 0, committed_offset: 100, commit_timestamp: -1, committed_metadata: ""}]
            }
          ]
        )

      assert expected_request == request
    end

    test "returns request with v1 (generation_id and member_id)" do
      state = %KafkaEx.Client.State{api_versions: %{8 => {0, 3}}}
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

      expected_request =
        Fixtures.build_request(:offset_commit, 1,
          client_id: nil,
          correlation_id: nil,
          group_id: "consumer-group",
          generation_id: 5,
          member_id: "member-123",
          topics: [
            %{
              name: "my-topic",
              partitions: [%{partition_index: 1, committed_offset: 200, commit_timestamp: -1, committed_metadata: ""}]
            }
          ]
        )

      assert expected_request == request
    end

    test "returns request with v2 and custom retention_time" do
      state = %KafkaEx.Client.State{api_versions: %{8 => {0, 3}}}
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

      expected_request =
        Fixtures.build_request(:offset_commit, 2,
          client_id: nil,
          correlation_id: nil,
          group_id: "retention-group",
          generation_id: 10,
          member_id: "member-abc",
          retention_time_ms: 86_400_000,
          topics: [
            %{name: "topic-a", partitions: [%{partition_index: 0, committed_offset: 300, committed_metadata: ""}]}
          ]
        )

      assert expected_request == request
    end

    test "returns request with v0 (no generation_id or member_id)" do
      state = %KafkaEx.Client.State{api_versions: %{8 => {0, 3}}}
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

      expected_request =
        Fixtures.build_request(:offset_commit, 0,
          client_id: nil,
          correlation_id: nil,
          group_id: "legacy-group",
          topics: [
            %{name: "legacy-topic", partitions: [%{partition_index: 0, committed_offset: 50, committed_metadata: ""}]}
          ]
        )

      assert expected_request == request
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{8 => {0, 2}}}
      group_id = "test-group"
      topics = [{"test-topic", [%{partition_num: 0, offset: 100}]}]

      {:error, error_value} =
        RequestBuilder.offset_commit_request([group_id: group_id, topics: topics, api_version: 5], state)

      assert error_value == :api_version_no_supported
    end
  end

  describe "heartbeat_request/2" do
    test "returns request for Heartbeat API v1 (default)" do
      state = %KafkaEx.Client.State{api_versions: %{12 => {0, 1}}}
      group_id = "test-group"
      member_id = "consumer-123"
      generation_id = 5

      {:ok, request} =
        RequestBuilder.heartbeat_request(
          [group_id: group_id, member_id: member_id, generation_id: generation_id],
          state
        )

      expected_request =
        Fixtures.build_request(:heartbeat, 1,
          client_id: nil,
          correlation_id: nil,
          group_id: "test-group",
          member_id: "consumer-123",
          generation_id: 5
        )

      assert expected_request == request
    end

    test "returns request for Heartbeat API v0 when explicitly requested" do
      state = %KafkaEx.Client.State{api_versions: %{12 => {0, 1}}}
      group_id = "test-group"
      member_id = "consumer-123"
      generation_id = 5

      {:ok, request} =
        RequestBuilder.heartbeat_request(
          [group_id: group_id, member_id: member_id, generation_id: generation_id, api_version: 0],
          state
        )

      expected_request =
        Fixtures.build_request(:heartbeat, 0,
          client_id: nil,
          correlation_id: nil,
          group_id: "test-group",
          member_id: "consumer-123",
          generation_id: 5
        )

      assert expected_request == request
    end

    test "returns request for Heartbeat API v1" do
      state = %KafkaEx.Client.State{api_versions: %{12 => {0, 1}}}
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

      expected_request =
        Fixtures.build_request(:heartbeat, 1,
          client_id: nil,
          correlation_id: nil,
          group_id: "consumer-group",
          member_id: "member-abc",
          generation_id: 10
        )

      assert expected_request == request
    end

    test "handles generation_id 0" do
      state = %KafkaEx.Client.State{api_versions: %{12 => {0, 1}}}

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
      state = %KafkaEx.Client.State{api_versions: %{12 => {0, 1}}}

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
      state = %KafkaEx.Client.State{api_versions: %{12 => {0, 1}}}

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
      state = %KafkaEx.Client.State{api_versions: %{12 => {0, 1}}}

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
      assert Fixtures.request_type?(request, :heartbeat, 1)
    end

    test "can explicitly request v0 when needed" do
      state = %KafkaEx.Client.State{api_versions: %{12 => {0, 1}}}

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

      assert Fixtures.request_type?(request, :heartbeat, 0)
    end
  end

  describe "leave_group_request/2" do
    test "returns request for LeaveGroup API v1 (default)" do
      state = %KafkaEx.Client.State{api_versions: %{13 => {0, 2}}}
      group_id = "test-group"
      member_id = "consumer-123"

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [group_id: group_id, member_id: member_id],
          state
        )

      expected_request =
        Fixtures.build_request(:leave_group, 1,
          client_id: nil,
          correlation_id: nil,
          group_id: "test-group",
          member_id: "consumer-123"
        )

      assert expected_request == request
    end

    test "returns request for LeaveGroup API v0 when explicitly requested" do
      state = %KafkaEx.Client.State{api_versions: %{13 => {0, 2}}}
      group_id = "test-group"
      member_id = "consumer-123"

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [group_id: group_id, member_id: member_id, api_version: 0],
          state
        )

      expected_request =
        Fixtures.build_request(:leave_group, 0,
          client_id: nil,
          correlation_id: nil,
          group_id: "test-group",
          member_id: "consumer-123"
        )

      assert expected_request == request
    end

    test "returns request for LeaveGroup API v1" do
      state = %KafkaEx.Client.State{api_versions: %{13 => {0, 2}}}
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

      expected_request =
        Fixtures.build_request(:leave_group, 1,
          client_id: nil,
          correlation_id: nil,
          group_id: "consumer-group",
          member_id: "member-abc"
        )

      assert expected_request == request
    end

    test "handles empty member_id" do
      state = %KafkaEx.Client.State{api_versions: %{13 => {0, 2}}}

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
      state = %KafkaEx.Client.State{api_versions: %{13 => {0, 2}}}

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
      state = %KafkaEx.Client.State{api_versions: %{13 => {0, 1}}}

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
      state = %KafkaEx.Client.State{api_versions: %{13 => {0, 2}}}

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [
            group_id: "group",
            member_id: "member"
          ],
          state
        )

      # Should use v1 as default (changed from v0 for better throttle visibility)
      assert Fixtures.request_type?(request, :leave_group, 1)
    end

    test "can explicitly request v0 when needed" do
      state = %KafkaEx.Client.State{api_versions: %{13 => {0, 2}}}

      {:ok, request} =
        RequestBuilder.leave_group_request(
          [
            group_id: "group",
            member_id: "member",
            api_version: 0
          ],
          state
        )

      assert Fixtures.request_type?(request, :leave_group, 0)
    end
  end

  describe "join_group_request/2" do
    test "returns request for JoinGroup API v1 (default)" do
      state = %KafkaEx.Client.State{api_versions: %{11 => {0, 2}}}
      group_id = "test-group"
      member_id = ""
      session_timeout = 30_000
      rebalance_timeout = 60_000

      group_protocols = [
        %{name: "assign", metadata: <<0, 1, 2>>}
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

      expected_request =
        Fixtures.build_request(:join_group, 1,
          client_id: nil,
          correlation_id: nil,
          group_id: "test-group",
          member_id: "",
          session_timeout_ms: 30_000,
          rebalance_timeout_ms: 60_000,
          protocol_type: "consumer",
          protocols: group_protocols
        )

      assert expected_request == request
    end

    test "returns request for JoinGroup API v0 when explicitly requested" do
      state = %KafkaEx.Client.State{api_versions: %{11 => {0, 2}}}
      group_id = "legacy-group"
      member_id = "member-123"
      session_timeout = 10_000

      group_protocols = [
        %{name: "roundrobin", metadata: <<1, 2, 3>>}
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

      expected_request =
        Fixtures.build_request(:join_group, 0,
          client_id: nil,
          correlation_id: nil,
          group_id: "legacy-group",
          member_id: "member-123",
          session_timeout_ms: 10_000,
          protocol_type: "consumer",
          protocols: group_protocols
        )

      assert expected_request == request
      # V0 doesn't have rebalance_timeout_ms
      refute Map.has_key?(request, :rebalance_timeout_ms)
    end

    test "returns request for JoinGroup API v2" do
      state = %KafkaEx.Client.State{api_versions: %{11 => {0, 2}}}

      group_protocols = [
        %{name: "assign", metadata: <<>>}
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

      expected_request =
        Fixtures.build_request(:join_group, 2,
          client_id: nil,
          correlation_id: nil,
          group_id: "my-group",
          member_id: "member-456",
          session_timeout_ms: 45_000,
          rebalance_timeout_ms: 90_000,
          protocol_type: "consumer",
          protocols: group_protocols
        )

      assert expected_request == request
    end

    test "uses custom protocol_type when provided" do
      state = %KafkaEx.Client.State{api_versions: %{11 => {0, 2}}}

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
      state = %KafkaEx.Client.State{api_versions: %{11 => {0, 2}}}

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
      state = %KafkaEx.Client.State{api_versions: %{11 => {0, 2}}}

      group_protocols = [
        %{name: "roundrobin", metadata: <<1>>},
        %{name: "range", metadata: <<2>>},
        %{name: "sticky", metadata: <<3>>}
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

      assert length(request.protocols) == 3
      assert request.protocols == group_protocols
    end

    test "returns error when requested API version not supported" do
      state = %KafkaEx.Client.State{api_versions: %{11 => {0, 1}}}

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
      state = %KafkaEx.Client.State{api_versions: %{14 => {0, 1}}}
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

      expected_request =
        Fixtures.build_request(:sync_group, 1,
          client_id: nil,
          correlation_id: nil,
          group_id: "test-group",
          member_id: "consumer-123",
          generation_id: 5,
          assignments: []
        )

      assert expected_request == request
    end

    test "returns request for SyncGroup API v0 when explicitly requested" do
      state = %KafkaEx.Client.State{api_versions: %{14 => {0, 1}}}
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

      expected_request =
        Fixtures.build_request(:sync_group, 0,
          client_id: nil,
          correlation_id: nil,
          group_id: "legacy-group",
          member_id: "member-abc",
          generation_id: 10,
          assignments: []
        )

      assert expected_request == request
    end

    test "returns request with group_assignment for leader" do
      state = %KafkaEx.Client.State{api_versions: %{14 => {0, 1}}}

      assignments = [
        %{member_id: "member-1", assignment: <<1, 2, 3>>},
        %{member_id: "member-2", assignment: <<4, 5, 6>>}
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

      assert request.assignments == assignments
    end

    test "handles generation_id 0" do
      state = %KafkaEx.Client.State{api_versions: %{14 => {0, 1}}}

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
      state = %KafkaEx.Client.State{api_versions: %{14 => {0, 1}}}

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
      state = %KafkaEx.Client.State{api_versions: %{14 => {0, 1}}}

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
      state = %KafkaEx.Client.State{api_versions: %{14 => {0, 1}}}

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
      state = %KafkaEx.Client.State{api_versions: %{14 => {0, 1}}}

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
      assert Fixtures.request_type?(request, :sync_group, 1)
    end
  end

  describe "produce_request/2" do
    test "returns request for Produce API v3 (default)" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 3}}}
      messages = [%{value: "hello", key: "key1"}]
      {:ok, request} = RequestBuilder.produce_request([topic: "test-topic", partition: 0, messages: messages], state)

      # V3 uses RecordBatch
      assert Fixtures.request_type?(request, :produce, 3)
      assert request.acks == -1
      assert request.timeout == 5000
      assert [%{topic: "test-topic", data: [%{partition: 0}]}] = request.topic_data
    end

    test "returns request for Produce API v0 when explicitly requested" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 3}}}
      messages = [%{value: "hello"}]

      {:ok, request} =
        RequestBuilder.produce_request([topic: "events", partition: 1, messages: messages, api_version: 0], state)

      assert Fixtures.request_type?(request, :produce, 0)
      assert [%{topic: "events", data: [%{partition: 1}]}] = request.topic_data
    end

    test "returns request with custom acks and timeout" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 3}}}
      messages = [%{value: "data"}]

      {:ok, request} =
        RequestBuilder.produce_request(
          [topic: "topic", partition: 0, messages: messages, acks: 1, timeout: 10_000],
          state
        )

      assert request.acks == 1
      assert request.timeout == 10_000
    end

    test "returns request with compression" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 3}}}
      messages = [%{value: "data"}]

      {:ok, request} =
        RequestBuilder.produce_request(
          [topic: "compressed-topic", partition: 0, messages: messages, compression: :gzip],
          state
        )

      assert Fixtures.request_type?(request, :produce, 3)
      [%{data: [%{record_set: record_batch}]}] = request.topic_data
      assert record_batch.attributes == 1
    end

    test "returns request with transactional_id for V3" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 3}}}
      messages = [%{value: "tx-data"}]

      {:ok, request} =
        RequestBuilder.produce_request(
          [topic: "transactions", partition: 0, messages: messages, transactional_id: "my-tx-id"],
          state
        )

      assert request.transactional_id == "my-tx-id"
    end

    test "uses MessageSet for V0-V2" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 3}}}
      messages = [%{value: "hello", key: "k1"}]

      {:ok, request} =
        RequestBuilder.produce_request([topic: "test", partition: 0, messages: messages, api_version: 2], state)

      assert Fixtures.request_type?(request, :produce, 2)
      [%{data: [%{record_set: message_set}]}] = request.topic_data
      assert Fixtures.message_set?(message_set)
    end

    test "uses RecordBatch for V3+" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 3}}}
      messages = [%{value: "hello", key: "k1"}]

      {:ok, request} =
        RequestBuilder.produce_request([topic: "test", partition: 0, messages: messages, api_version: 3], state)

      assert Fixtures.request_type?(request, :produce, 3)
      [%{data: [%{record_set: record_batch}]}] = request.topic_data
      assert Fixtures.record_batch?(record_batch)
    end

    test "returns error when api version is not supported" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 2}}}
      messages = [%{value: "data"}]

      {:error, error_value} =
        RequestBuilder.produce_request([topic: "topic", partition: 0, messages: messages, api_version: 5], state)

      assert error_value == :api_version_no_supported
    end

    test "handles multiple messages" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 3}}}

      messages = [
        %{value: "msg1", key: "k1"},
        %{value: "msg2", key: "k2"},
        %{value: "msg3"}
      ]

      {:ok, request} =
        RequestBuilder.produce_request(
          [
            topic: "batch-topic",
            partition: 0,
            messages: messages
          ],
          state
        )

      [%{data: [%{record_set: record_batch}]}] = request.topic_data
      assert length(record_batch.records) == 3
    end

    test "handles messages with headers (V3+)" do
      state = %KafkaEx.Client.State{api_versions: %{0 => {0, 3}}}

      messages = [
        %{
          value: "event-data",
          key: "event-1",
          headers: [{"content-type", "application/json"}, {"version", "1.0"}]
        }
      ]

      {:ok, request} =
        RequestBuilder.produce_request(
          [
            topic: "events",
            partition: 0,
            messages: messages
          ],
          state
        )

      [%{data: [%{record_set: record_batch}]}] = request.topic_data
      [record] = record_batch.records
      assert length(record.headers) == 2
    end
  end

  describe "fetch_request/2" do
    test "builds a basic fetch request" do
      state = %KafkaEx.Client.State{api_versions: %{1 => {0, 7}}}

      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 100,
        api_version: 3
      ]

      assert {:ok, request} = RequestBuilder.fetch_request(opts, state)

      assert Fixtures.request_type?(request, :fetch, 3)
      assert request.replica_id == -1
      assert [%{topic: "test_topic", partitions: [partition]}] = request.topics
      assert partition.partition == 0
      assert partition.fetch_offset == 100
    end

    test "builds V0 fetch request" do
      state = %KafkaEx.Client.State{api_versions: %{1 => {0, 7}}}

      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 0,
        api_version: 0
      ]

      assert {:ok, request} = RequestBuilder.fetch_request(opts, state)
      assert Fixtures.request_type?(request, :fetch, 0)
    end

    test "builds V4 fetch request with isolation_level" do
      state = %KafkaEx.Client.State{api_versions: %{1 => {0, 7}}}

      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 0,
        api_version: 4,
        isolation_level: 1
      ]

      assert {:ok, request} = RequestBuilder.fetch_request(opts, state)
      assert Fixtures.request_type?(request, :fetch, 4)
      assert request.isolation_level == 1
    end

    test "builds V5 fetch request with log_start_offset" do
      state = %KafkaEx.Client.State{api_versions: %{1 => {0, 7}}}

      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 0,
        api_version: 5,
        log_start_offset: 50
      ]

      assert {:ok, request} = RequestBuilder.fetch_request(opts, state)
      assert Fixtures.request_type?(request, :fetch, 5)

      [%{partitions: [partition]}] = request.topics
      assert partition.log_start_offset == 50
    end

    test "builds V7 fetch request with session fields" do
      state = %KafkaEx.Client.State{api_versions: %{1 => {0, 7}}}

      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 0,
        api_version: 7,
        session_id: 123,
        epoch: 5
      ]

      assert {:ok, request} = RequestBuilder.fetch_request(opts, state)
      assert Fixtures.request_type?(request, :fetch, 7)
      assert request.session_id == 123
      assert request.session_epoch == 5
    end

    test "returns error when api version not supported" do
      state = %KafkaEx.Client.State{api_versions: %{}}

      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 0,
        api_version: 10
      ]

      assert {:error, :api_version_no_supported} = RequestBuilder.fetch_request(opts, state)
    end

    test "uses default options when not specified" do
      state = %KafkaEx.Client.State{api_versions: %{1 => {0, 7}}}

      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 0
      ]

      assert {:ok, request} = RequestBuilder.fetch_request(opts, state)

      # Default is V3
      assert Fixtures.request_type?(request, :fetch, 3)
      assert request.max_wait_time == 10_000
      assert request.min_bytes == 1
    end

    test "allows custom max_bytes, max_wait_time, and min_bytes" do
      state = %KafkaEx.Client.State{api_versions: %{1 => {0, 7}}}

      opts = [
        topic: "test_topic",
        partition: 0,
        offset: 0,
        max_bytes: 500_000,
        max_wait_time: 5_000,
        min_bytes: 100
      ]

      assert {:ok, request} = RequestBuilder.fetch_request(opts, state)

      assert request.max_wait_time == 5_000
      assert request.min_bytes == 100
      [%{partitions: [partition]}] = request.topics
      assert partition.partition_max_bytes == 500_000
    end
  end
end
