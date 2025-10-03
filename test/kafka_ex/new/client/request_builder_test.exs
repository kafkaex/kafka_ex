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
    test "returns request for OffsetCommit API v2 (default)" do
      state = %KafkaEx.New.Client.State{api_versions: %{8 => {0, 3}}}
      group_id = "test-group"
      topics = [{"test-topic", [%{partition_num: 0, offset: 100}]}]

      {:ok, request} = RequestBuilder.offset_commit_request([group_id: group_id, topics: topics], state)

      expected_request = %Kayrock.OffsetCommit.V2.Request{
        client_id: nil,
        correlation_id: nil,
        group_id: "test-group",
        generation_id: -1,
        member_id: "",
        retention_time: -1,
        topics: [%{topic: "test-topic", partitions: [%{partition: 0, offset: 100, metadata: ""}]}]
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
end
