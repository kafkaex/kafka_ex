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
end
