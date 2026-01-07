defmodule KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts topics and timeout" do
      opts = [topics: [%{topic: "my-topic", num_partitions: 3, replication_factor: 2}], timeout: 30_000]

      result = RequestHelpers.extract_common_fields(opts)

      assert length(result.topics) == 1
      assert result.timeout == 30_000
    end

    test "raises on missing topics" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(timeout: 30_000)
      end
    end

    test "raises on missing timeout" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(topics: [])
      end
    end
  end

  describe "build_topic_request/1" do
    test "builds topic request from map" do
      topic_config = %{topic: "my-topic", num_partitions: 5, replication_factor: 3}

      result = RequestHelpers.build_topic_request(topic_config)

      assert result.topic == "my-topic"
      assert result.num_partitions == 5
      assert result.replication_factor == 3
      assert result.replica_assignment == []
      assert result.config_entries == []
    end

    test "builds topic request from keyword list" do
      topic_config = [topic: "my-topic", num_partitions: 2]

      result = RequestHelpers.build_topic_request(topic_config)

      assert result.topic == "my-topic"
      assert result.num_partitions == 2
      assert result.replication_factor == -1
    end

    test "uses default values for optional fields" do
      topic_config = %{topic: "my-topic"}

      result = RequestHelpers.build_topic_request(topic_config)

      assert result.num_partitions == -1
      assert result.replication_factor == -1
      assert result.replica_assignment == []
      assert result.config_entries == []
    end

    test "includes replica_assignment when provided" do
      topic_config = %{
        topic: "my-topic",
        replica_assignment: [{0, [1, 2]}, {1, [2, 3]}]
      }

      result = RequestHelpers.build_topic_request(topic_config)

      assert length(result.replica_assignment) == 2
      [ra1, ra2] = result.replica_assignment
      assert ra1.partition == 0
      assert ra1.replicas == [1, 2]
      assert ra2.partition == 1
      assert ra2.replicas == [2, 3]
    end

    test "includes config_entries when provided" do
      topic_config = %{
        topic: "my-topic",
        config_entries: [{"retention.ms", "86400000"}, {"cleanup.policy", "compact"}]
      }

      result = RequestHelpers.build_topic_request(topic_config)

      assert length(result.config_entries) == 2
      [c1, c2] = result.config_entries
      assert c1.config_name == "retention.ms"
      assert c1.config_value == "86400000"
      assert c2.config_name == "cleanup.policy"
      assert c2.config_value == "compact"
    end
  end

  describe "build_replica_assignments/1" do
    test "converts tuple format to map format" do
      assignments = [{0, [1, 2, 3]}, {1, [2, 3, 4]}]

      result = RequestHelpers.build_replica_assignments(assignments)

      assert length(result) == 2
      [a1, a2] = result
      assert a1.partition == 0
      assert a1.replicas == [1, 2, 3]
      assert a2.partition == 1
      assert a2.replicas == [2, 3, 4]
    end

    test "passes through map format unchanged" do
      assignments = [
        %{partition: 0, replicas: [1, 2]},
        %{partition: 1, replicas: [2, 3]}
      ]

      result = RequestHelpers.build_replica_assignments(assignments)

      assert result == assignments
    end

    test "returns empty list for empty input" do
      assert RequestHelpers.build_replica_assignments([]) == []
    end
  end

  describe "build_config_entries/1" do
    test "converts tuple format to map format" do
      entries = [{"retention.ms", "1000"}, {:cleanup_policy, "delete"}]

      result = RequestHelpers.build_config_entries(entries)

      assert length(result) == 2
      [e1, e2] = result
      assert e1.config_name == "retention.ms"
      assert e1.config_value == "1000"
      assert e2.config_name == "cleanup_policy"
      assert e2.config_value == "delete"
    end

    test "passes through map format unchanged" do
      entries = [
        %{config_name: "retention.ms", config_value: "1000"}
      ]

      result = RequestHelpers.build_config_entries(entries)

      assert result == entries
    end

    test "returns empty list for empty input" do
      assert RequestHelpers.build_config_entries([]) == []
    end
  end

  describe "build_v1_v2_request/2" do
    test "builds complete request" do
      template = %{}

      opts = [
        topics: [%{topic: "new-topic", num_partitions: 3, replication_factor: 2}],
        timeout: 30_000,
        validate_only: false
      ]

      result = RequestHelpers.build_v1_v2_request(template, opts)

      assert result.timeout == 30_000
      assert result.validate_only == false
      assert length(result.create_topic_requests) == 1
      [topic_req] = result.create_topic_requests
      assert topic_req.topic == "new-topic"
    end

    test "sets validate_only to true when specified" do
      template = %{}

      opts = [
        topics: [%{topic: "test-topic"}],
        timeout: 5_000,
        validate_only: true
      ]

      result = RequestHelpers.build_v1_v2_request(template, opts)

      assert result.validate_only == true
    end

    test "defaults validate_only to false" do
      template = %{}

      opts = [
        topics: [%{topic: "test-topic"}],
        timeout: 5_000
      ]

      result = RequestHelpers.build_v1_v2_request(template, opts)

      assert result.validate_only == false
    end
  end
end
