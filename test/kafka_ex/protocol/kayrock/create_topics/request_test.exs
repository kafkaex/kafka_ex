defmodule KafkaEx.Protocol.Kayrock.CreateTopics.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.CreateTopics
  alias KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts required fields" do
      opts = [
        topics: [[topic: "test-topic", num_partitions: 3]],
        timeout: 10_000
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.topics == [[topic: "test-topic", num_partitions: 3]]
      assert result.timeout == 10_000
    end

    test "raises when topics is missing" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(timeout: 10_000)
      end
    end

    test "raises when timeout is missing" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(topics: [[topic: "test"]])
      end
    end
  end

  describe "RequestHelpers.build_topic_request/1 with keyword list" do
    test "builds topic request with all fields" do
      topic_config = [
        topic: "my-topic",
        num_partitions: 3,
        replication_factor: 2,
        replica_assignment: [],
        config_entries: []
      ]

      result = RequestHelpers.build_topic_request(topic_config)

      assert result.name == "my-topic"
      assert result.num_partitions == 3
      assert result.replication_factor == 2
      assert result.assignments == []
      assert result.configs == []
    end

    test "uses defaults for missing fields" do
      topic_config = [topic: "my-topic"]

      result = RequestHelpers.build_topic_request(topic_config)

      assert result.name == "my-topic"
      assert result.num_partitions == -1
      assert result.replication_factor == -1
      assert result.assignments == []
      assert result.configs == []
    end
  end

  describe "RequestHelpers.build_topic_request/1 with map" do
    test "builds topic request from map" do
      topic_config = %{
        topic: "my-topic",
        num_partitions: 6,
        replication_factor: 3
      }

      result = RequestHelpers.build_topic_request(topic_config)

      assert result.name == "my-topic"
      assert result.num_partitions == 6
      assert result.replication_factor == 3
    end
  end

  describe "RequestHelpers.build_replica_assignments/1" do
    test "converts map format to expected format" do
      assignments = [
        %{partition: 0, replicas: [1, 2, 3]},
        %{partition: 1, replicas: [2, 3, 1]}
      ]

      result = RequestHelpers.build_replica_assignments(assignments)

      assert result == [
               %{partition_index: 0, broker_ids: [1, 2, 3]},
               %{partition_index: 1, broker_ids: [2, 3, 1]}
             ]
    end

    test "converts tuple format to expected format" do
      assignments = [
        {0, [1, 2, 3]},
        {1, [2, 3, 1]}
      ]

      result = RequestHelpers.build_replica_assignments(assignments)

      assert result == [
               %{partition_index: 0, broker_ids: [1, 2, 3]},
               %{partition_index: 1, broker_ids: [2, 3, 1]}
             ]
    end

    test "returns empty list for empty input" do
      assert RequestHelpers.build_replica_assignments([]) == []
    end
  end

  describe "RequestHelpers.build_config_entries/1" do
    test "converts map format to expected format" do
      entries = [
        %{config_name: "cleanup.policy", config_value: "compact"},
        %{config_name: "retention.ms", config_value: "86400000"}
      ]

      result = RequestHelpers.build_config_entries(entries)

      assert result == [
               %{name: "cleanup.policy", value: "compact"},
               %{name: "retention.ms", value: "86400000"}
             ]
    end

    test "converts tuple format to expected format" do
      entries = [
        {"cleanup.policy", "compact"},
        {"retention.ms", "86400000"}
      ]

      result = RequestHelpers.build_config_entries(entries)

      assert result == [
               %{name: "cleanup.policy", value: "compact"},
               %{name: "retention.ms", value: "86400000"}
             ]
    end

    test "converts atom keys to strings" do
      entries = [{:cleanup_policy, "compact"}]
      result = RequestHelpers.build_config_entries(entries)

      assert result == [%{name: "cleanup_policy", value: "compact"}]
    end

    test "returns empty list for empty input" do
      assert RequestHelpers.build_config_entries([]) == []
    end
  end

  describe "V0 Request implementation" do
    test "builds request with all fields" do
      request = %Kayrock.CreateTopics.V0.Request{}

      opts = [
        topics: [[topic: "test-topic", num_partitions: 3, replication_factor: 2]],
        timeout: 10_000
      ]

      result = CreateTopics.Request.build_request(request, opts)

      assert %Kayrock.CreateTopics.V0.Request{} = result
      assert result.timeout_ms == 10_000
      assert length(result.topics) == 1

      [topic_req] = result.topics
      assert topic_req.name == "test-topic"
      assert topic_req.num_partitions == 3
      assert topic_req.replication_factor == 2
    end

    test "builds request with multiple topics" do
      request = %Kayrock.CreateTopics.V0.Request{}

      opts = [
        topics: [
          [topic: "topic1", num_partitions: 1],
          [topic: "topic2", num_partitions: 2],
          [topic: "topic3", num_partitions: 3]
        ],
        timeout: 5_000
      ]

      result = CreateTopics.Request.build_request(request, opts)

      assert length(result.topics) == 3
      topics = Enum.map(result.topics, & &1.name)
      assert topics == ["topic1", "topic2", "topic3"]
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.CreateTopics.V0.Request{
        correlation_id: 42,
        client_id: "test-client"
      }

      opts = [
        topics: [[topic: "test-topic"]],
        timeout: 10_000
      ]

      result = CreateTopics.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "test-client"
    end
  end

  describe "V1 Request implementation" do
    test "builds request with validate_only flag" do
      request = %Kayrock.CreateTopics.V1.Request{}

      opts = [
        topics: [[topic: "test-topic", num_partitions: 3]],
        timeout: 10_000,
        validate_only: true
      ]

      result = CreateTopics.Request.build_request(request, opts)

      assert %Kayrock.CreateTopics.V1.Request{} = result
      assert result.validate_only == true
      assert result.timeout_ms == 10_000
    end

    test "defaults validate_only to false" do
      request = %Kayrock.CreateTopics.V1.Request{}

      opts = [
        topics: [[topic: "test-topic"]],
        timeout: 10_000
      ]

      result = CreateTopics.Request.build_request(request, opts)

      assert result.validate_only == false
    end
  end

  describe "V2 Request implementation" do
    test "builds request with validate_only flag (same as V1)" do
      request = %Kayrock.CreateTopics.V2.Request{}

      opts = [
        topics: [[topic: "test-topic", num_partitions: 6]],
        timeout: 30_000,
        validate_only: true
      ]

      result = CreateTopics.Request.build_request(request, opts)

      assert %Kayrock.CreateTopics.V2.Request{} = result
      assert result.validate_only == true
      assert result.timeout_ms == 30_000
    end
  end

  describe "Request with config entries" do
    test "converts config entries correctly" do
      request = %Kayrock.CreateTopics.V0.Request{}

      opts = [
        topics: [
          [
            topic: "test-topic",
            num_partitions: 3,
            config_entries: [
              {"cleanup.policy", "compact"},
              {"retention.ms", "86400000"}
            ]
          ]
        ],
        timeout: 10_000
      ]

      result = CreateTopics.Request.build_request(request, opts)

      [topic_req] = result.topics
      assert length(topic_req.configs) == 2

      assert Enum.find(topic_req.configs, &(&1.name == "cleanup.policy"))
      assert Enum.find(topic_req.configs, &(&1.name == "retention.ms"))
    end
  end
end
