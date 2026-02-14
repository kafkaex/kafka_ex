defmodule KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.CreateTopics.TopicResult

  describe "parse_topic_results/2" do
    test "parses successful topic creation" do
      topic_errors = [
        %{name: "new-topic", error_code: 0}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, false)

      assert length(result) == 1
      [topic_result] = result
      assert %TopicResult{} = topic_result
      assert topic_result.topic == "new-topic"
      assert topic_result.error == :no_error
      assert topic_result.error_message == nil
    end

    test "parses topic creation with error" do
      topic_errors = [
        %{name: "existing-topic", error_code: 36}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, false)

      [topic_result] = result
      assert topic_result.topic == "existing-topic"
      assert topic_result.error == :topic_already_exists
    end

    test "parses multiple topics" do
      topic_errors = [
        %{name: "topic1", error_code: 0},
        %{name: "topic2", error_code: 36},
        %{name: "topic3", error_code: 0}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, false)

      assert length(result) == 3
      topics = Enum.map(result, & &1.topic)
      assert topics == ["topic1", "topic2", "topic3"]
    end

    test "includes error_message when has_error_message? is true" do
      topic_errors = [
        %{name: "topic1", error_code: 36, error_message: "Topic already exists"}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, true)

      [topic_result] = result
      assert topic_result.error_message == "Topic already exists"
    end

    test "ignores error_message when has_error_message? is false" do
      topic_errors = [
        %{name: "topic1", error_code: 0, error_message: "should be ignored"}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, false)

      [topic_result] = result
      assert topic_result.error_message == nil
    end
  end

  describe "parse_v5_topic_results/1" do
    test "parses V5 topic with all fields" do
      topics = [
        %{
          name: "new-topic",
          error_code: 0,
          error_message: nil,
          num_partitions: 6,
          replication_factor: 3,
          configs: [
            %{
              name: "cleanup.policy",
              value: "delete",
              read_only: false,
              config_source: 5,
              is_sensitive: false
            }
          ]
        }
      ]

      result = ResponseHelpers.parse_v5_topic_results(topics)

      assert length(result) == 1
      [topic_result] = result
      assert %TopicResult{} = topic_result
      assert topic_result.topic == "new-topic"
      assert topic_result.error == :no_error
      assert topic_result.error_message == nil
      assert topic_result.num_partitions == 6
      assert topic_result.replication_factor == 3
      assert length(topic_result.configs) == 1

      [config] = topic_result.configs
      assert config.name == "cleanup.policy"
      assert config.value == "delete"
      assert config.read_only == false
      assert config.config_source == 5
      assert config.is_sensitive == false
    end

    test "parses V5 topic with error" do
      topics = [
        %{
          name: "bad-topic",
          error_code: 37,
          error_message: "Invalid partitions",
          num_partitions: -1,
          replication_factor: -1,
          configs: nil
        }
      ]

      result = ResponseHelpers.parse_v5_topic_results(topics)

      [topic_result] = result
      assert topic_result.error == :invalid_partitions
      assert topic_result.error_message == "Invalid partitions"
      assert topic_result.num_partitions == -1
      assert topic_result.replication_factor == -1
      assert topic_result.configs == nil
    end

    test "parses V5 topic with multiple configs" do
      topics = [
        %{
          name: "configured-topic",
          error_code: 0,
          error_message: nil,
          num_partitions: 3,
          replication_factor: 2,
          configs: [
            %{
              name: "retention.ms",
              value: "86400000",
              read_only: false,
              config_source: 5,
              is_sensitive: false
            },
            %{
              name: "segment.bytes",
              value: "1073741824",
              read_only: false,
              config_source: 5,
              is_sensitive: false
            },
            %{
              name: "compression.type",
              value: "producer",
              read_only: true,
              config_source: 1,
              is_sensitive: false
            }
          ]
        }
      ]

      result = ResponseHelpers.parse_v5_topic_results(topics)

      [topic_result] = result
      assert length(topic_result.configs) == 3

      compression_config = Enum.find(topic_result.configs, &(&1.name == "compression.type"))
      assert compression_config.read_only == true
      assert compression_config.config_source == 1
    end

    test "parses V5 topic with empty configs" do
      topics = [
        %{
          name: "empty-configs-topic",
          error_code: 0,
          error_message: nil,
          num_partitions: 1,
          replication_factor: 1,
          configs: []
        }
      ]

      result = ResponseHelpers.parse_v5_topic_results(topics)

      [topic_result] = result
      assert topic_result.configs == []
    end

    test "parses V5 topic with nil configs" do
      topics = [
        %{
          name: "nil-configs-topic",
          error_code: 0,
          error_message: nil,
          num_partitions: 1,
          replication_factor: 1,
          configs: nil
        }
      ]

      result = ResponseHelpers.parse_v5_topic_results(topics)

      [topic_result] = result
      assert topic_result.configs == nil
    end

    test "parses multiple V5 topics" do
      topics = [
        %{
          name: "topic1",
          error_code: 0,
          error_message: nil,
          num_partitions: 3,
          replication_factor: 2,
          configs: []
        },
        %{
          name: "topic2",
          error_code: 36,
          error_message: "Already exists",
          num_partitions: -1,
          replication_factor: -1,
          configs: nil
        }
      ]

      result = ResponseHelpers.parse_v5_topic_results(topics)

      assert length(result) == 2
      [t1, t2] = result
      assert t1.topic == "topic1"
      assert t1.error == :no_error
      assert t2.topic == "topic2"
      assert t2.error == :topic_already_exists
    end

    test "handles config with nil value" do
      topics = [
        %{
          name: "topic",
          error_code: 0,
          error_message: nil,
          num_partitions: 1,
          replication_factor: 1,
          configs: [
            %{
              name: "some.config",
              value: nil,
              read_only: false,
              config_source: 5,
              is_sensitive: false
            }
          ]
        }
      ]

      result = ResponseHelpers.parse_v5_topic_results(topics)

      [topic_result] = result
      [config] = topic_result.configs
      assert config.value == nil
    end
  end

  describe "build_response/2" do
    test "builds CreateTopics struct from topic results" do
      topic_results = [
        %TopicResult{topic: "topic1", error: :no_error}
      ]

      result = ResponseHelpers.build_response(topic_results)

      assert %CreateTopics{} = result
      assert result.topic_results == topic_results
      assert result.throttle_time_ms == nil
    end

    test "includes throttle_time_ms when provided" do
      topic_results = [%TopicResult{topic: "topic1", error: :no_error}]

      result = ResponseHelpers.build_response(topic_results, 100)

      assert result.throttle_time_ms == 100
    end
  end
end
