defmodule KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.CreateTopics.TopicResult

  describe "parse_topic_results/2" do
    test "parses successful topic creation" do
      topic_errors = [
        %{topic: "new-topic", error_code: 0}
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
        %{topic: "existing-topic", error_code: 36}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, false)

      [topic_result] = result
      assert topic_result.topic == "existing-topic"
      assert topic_result.error == :topic_already_exists
    end

    test "parses multiple topics" do
      topic_errors = [
        %{topic: "topic1", error_code: 0},
        %{topic: "topic2", error_code: 36},
        %{topic: "topic3", error_code: 0}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, false)

      assert length(result) == 3
      topics = Enum.map(result, & &1.topic)
      assert topics == ["topic1", "topic2", "topic3"]
    end

    test "includes error_message when has_error_message? is true" do
      topic_errors = [
        %{topic: "topic1", error_code: 36, error_message: "Topic already exists"}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, true)

      [topic_result] = result
      assert topic_result.error_message == "Topic already exists"
    end

    test "ignores error_message when has_error_message? is false" do
      topic_errors = [
        %{topic: "topic1", error_code: 0, error_message: "should be ignored"}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, false)

      [topic_result] = result
      assert topic_result.error_message == nil
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
