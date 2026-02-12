defmodule KafkaEx.Protocol.Kayrock.DeleteTopics.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.ResponseHelpers
  alias KafkaEx.Messages.DeleteTopics
  alias KafkaEx.Messages.DeleteTopics.TopicResult

  describe "parse_topic_results/1" do
    test "parses successful topic deletion" do
      topic_error_codes = [
        %{name: "deleted-topic", error_code: 0}
      ]

      result = ResponseHelpers.parse_topic_results(topic_error_codes)

      assert length(result) == 1
      [topic_result] = result
      assert %TopicResult{} = topic_result
      assert topic_result.topic == "deleted-topic"
      assert topic_result.error == :no_error
    end

    test "parses topic deletion with error" do
      topic_error_codes = [
        %{name: "nonexistent-topic", error_code: 3}
      ]

      result = ResponseHelpers.parse_topic_results(topic_error_codes)

      [topic_result] = result
      assert topic_result.topic == "nonexistent-topic"
      assert topic_result.error == :unknown_topic_or_partition
    end

    test "parses multiple topics" do
      topic_error_codes = [
        %{name: "topic1", error_code: 0},
        %{name: "topic2", error_code: 0},
        %{name: "topic3", error_code: 3}
      ]

      result = ResponseHelpers.parse_topic_results(topic_error_codes)

      assert length(result) == 3
      [r1, r2, r3] = result
      assert r1.error == :no_error
      assert r2.error == :no_error
      assert r3.error == :unknown_topic_or_partition
    end
  end

  describe "build_response/2" do
    test "builds DeleteTopics struct from topic results" do
      topic_results = [
        %TopicResult{topic: "topic1", error: :no_error}
      ]

      result = ResponseHelpers.build_response(topic_results)

      assert %DeleteTopics{} = result
      assert result.topic_results == topic_results
      assert result.throttle_time_ms == nil
    end

    test "includes throttle_time_ms when provided" do
      topic_results = [%TopicResult{topic: "topic1", error: :no_error}]

      result = ResponseHelpers.build_response(topic_results, 50)

      assert result.throttle_time_ms == 50
    end
  end
end
