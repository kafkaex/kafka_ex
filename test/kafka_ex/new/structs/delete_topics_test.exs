defmodule KafkaEx.New.Kafka.DeleteTopicsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Kafka.DeleteTopics
  alias KafkaEx.New.Kafka.DeleteTopics.TopicResult

  describe "DeleteTopics struct" do
    test "build/1 creates struct with defaults" do
      result = DeleteTopics.build()
      assert %DeleteTopics{} = result
      assert result.topic_results == []
      assert result.throttle_time_ms == nil
    end

    test "build/1 creates struct with topic_results" do
      topic_results = [
        TopicResult.build(topic: "topic1", error: :no_error),
        TopicResult.build(topic: "topic2", error: :no_error)
      ]

      result = DeleteTopics.build(topic_results: topic_results)
      assert length(result.topic_results) == 2
    end

    test "build/1 creates struct with throttle_time_ms" do
      result = DeleteTopics.build(throttle_time_ms: 100)
      assert result.throttle_time_ms == 100
    end
  end

  describe "success?/1" do
    test "returns true when all topics deleted successfully" do
      result =
        DeleteTopics.build(
          topic_results: [
            TopicResult.build(topic: "topic1", error: :no_error),
            TopicResult.build(topic: "topic2", error: :no_error)
          ]
        )

      assert DeleteTopics.success?(result) == true
    end

    test "returns false when any topic failed" do
      result =
        DeleteTopics.build(
          topic_results: [
            TopicResult.build(topic: "topic1", error: :no_error),
            TopicResult.build(topic: "topic2", error: :unknown_topic_or_partition)
          ]
        )

      assert DeleteTopics.success?(result) == false
    end

    test "returns true for empty topic_results" do
      result = DeleteTopics.build(topic_results: [])
      assert DeleteTopics.success?(result) == true
    end
  end

  describe "failed_topics/1" do
    test "returns empty list when all succeeded" do
      result =
        DeleteTopics.build(
          topic_results: [
            TopicResult.build(topic: "topic1", error: :no_error),
            TopicResult.build(topic: "topic2", error: :no_error)
          ]
        )

      assert DeleteTopics.failed_topics(result) == []
    end

    test "returns only failed topics" do
      failed = TopicResult.build(topic: "topic2", error: :unknown_topic_or_partition)

      result =
        DeleteTopics.build(
          topic_results: [
            TopicResult.build(topic: "topic1", error: :no_error),
            failed
          ]
        )

      assert DeleteTopics.failed_topics(result) == [failed]
    end
  end

  describe "successful_topics/1" do
    test "returns only successful topics" do
      success = TopicResult.build(topic: "topic1", error: :no_error)

      result =
        DeleteTopics.build(
          topic_results: [
            success,
            TopicResult.build(topic: "topic2", error: :unknown_topic_or_partition)
          ]
        )

      assert DeleteTopics.successful_topics(result) == [success]
    end
  end

  describe "get_topic_result/2" do
    test "returns topic result for existing topic" do
      topic1 = TopicResult.build(topic: "topic1", error: :no_error)
      topic2 = TopicResult.build(topic: "topic2", error: :unknown_topic_or_partition)

      result = DeleteTopics.build(topic_results: [topic1, topic2])

      assert DeleteTopics.get_topic_result(result, "topic1") == topic1
      assert DeleteTopics.get_topic_result(result, "topic2") == topic2
    end

    test "returns nil for non-existent topic" do
      result =
        DeleteTopics.build(topic_results: [TopicResult.build(topic: "topic1", error: :no_error)])

      assert DeleteTopics.get_topic_result(result, "unknown") == nil
    end
  end

  describe "TopicResult struct" do
    test "build/1 creates struct with required fields" do
      result = TopicResult.build(topic: "my-topic", error: :no_error)
      assert result.topic == "my-topic"
      assert result.error == :no_error
    end

    test "build/1 creates struct with error" do
      result = TopicResult.build(topic: "my-topic", error: :unknown_topic_or_partition)

      assert result.topic == "my-topic"
      assert result.error == :unknown_topic_or_partition
    end

    test "build/1 raises when topic is missing" do
      assert_raise KeyError, fn ->
        TopicResult.build(error: :no_error)
      end
    end
  end

  describe "TopicResult.success?/1" do
    test "returns true for :no_error" do
      result = TopicResult.build(topic: "my-topic", error: :no_error)
      assert TopicResult.success?(result) == true
    end

    test "returns false for any other error" do
      result = TopicResult.build(topic: "my-topic", error: :unknown_topic_or_partition)
      assert TopicResult.success?(result) == false

      result = TopicResult.build(topic: "my-topic", error: :topic_deletion_disabled)
      assert TopicResult.success?(result) == false

      result = TopicResult.build(topic: "my-topic", error: :unknown)
      assert TopicResult.success?(result) == false
    end
  end
end
