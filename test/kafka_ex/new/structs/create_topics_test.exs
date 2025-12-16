defmodule KafkaEx.New.Kafka.CreateTopicsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Kafka.CreateTopics
  alias KafkaEx.New.Kafka.CreateTopics.TopicResult

  describe "CreateTopics struct" do
    test "build/1 creates struct with defaults" do
      result = CreateTopics.build()
      assert %CreateTopics{} = result
      assert result.topic_results == []
      assert result.throttle_time_ms == nil
    end

    test "build/1 creates struct with topic_results" do
      topic_results = [
        TopicResult.build(topic: "topic1", error: :no_error),
        TopicResult.build(topic: "topic2", error: :no_error)
      ]

      result = CreateTopics.build(topic_results: topic_results)
      assert length(result.topic_results) == 2
    end

    test "build/1 creates struct with throttle_time_ms" do
      result = CreateTopics.build(throttle_time_ms: 100)
      assert result.throttle_time_ms == 100
    end
  end

  describe "success?/1" do
    test "returns true when all topics created successfully" do
      result =
        CreateTopics.build(
          topic_results: [
            TopicResult.build(topic: "topic1", error: :no_error),
            TopicResult.build(topic: "topic2", error: :no_error)
          ]
        )

      assert CreateTopics.success?(result) == true
    end

    test "returns false when any topic failed" do
      result =
        CreateTopics.build(
          topic_results: [
            TopicResult.build(topic: "topic1", error: :no_error),
            TopicResult.build(topic: "topic2", error: :topic_already_exists)
          ]
        )

      assert CreateTopics.success?(result) == false
    end

    test "returns true for empty topic_results" do
      result = CreateTopics.build(topic_results: [])
      assert CreateTopics.success?(result) == true
    end
  end

  describe "failed_topics/1" do
    test "returns empty list when all succeeded" do
      result =
        CreateTopics.build(
          topic_results: [
            TopicResult.build(topic: "topic1", error: :no_error),
            TopicResult.build(topic: "topic2", error: :no_error)
          ]
        )

      assert CreateTopics.failed_topics(result) == []
    end

    test "returns only failed topics" do
      failed = TopicResult.build(topic: "topic2", error: :topic_already_exists)

      result =
        CreateTopics.build(
          topic_results: [
            TopicResult.build(topic: "topic1", error: :no_error),
            failed
          ]
        )

      assert CreateTopics.failed_topics(result) == [failed]
    end
  end

  describe "successful_topics/1" do
    test "returns only successful topics" do
      success = TopicResult.build(topic: "topic1", error: :no_error)

      result =
        CreateTopics.build(
          topic_results: [
            success,
            TopicResult.build(topic: "topic2", error: :topic_already_exists)
          ]
        )

      assert CreateTopics.successful_topics(result) == [success]
    end
  end

  describe "get_topic_result/2" do
    test "returns topic result for existing topic" do
      topic1 = TopicResult.build(topic: "topic1", error: :no_error)
      topic2 = TopicResult.build(topic: "topic2", error: :topic_already_exists)

      result = CreateTopics.build(topic_results: [topic1, topic2])

      assert CreateTopics.get_topic_result(result, "topic1") == topic1
      assert CreateTopics.get_topic_result(result, "topic2") == topic2
    end

    test "returns nil for non-existent topic" do
      result =
        CreateTopics.build(topic_results: [TopicResult.build(topic: "topic1", error: :no_error)])

      assert CreateTopics.get_topic_result(result, "unknown") == nil
    end
  end

  describe "TopicResult struct" do
    test "build/1 creates struct with required fields" do
      result = TopicResult.build(topic: "my-topic", error: :no_error)
      assert result.topic == "my-topic"
      assert result.error == :no_error
      assert result.error_message == nil
    end

    test "build/1 creates struct with error_message" do
      result =
        TopicResult.build(
          topic: "my-topic",
          error: :topic_already_exists,
          error_message: "Topic already exists"
        )

      assert result.topic == "my-topic"
      assert result.error == :topic_already_exists
      assert result.error_message == "Topic already exists"
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
      result = TopicResult.build(topic: "my-topic", error: :topic_already_exists)
      assert TopicResult.success?(result) == false

      result = TopicResult.build(topic: "my-topic", error: :invalid_replication_factor)
      assert TopicResult.success?(result) == false

      result = TopicResult.build(topic: "my-topic", error: :unknown)
      assert TopicResult.success?(result) == false
    end
  end
end
