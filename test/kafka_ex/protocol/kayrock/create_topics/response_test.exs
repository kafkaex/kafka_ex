defmodule KafkaEx.Protocol.Kayrock.CreateTopics.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.CreateTopics
  alias KafkaEx.Messages.CreateTopics, as: CreateTopicsStruct
  alias KafkaEx.Protocol.Kayrock.CreateTopics.ResponseHelpers

  describe "ResponseHelpers.parse_topic_results/2" do
    test "parses successful topics without error_message" do
      topic_errors = [
        %{name: "topic1", error_code: 0},
        %{name: "topic2", error_code: 0}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, false)

      assert length(result) == 2
      assert Enum.all?(result, &(&1.error == :no_error))
      assert Enum.all?(result, &(&1.error_message == nil))
    end

    test "parses topics with errors" do
      topic_errors = [
        %{name: "topic1", error_code: 0},
        %{name: "topic2", error_code: 36}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, false)

      [success, failure] = result
      assert success.topic == "topic1"
      assert success.error == :no_error
      assert failure.topic == "topic2"
      assert failure.error == :topic_already_exists
    end

    test "includes error_message when has_error_message? is true" do
      topic_errors = [
        %{name: "topic1", error_code: 0, error_message: nil},
        %{name: "topic2", error_code: 36, error_message: "Topic already exists"}
      ]

      result = ResponseHelpers.parse_topic_results(topic_errors, true)

      [success, failure] = result
      assert success.error_message == nil
      assert failure.error_message == "Topic already exists"
    end
  end

  describe "ResponseHelpers.build_response/2" do
    test "builds response without throttle_time_ms" do
      topic_results = ResponseHelpers.parse_topic_results([%{name: "t1", error_code: 0}], false)

      result = ResponseHelpers.build_response(topic_results)

      assert %CreateTopicsStruct{} = result
      assert length(result.topic_results) == 1
      assert result.throttle_time_ms == nil
    end

    test "builds response with throttle_time_ms" do
      topic_results = ResponseHelpers.parse_topic_results([%{name: "t1", error_code: 0}], false)

      result = ResponseHelpers.build_response(topic_results, 100)

      assert result.throttle_time_ms == 100
    end
  end

  describe "V0 Response implementation" do
    test "parses successful response" do
      response = %Kayrock.CreateTopics.V0.Response{
        topics: [
          %{name: "topic1", error_code: 0},
          %{name: "topic2", error_code: 0}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert %CreateTopicsStruct{} = result
      assert length(result.topic_results) == 2
      assert CreateTopicsStruct.success?(result)
      assert result.throttle_time_ms == nil
    end

    test "parses response with errors" do
      response = %Kayrock.CreateTopics.V0.Response{
        topics: [
          %{name: "topic1", error_code: 0},
          %{name: "topic2", error_code: 36},
          %{name: "topic3", error_code: 37}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert length(result.topic_results) == 3

      failed = CreateTopicsStruct.failed_topics(result)
      assert length(failed) == 2
      assert Enum.find(failed, &(&1.error == :topic_already_exists))
      assert Enum.find(failed, &(&1.error == :invalid_partitions))
    end

    test "V0 does not include error_message" do
      response = %Kayrock.CreateTopics.V0.Response{
        topics: [
          %{name: "topic1", error_code: 36}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)

      [topic_result] = result.topic_results
      assert topic_result.error_message == nil
    end
  end

  describe "V1 Response implementation" do
    test "parses successful response" do
      response = %Kayrock.CreateTopics.V1.Response{
        topics: [
          %{name: "topic1", error_code: 0, error_message: nil}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert CreateTopicsStruct.success?(result)
    end

    test "parses response with error_message" do
      response = %Kayrock.CreateTopics.V1.Response{
        topics: [
          %{name: "topic1", error_code: 36, error_message: "Topic 'topic1' already exists."}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)

      [topic_result] = result.topic_results
      assert topic_result.error == :topic_already_exists
      assert topic_result.error_message == "Topic 'topic1' already exists."
    end

    test "V1 does not include throttle_time_ms" do
      response = %Kayrock.CreateTopics.V1.Response{
        topics: [%{name: "topic1", error_code: 0, error_message: nil}]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert result.throttle_time_ms == nil
    end
  end

  describe "V2 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.CreateTopics.V2.Response{
        throttle_time_ms: 50,
        topics: [
          %{name: "topic1", error_code: 0, error_message: nil}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert CreateTopicsStruct.success?(result)
      assert result.throttle_time_ms == 50
    end

    test "parses response with error and error_message" do
      response = %Kayrock.CreateTopics.V2.Response{
        throttle_time_ms: 0,
        topics: [
          %{name: "topic1", error_code: 38, error_message: "Invalid replication factor"}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)

      [topic_result] = result.topic_results
      assert topic_result.error == :invalid_replication_factor
      assert topic_result.error_message == "Invalid replication factor"
    end

    test "handles zero throttle time" do
      response = %Kayrock.CreateTopics.V2.Response{
        throttle_time_ms: 0,
        topics: [%{name: "topic1", error_code: 0, error_message: nil}]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert result.throttle_time_ms == 0
    end
  end

  describe "Version comparison" do
    test "all versions return CreateTopics struct on success" do
      for {response_mod, topic_errors, extra_fields} <- [
            {Kayrock.CreateTopics.V0.Response, [%{name: "t", error_code: 0}], %{}},
            {Kayrock.CreateTopics.V1.Response, [%{name: "t", error_code: 0, error_message: nil}], %{}},
            {Kayrock.CreateTopics.V2.Response, [%{name: "t", error_code: 0, error_message: nil}],
             %{throttle_time_ms: 0}}
          ] do
        response = struct(response_mod, Map.merge(%{topics: topic_errors}, extra_fields))
        assert {:ok, %CreateTopicsStruct{}} = CreateTopics.Response.parse_response(response)
      end
    end

    test "V1 and V2 include error_message while V0 does not" do
      # V0 - no error_message field expected in response
      v0_response = %Kayrock.CreateTopics.V0.Response{
        topics: [%{name: "t", error_code: 36}]
      }

      {:ok, v0_result} = CreateTopics.Response.parse_response(v0_response)
      [v0_topic] = v0_result.topic_results
      assert v0_topic.error_message == nil

      # V1 - has error_message
      v1_response = %Kayrock.CreateTopics.V1.Response{
        topics: [%{name: "t", error_code: 36, error_message: "Already exists"}]
      }

      {:ok, v1_result} = CreateTopics.Response.parse_response(v1_response)
      [v1_topic] = v1_result.topic_results
      assert v1_topic.error_message == "Already exists"

      # V2 - has error_message and throttle_time_ms
      v2_response = %Kayrock.CreateTopics.V2.Response{
        throttle_time_ms: 10,
        topics: [%{name: "t", error_code: 36, error_message: "Already exists"}]
      }

      {:ok, v2_result} = CreateTopics.Response.parse_response(v2_response)
      [v2_topic] = v2_result.topic_results
      assert v2_topic.error_message == "Already exists"
      assert v2_result.throttle_time_ms == 10
    end
  end

  describe "Edge cases" do
    test "handles empty topic list" do
      response = %Kayrock.CreateTopics.V0.Response{
        topics: []
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert result.topic_results == []
      assert CreateTopicsStruct.success?(result)
    end

    test "handles nil error_message in V1" do
      response = %Kayrock.CreateTopics.V1.Response{
        topics: [%{name: "t", error_code: 0, error_message: nil}]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      [topic] = result.topic_results
      assert topic.error_message == nil
    end
  end
end
