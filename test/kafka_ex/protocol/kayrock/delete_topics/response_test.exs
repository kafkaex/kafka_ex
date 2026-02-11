defmodule KafkaEx.Protocol.Kayrock.DeleteTopics.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.DeleteTopics
  alias KafkaEx.Messages.DeleteTopics, as: DeleteTopicsStruct
  alias KafkaEx.Protocol.Kayrock.DeleteTopics.ResponseHelpers

  describe "ResponseHelpers.parse_topic_results/1" do
    test "parses successful topics" do
      topic_error_codes = [
        %{name: "topic1", error_code: 0},
        %{name: "topic2", error_code: 0}
      ]

      result = ResponseHelpers.parse_topic_results(topic_error_codes)

      assert length(result) == 2
      assert Enum.all?(result, &(&1.error == :no_error))
    end

    test "parses topics with errors" do
      topic_error_codes = [
        %{name: "topic1", error_code: 0},
        %{name: "topic2", error_code: 3}
      ]

      result = ResponseHelpers.parse_topic_results(topic_error_codes)

      [success, failure] = result
      assert success.topic == "topic1"
      assert success.error == :no_error
      assert failure.topic == "topic2"
      assert failure.error == :unknown_topic_or_partition
    end
  end

  describe "ResponseHelpers.build_response/2" do
    test "builds response without throttle_time_ms" do
      topic_results = ResponseHelpers.parse_topic_results([%{name: "t1", error_code: 0}])

      result = ResponseHelpers.build_response(topic_results)

      assert %DeleteTopicsStruct{} = result
      assert length(result.topic_results) == 1
      assert result.throttle_time_ms == nil
    end

    test "builds response with throttle_time_ms" do
      topic_results = ResponseHelpers.parse_topic_results([%{name: "t1", error_code: 0}])

      result = ResponseHelpers.build_response(topic_results, 100)

      assert result.throttle_time_ms == 100
    end
  end

  describe "V0 Response implementation" do
    test "parses successful response" do
      response = %Kayrock.DeleteTopics.V0.Response{
        responses: [
          %{name: "topic1", error_code: 0},
          %{name: "topic2", error_code: 0}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert %DeleteTopicsStruct{} = result
      assert length(result.topic_results) == 2
      assert DeleteTopicsStruct.success?(result)
      assert result.throttle_time_ms == nil
    end

    test "parses response with errors" do
      response = %Kayrock.DeleteTopics.V0.Response{
        responses: [
          %{name: "topic1", error_code: 0},
          %{name: "topic2", error_code: 3},
          %{name: "topic3", error_code: 41}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert length(result.topic_results) == 3

      failed = DeleteTopicsStruct.failed_topics(result)
      assert length(failed) == 2
      assert Enum.find(failed, &(&1.error == :unknown_topic_or_partition))
      assert Enum.find(failed, &(&1.error == :not_controller))
    end

    test "V0 does not include throttle_time_ms" do
      response = %Kayrock.DeleteTopics.V0.Response{
        responses: [
          %{name: "topic1", error_code: 0}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert result.throttle_time_ms == nil
    end
  end

  describe "V1 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.DeleteTopics.V1.Response{
        throttle_time_ms: 50,
        responses: [
          %{name: "topic1", error_code: 0}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert DeleteTopicsStruct.success?(result)
      assert result.throttle_time_ms == 50
    end

    test "parses response with error" do
      response = %Kayrock.DeleteTopics.V1.Response{
        throttle_time_ms: 0,
        responses: [
          %{name: "topic1", error_code: 3}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)

      [topic_result] = result.topic_results
      assert topic_result.error == :unknown_topic_or_partition
    end

    test "handles zero throttle time" do
      response = %Kayrock.DeleteTopics.V1.Response{
        throttle_time_ms: 0,
        responses: [%{name: "topic1", error_code: 0}]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert result.throttle_time_ms == 0
    end
  end

  describe "Version comparison" do
    test "all versions return DeleteTopics struct on success" do
      for {response_mod, extra_fields} <- [
            {Kayrock.DeleteTopics.V0.Response, %{}},
            {Kayrock.DeleteTopics.V1.Response, %{throttle_time_ms: 0}}
          ] do
        response =
          struct(response_mod, Map.merge(%{responses: [%{name: "t", error_code: 0}]}, extra_fields))

        assert {:ok, %DeleteTopicsStruct{}} = DeleteTopics.Response.parse_response(response)
      end
    end

    test "V1 includes throttle_time_ms while V0 does not" do
      # V0 - no throttle_time_ms
      v0_response = %Kayrock.DeleteTopics.V0.Response{
        responses: [%{name: "t", error_code: 0}]
      }

      {:ok, v0_result} = DeleteTopics.Response.parse_response(v0_response)
      assert v0_result.throttle_time_ms == nil

      # V1 - has throttle_time_ms
      v1_response = %Kayrock.DeleteTopics.V1.Response{
        throttle_time_ms: 25,
        responses: [%{name: "t", error_code: 0}]
      }

      {:ok, v1_result} = DeleteTopics.Response.parse_response(v1_response)
      assert v1_result.throttle_time_ms == 25
    end
  end

  describe "Edge cases" do
    test "handles empty topic list" do
      response = %Kayrock.DeleteTopics.V0.Response{
        responses: []
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert result.topic_results == []
      assert DeleteTopicsStruct.success?(result)
    end
  end
end
