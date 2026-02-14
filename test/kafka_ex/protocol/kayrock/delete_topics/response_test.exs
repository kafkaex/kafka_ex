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

  describe "ResponseHelpers.parse_v0_response/1" do
    test "parses V0 response without throttle_time_ms" do
      response = %{responses: [%{name: "topic1", error_code: 0}]}

      assert {:ok, result} = ResponseHelpers.parse_v0_response(response)
      assert %DeleteTopicsStruct{} = result
      assert result.throttle_time_ms == nil
      assert length(result.topic_results) == 1
    end
  end

  describe "ResponseHelpers.parse_v1_plus_response/1" do
    test "parses V1+ response with throttle_time_ms" do
      response = %{throttle_time_ms: 42, responses: [%{name: "topic1", error_code: 0}]}

      assert {:ok, result} = ResponseHelpers.parse_v1_plus_response(response)
      assert %DeleteTopicsStruct{} = result
      assert result.throttle_time_ms == 42
      assert length(result.topic_results) == 1
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

  describe "V2 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.DeleteTopics.V2.Response{
        throttle_time_ms: 75,
        responses: [
          %{name: "v2-topic", error_code: 0}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert %DeleteTopicsStruct{} = result
      assert DeleteTopicsStruct.success?(result)
      assert result.throttle_time_ms == 75
    end

    test "parses response with mixed results" do
      response = %Kayrock.DeleteTopics.V2.Response{
        throttle_time_ms: 0,
        responses: [
          %{name: "good-topic", error_code: 0},
          %{name: "bad-topic", error_code: 36}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert length(result.topic_results) == 2

      [success, failure] = result.topic_results
      assert success.topic == "good-topic"
      assert success.error == :no_error
      assert failure.topic == "bad-topic"
      assert failure.error == :topic_already_exists
    end
  end

  describe "V3 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.DeleteTopics.V3.Response{
        throttle_time_ms: 10,
        responses: [
          %{name: "v3-topic-1", error_code: 0},
          %{name: "v3-topic-2", error_code: 0}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert %DeleteTopicsStruct{} = result
      assert length(result.topic_results) == 2
      assert DeleteTopicsStruct.success?(result)
      assert result.throttle_time_ms == 10
    end

    test "parses response with topic_authorization_failed error" do
      response = %Kayrock.DeleteTopics.V3.Response{
        throttle_time_ms: 0,
        responses: [
          %{name: "unauthorized-topic", error_code: 29}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)

      [topic_result] = result.topic_results
      assert topic_result.topic == "unauthorized-topic"
      assert topic_result.error == :topic_authorization_failed
    end
  end

  describe "V4 Response implementation (FLEX)" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.DeleteTopics.V4.Response{
        throttle_time_ms: 5,
        responses: [
          %{name: "flex-topic", error_code: 0}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert %DeleteTopicsStruct{} = result
      assert DeleteTopicsStruct.success?(result)
      assert result.throttle_time_ms == 5
    end

    test "parses response with errors" do
      response = %Kayrock.DeleteTopics.V4.Response{
        throttle_time_ms: 0,
        responses: [
          %{name: "topic1", error_code: 0},
          %{name: "topic2", error_code: 41}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)

      [success, failure] = result.topic_results
      assert success.error == :no_error
      assert failure.error == :not_controller
    end

    test "V4 struct has tagged_fields but they do not affect parsed result" do
      response = %Kayrock.DeleteTopics.V4.Response{
        throttle_time_ms: 0,
        responses: [%{name: "t", error_code: 0}],
        tagged_fields: %{}
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert %DeleteTopicsStruct{} = result
      assert result.throttle_time_ms == 0
    end
  end

  describe "cross-version consistency V0-V4" do
    @v1_plus_versions [
      {Kayrock.DeleteTopics.V1.Response, "V1"},
      {Kayrock.DeleteTopics.V2.Response, "V2"},
      {Kayrock.DeleteTopics.V3.Response, "V3"},
      {Kayrock.DeleteTopics.V4.Response, "V4"}
    ]

    test "all versions return DeleteTopics struct on success" do
      all_versions = [
        {Kayrock.DeleteTopics.V0.Response, %{}},
        {Kayrock.DeleteTopics.V1.Response, %{throttle_time_ms: 0}},
        {Kayrock.DeleteTopics.V2.Response, %{throttle_time_ms: 0}},
        {Kayrock.DeleteTopics.V3.Response, %{throttle_time_ms: 0}},
        {Kayrock.DeleteTopics.V4.Response, %{throttle_time_ms: 0}}
      ]

      for {response_mod, extra_fields} <- all_versions do
        response =
          struct(response_mod, Map.merge(%{responses: [%{name: "t", error_code: 0}]}, extra_fields))

        assert {:ok, %DeleteTopicsStruct{}} = DeleteTopics.Response.parse_response(response)
      end
    end

    for {mod, label} <- @v1_plus_versions do
      test "#{label} includes throttle_time_ms" do
        response =
          struct(unquote(mod), %{
            throttle_time_ms: 99,
            responses: [%{name: "t", error_code: 0}]
          })

        {:ok, result} = DeleteTopics.Response.parse_response(response)
        assert result.throttle_time_ms == 99
      end

      test "#{label} parses error codes correctly" do
        response =
          struct(unquote(mod), %{
            throttle_time_ms: 0,
            responses: [
              %{name: "ok-topic", error_code: 0},
              %{name: "bad-topic", error_code: 3}
            ]
          })

        {:ok, result} = DeleteTopics.Response.parse_response(response)

        assert length(result.topic_results) == 2
        [ok, bad] = result.topic_results
        assert ok.error == :no_error
        assert bad.error == :unknown_topic_or_partition
      end
    end

    test "V0 does not include throttle_time_ms while V1+ does" do
      v0_response = %Kayrock.DeleteTopics.V0.Response{
        responses: [%{name: "t", error_code: 0}]
      }

      {:ok, v0_result} = DeleteTopics.Response.parse_response(v0_response)
      assert v0_result.throttle_time_ms == nil

      v1_response = %Kayrock.DeleteTopics.V1.Response{
        throttle_time_ms: 25,
        responses: [%{name: "t", error_code: 0}]
      }

      {:ok, v1_result} = DeleteTopics.Response.parse_response(v1_response)
      assert v1_result.throttle_time_ms == 25
    end
  end

  describe "Any fallback response implementation" do
    test "handles V0-like map (no throttle_time_ms)" do
      response = %{responses: [%{name: "any-topic", error_code: 0}]}

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert %DeleteTopicsStruct{} = result
      assert result.throttle_time_ms == nil
      assert length(result.topic_results) == 1
    end

    test "handles V1+-like map (with throttle_time_ms)" do
      response = %{
        throttle_time_ms: 33,
        responses: [
          %{name: "any-topic-1", error_code: 0},
          %{name: "any-topic-2", error_code: 3}
        ]
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert %DeleteTopicsStruct{} = result
      assert result.throttle_time_ms == 33
      assert length(result.topic_results) == 2

      [ok, bad] = result.topic_results
      assert ok.error == :no_error
      assert bad.error == :unknown_topic_or_partition
    end

    test "handles V4-like map (with tagged_fields and throttle_time_ms)" do
      response = %{
        throttle_time_ms: 0,
        responses: [%{name: "flex-any-topic", error_code: 0}],
        tagged_fields: %{}
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert %DeleteTopicsStruct{} = result
      assert result.throttle_time_ms == 0
      assert DeleteTopicsStruct.success?(result)
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

    test "handles empty topic list with V1+ throttle_time_ms" do
      response = %Kayrock.DeleteTopics.V1.Response{
        throttle_time_ms: 0,
        responses: []
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert result.topic_results == []
      assert result.throttle_time_ms == 0
      assert DeleteTopicsStruct.success?(result)
    end

    test "handles large number of topics" do
      topic_responses =
        for i <- 1..100 do
          %{name: "topic-#{i}", error_code: 0}
        end

      response = %Kayrock.DeleteTopics.V2.Response{
        throttle_time_ms: 0,
        responses: topic_responses
      }

      assert {:ok, result} = DeleteTopics.Response.parse_response(response)
      assert length(result.topic_results) == 100
      assert DeleteTopicsStruct.success?(result)
    end

    test "handles all common error codes" do
      error_scenarios = [
        {0, :no_error},
        {3, :unknown_topic_or_partition},
        {29, :topic_authorization_failed},
        {41, :not_controller},
        {73, :topic_deletion_disabled}
      ]

      for {code, expected_atom} <- error_scenarios do
        response = %Kayrock.DeleteTopics.V3.Response{
          throttle_time_ms: 0,
          responses: [%{name: "topic", error_code: code}]
        }

        assert {:ok, result} = DeleteTopics.Response.parse_response(response)
        [topic_result] = result.topic_results
        assert topic_result.error == expected_atom
      end
    end
  end
end
