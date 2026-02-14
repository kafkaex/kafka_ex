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

  describe "V3 Response implementation" do
    test "parses successful response with throttle_time_ms (identical to V2)" do
      response = %Kayrock.CreateTopics.V3.Response{
        throttle_time_ms: 25,
        topics: [
          %{name: "topic1", error_code: 0, error_message: nil}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert CreateTopicsStruct.success?(result)
      assert result.throttle_time_ms == 25
    end

    test "parses response with error and error_message" do
      response = %Kayrock.CreateTopics.V3.Response{
        throttle_time_ms: 0,
        topics: [
          %{name: "topic1", error_code: 36, error_message: "Topic already exists"}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)

      [topic_result] = result.topic_results
      assert topic_result.error == :topic_already_exists
      assert topic_result.error_message == "Topic already exists"
    end

    test "handles zero throttle time" do
      response = %Kayrock.CreateTopics.V3.Response{
        throttle_time_ms: 0,
        topics: [%{name: "t", error_code: 0, error_message: nil}]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert result.throttle_time_ms == 0
    end
  end

  describe "V4 Response implementation" do
    test "parses successful response with throttle_time_ms (identical to V2/V3)" do
      response = %Kayrock.CreateTopics.V4.Response{
        throttle_time_ms: 75,
        topics: [
          %{name: "topic1", error_code: 0, error_message: nil},
          %{name: "topic2", error_code: 0, error_message: nil}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert CreateTopicsStruct.success?(result)
      assert result.throttle_time_ms == 75
      assert length(result.topic_results) == 2
    end

    test "parses response with mixed success and errors" do
      response = %Kayrock.CreateTopics.V4.Response{
        throttle_time_ms: 0,
        topics: [
          %{name: "new-topic", error_code: 0, error_message: nil},
          %{name: "existing-topic", error_code: 36, error_message: "Topic already exists"}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      refute CreateTopicsStruct.success?(result)

      assert length(CreateTopicsStruct.successful_topics(result)) == 1
      assert length(CreateTopicsStruct.failed_topics(result)) == 1
    end
  end

  describe "V5 Response implementation (FLEX)" do
    test "parses successful response with V5-specific fields" do
      response = %Kayrock.CreateTopics.V5.Response{
        throttle_time_ms: 10,
        topics: [
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
                is_sensitive: false,
                tagged_fields: []
              },
              %{
                name: "retention.ms",
                value: "604800000",
                read_only: false,
                config_source: 5,
                is_sensitive: false,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert CreateTopicsStruct.success?(result)
      assert result.throttle_time_ms == 10

      [topic_result] = result.topic_results
      assert topic_result.topic == "new-topic"
      assert topic_result.error == :no_error
      assert topic_result.num_partitions == 6
      assert topic_result.replication_factor == 3

      assert length(topic_result.configs) == 2
      cleanup_config = Enum.find(topic_result.configs, &(&1.name == "cleanup.policy"))
      assert cleanup_config.value == "delete"
      assert cleanup_config.read_only == false
      assert cleanup_config.config_source == 5
      assert cleanup_config.is_sensitive == false

      retention_config = Enum.find(topic_result.configs, &(&1.name == "retention.ms"))
      assert retention_config.value == "604800000"
    end

    test "parses V5 response with error" do
      response = %Kayrock.CreateTopics.V5.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "bad-topic",
            error_code: 37,
            error_message: "Number of partitions must be larger than 0.",
            num_partitions: -1,
            replication_factor: -1,
            configs: nil,
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      refute CreateTopicsStruct.success?(result)

      [topic_result] = result.topic_results
      assert topic_result.error == :invalid_partitions
      assert topic_result.error_message == "Number of partitions must be larger than 0."
      assert topic_result.num_partitions == -1
      assert topic_result.replication_factor == -1
      assert topic_result.configs == nil
    end

    test "parses V5 response with empty configs list" do
      response = %Kayrock.CreateTopics.V5.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "simple-topic",
            error_code: 0,
            error_message: nil,
            num_partitions: 1,
            replication_factor: 1,
            configs: [],
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)

      [topic_result] = result.topic_results
      assert topic_result.configs == []
      assert topic_result.num_partitions == 1
      assert topic_result.replication_factor == 1
    end

    test "parses V5 response with multiple topics" do
      response = %Kayrock.CreateTopics.V5.Response{
        throttle_time_ms: 5,
        topics: [
          %{
            name: "topic-a",
            error_code: 0,
            error_message: nil,
            num_partitions: 3,
            replication_factor: 2,
            configs: [],
            tagged_fields: []
          },
          %{
            name: "topic-b",
            error_code: 36,
            error_message: "Topic already exists",
            num_partitions: -1,
            replication_factor: -1,
            configs: nil,
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      assert result.throttle_time_ms == 5
      assert length(result.topic_results) == 2

      [topic_a, topic_b] = result.topic_results
      assert topic_a.topic == "topic-a"
      assert topic_a.error == :no_error
      assert topic_a.num_partitions == 3

      assert topic_b.topic == "topic-b"
      assert topic_b.error == :topic_already_exists
      assert topic_b.num_partitions == -1
    end
  end

  describe "Version comparison" do
    test "all versions return CreateTopics struct on success" do
      for {response_mod, topic_errors, extra_fields} <- [
            {Kayrock.CreateTopics.V0.Response, [%{name: "t", error_code: 0}], %{}},
            {Kayrock.CreateTopics.V1.Response, [%{name: "t", error_code: 0, error_message: nil}], %{}},
            {Kayrock.CreateTopics.V2.Response, [%{name: "t", error_code: 0, error_message: nil}],
             %{throttle_time_ms: 0}},
            {Kayrock.CreateTopics.V3.Response, [%{name: "t", error_code: 0, error_message: nil}],
             %{throttle_time_ms: 0}},
            {Kayrock.CreateTopics.V4.Response, [%{name: "t", error_code: 0, error_message: nil}],
             %{throttle_time_ms: 0}},
            {Kayrock.CreateTopics.V5.Response,
             [
               %{
                 name: "t",
                 error_code: 0,
                 error_message: nil,
                 num_partitions: 1,
                 replication_factor: 1,
                 configs: [],
                 tagged_fields: []
               }
             ], %{throttle_time_ms: 0, tagged_fields: []}}
          ] do
        response = struct(response_mod, Map.merge(%{topics: topic_errors}, extra_fields))
        assert {:ok, %CreateTopicsStruct{}} = CreateTopics.Response.parse_response(response)
      end
    end

    test "V1+ include error_message while V0 does not" do
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

    test "V2-V4 share identical response format (throttle + error_message)" do
      for {version, response_mod} <- [
            {2, Kayrock.CreateTopics.V2.Response},
            {3, Kayrock.CreateTopics.V3.Response},
            {4, Kayrock.CreateTopics.V4.Response}
          ] do
        response =
          struct(response_mod, %{
            throttle_time_ms: 42,
            topics: [
              %{name: "test-topic", error_code: 36, error_message: "Already exists"}
            ]
          })

        {:ok, result} = CreateTopics.Response.parse_response(response)

        assert result.throttle_time_ms == 42,
               "V#{version}: throttle_time_ms should be 42"

        [topic_result] = result.topic_results

        assert topic_result.error == :topic_already_exists,
               "V#{version}: error should be :topic_already_exists"

        assert topic_result.error_message == "Already exists",
               "V#{version}: error_message mismatch"

        # V0-V4 should not have V5 fields
        assert topic_result.num_partitions == nil,
               "V#{version}: num_partitions should be nil"

        assert topic_result.replication_factor == nil,
               "V#{version}: replication_factor should be nil"

        assert topic_result.configs == nil,
               "V#{version}: configs should be nil"
      end
    end

    test "V5 adds num_partitions, replication_factor, configs" do
      response = %Kayrock.CreateTopics.V5.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "t",
            error_code: 0,
            error_message: nil,
            num_partitions: 10,
            replication_factor: 3,
            configs: [
              %{
                name: "retention.ms",
                value: "86400000",
                read_only: false,
                config_source: 5,
                is_sensitive: false,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      {:ok, result} = CreateTopics.Response.parse_response(response)
      [topic_result] = result.topic_results

      assert topic_result.num_partitions == 10
      assert topic_result.replication_factor == 3
      assert length(topic_result.configs) == 1
    end
  end

  describe "Any fallback Response implementation" do
    test "routes V5-style response with num_partitions to V5 path" do
      # Simulate a future V6 response with V5+ fields
      future_response = %{
        throttle_time_ms: 5,
        topics: [
          %{
            name: "any-topic",
            error_code: 0,
            error_message: nil,
            num_partitions: 4,
            replication_factor: 2,
            configs: [
              %{
                name: "retention.ms",
                value: "100000",
                read_only: false,
                config_source: 1,
                is_sensitive: false
              }
            ]
          }
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(future_response)
      assert result.throttle_time_ms == 5

      [topic_result] = result.topic_results
      assert topic_result.topic == "any-topic"
      assert topic_result.num_partitions == 4
      assert topic_result.replication_factor == 2
      assert length(topic_result.configs) == 1
    end

    test "routes V2+-style response (with throttle, no V5 fields) to V2+ path" do
      future_response = %{
        throttle_time_ms: 15,
        topics: [
          %{name: "any-topic", error_code: 0, error_message: nil}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(future_response)
      assert result.throttle_time_ms == 15

      [topic_result] = result.topic_results
      assert topic_result.topic == "any-topic"
      assert topic_result.error == :no_error
      assert topic_result.error_message == nil
    end

    test "routes V1-style response (error_message, no throttle) to V1 path" do
      future_response = %{
        topics: [
          %{name: "any-topic", error_code: 36, error_message: "Already exists"}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(future_response)
      assert result.throttle_time_ms == nil

      [topic_result] = result.topic_results
      assert topic_result.error == :topic_already_exists
      assert topic_result.error_message == "Already exists"
    end

    test "routes V0-style response (no error_message, no throttle) to V0 path" do
      future_response = %{
        topics: [
          %{name: "any-topic", error_code: 0}
        ]
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(future_response)
      assert result.throttle_time_ms == nil

      [topic_result] = result.topic_results
      assert topic_result.error == :no_error
      assert topic_result.error_message == nil
    end

    test "handles empty topic list in Any fallback" do
      future_response = %{
        throttle_time_ms: 0,
        topics: []
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(future_response)
      assert result.topic_results == []
      assert result.throttle_time_ms == 0
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

    test "handles nil configs in V5" do
      response = %Kayrock.CreateTopics.V5.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "t",
            error_code: 0,
            error_message: nil,
            num_partitions: 1,
            replication_factor: 1,
            configs: nil,
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      [topic_result] = result.topic_results
      assert topic_result.configs == nil
    end

    test "handles empty configs list in V5" do
      response = %Kayrock.CreateTopics.V5.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "t",
            error_code: 0,
            error_message: nil,
            num_partitions: 1,
            replication_factor: 1,
            configs: [],
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert {:ok, result} = CreateTopics.Response.parse_response(response)
      [topic_result] = result.topic_results
      assert topic_result.configs == []
    end
  end
end
