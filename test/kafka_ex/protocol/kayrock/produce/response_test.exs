defmodule KafkaEx.Protocol.Kayrock.Produce.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Produce.Response
  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.RecordMetadata

  # Note: ResponseHelpers functions (extract_first_partition_response, check_error)
  # are tested in response_helpers_test.exs. This file focuses on Response protocol impls.

  describe "V0 Response implementation" do
    test "parses successful V0 response" do
      response = %Kayrock.Produce.V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 42}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "test-topic"
      assert produce.partition == 0
      assert produce.base_offset == 42
      assert produce.log_append_time == nil
      assert produce.throttle_time_ms == nil
      assert produce.log_start_offset == nil
    end

    test "parses V0 response with error" do
      response = %Kayrock.Produce.V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              # 7 = request_timed_out
              %{partition: 0, error_code: 7, base_offset: -1}
            ]
          }
        ]
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :request_timed_out
    end

    test "returns error for empty responses" do
      response = %Kayrock.Produce.V0.Response{responses: []}

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :empty_response
    end
  end

  describe "V1 Response implementation" do
    test "parses successful V1 response with throttle_time_ms" do
      response = %Kayrock.Produce.V1.Response{
        throttle_time_ms: 50,
        responses: [
          %{
            topic: "events",
            partition_responses: [
              %{partition: 1, error_code: 0, base_offset: 999}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "events"
      assert produce.partition == 1
      assert produce.base_offset == 999
      assert produce.throttle_time_ms == 50
      assert produce.log_append_time == nil
    end

    test "parses V1 response with zero throttle_time_ms" do
      response = %Kayrock.Produce.V1.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 100}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.throttle_time_ms == 0
    end
  end

  describe "V2 Response implementation" do
    test "parses successful V2 response with log_append_time" do
      response = %Kayrock.Produce.V2.Response{
        throttle_time_ms: 10,
        responses: [
          %{
            topic: "logs",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 500, log_append_time: 1_702_000_000_000}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "logs"
      assert produce.base_offset == 500
      assert produce.log_append_time == 1_702_000_000_000
      assert produce.throttle_time_ms == 10
    end

    test "parses V2 response with -1 log_append_time (CreateTime)" do
      response = %Kayrock.Produce.V2.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 100, log_append_time: -1}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.log_append_time == -1
    end
  end

  describe "V3 Response implementation" do
    test "parses successful V3 response with throttle_time_ms" do
      response = %Kayrock.Produce.V3.Response{
        throttle_time_ms: 10,
        responses: [
          %{
            topic: "transactions",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 1000, log_append_time: 1_702_000_000_000}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "transactions"
      assert produce.base_offset == 1000
      assert produce.log_append_time == 1_702_000_000_000
      assert produce.throttle_time_ms == 10
      assert produce.log_start_offset == nil
    end

    test "parses V3 response with zero throttle_time_ms" do
      response = %Kayrock.Produce.V3.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 50, log_append_time: -1}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.throttle_time_ms == 0
    end

    test "parses V3 response with error" do
      response = %Kayrock.Produce.V3.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              # 1 = offset_out_of_range
              %{partition: 0, error_code: 1, base_offset: -1, log_append_time: -1}
            ]
          }
        ]
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :offset_out_of_range
    end
  end

  describe "V4 Response implementation" do
    test "parses successful V4 response (same schema as V3)" do
      response = %Kayrock.Produce.V4.Response{
        throttle_time_ms: 25,
        responses: [
          %{
            topic: "v4-topic",
            partition_responses: [
              %{partition: 2, error_code: 0, base_offset: 2000, log_append_time: 1_702_100_000_000}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "v4-topic"
      assert produce.partition == 2
      assert produce.base_offset == 2000
      assert produce.log_append_time == 1_702_100_000_000
      assert produce.throttle_time_ms == 25
      assert produce.log_start_offset == nil
    end

    test "parses V4 response with error" do
      response = %Kayrock.Produce.V4.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              # 6 = not_leader_for_partition
              %{partition: 0, error_code: 6, base_offset: -1, log_append_time: -1}
            ]
          }
        ]
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :not_leader_for_partition
    end
  end

  describe "V5 Response implementation" do
    test "parses successful V5 response with log_start_offset" do
      response = %Kayrock.Produce.V5.Response{
        throttle_time_ms: 15,
        responses: [
          %{
            topic: "v5-topic",
            partition_responses: [
              %{
                partition: 3,
                error_code: 0,
                base_offset: 5000,
                log_append_time: 1_702_200_000_000,
                log_start_offset: 1000
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "v5-topic"
      assert produce.partition == 3
      assert produce.base_offset == 5000
      assert produce.log_append_time == 1_702_200_000_000
      assert produce.throttle_time_ms == 15
      assert produce.log_start_offset == 1000
    end

    test "parses V5 response with zero log_start_offset" do
      response = %Kayrock.Produce.V5.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{
                partition: 0,
                error_code: 0,
                base_offset: 100,
                log_append_time: -1,
                log_start_offset: 0
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.log_start_offset == 0
    end

    test "parses V5 response with error" do
      response = %Kayrock.Produce.V5.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              # 19 = not_enough_replicas
              %{
                partition: 0,
                error_code: 19,
                base_offset: -1,
                log_append_time: -1,
                log_start_offset: -1
              }
            ]
          }
        ]
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :not_enough_replicas
    end
  end

  describe "V6 Response implementation" do
    test "parses successful V6 response (same schema as V5)" do
      response = %Kayrock.Produce.V6.Response{
        throttle_time_ms: 20,
        responses: [
          %{
            topic: "v6-topic",
            partition_responses: [
              %{
                partition: 0,
                error_code: 0,
                base_offset: 6000,
                log_append_time: 1_702_300_000_000,
                log_start_offset: 500
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "v6-topic"
      assert produce.partition == 0
      assert produce.base_offset == 6000
      assert produce.log_append_time == 1_702_300_000_000
      assert produce.log_start_offset == 500
      assert produce.throttle_time_ms == 20
    end

    test "parses V6 response with -1 log_append_time (CreateTime)" do
      response = %Kayrock.Produce.V6.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{
                partition: 0,
                error_code: 0,
                base_offset: 200,
                log_append_time: -1,
                log_start_offset: 0
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.log_append_time == -1
    end

    test "parses V6 response with error" do
      response = %Kayrock.Produce.V6.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              # 7 = request_timed_out
              %{
                partition: 0,
                error_code: 7,
                base_offset: -1,
                log_append_time: -1,
                log_start_offset: -1
              }
            ]
          }
        ]
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :request_timed_out
    end

    test "returns error for empty responses" do
      response = %Kayrock.Produce.V6.Response{throttle_time_ms: 0, responses: []}

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :empty_response
    end
  end

  describe "V7 Response implementation" do
    test "parses successful V7 response (same schema as V5/V6)" do
      response = %Kayrock.Produce.V7.Response{
        throttle_time_ms: 30,
        responses: [
          %{
            topic: "v7-topic",
            partition_responses: [
              %{
                partition: 1,
                error_code: 0,
                base_offset: 7000,
                log_append_time: 1_702_400_000_000,
                log_start_offset: 1000
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "v7-topic"
      assert produce.partition == 1
      assert produce.base_offset == 7000
      assert produce.log_append_time == 1_702_400_000_000
      assert produce.log_start_offset == 1000
      assert produce.throttle_time_ms == 30
    end

    test "parses V7 response with zero throttle_time_ms" do
      response = %Kayrock.Produce.V7.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{
                partition: 0,
                error_code: 0,
                base_offset: 100,
                log_append_time: -1,
                log_start_offset: 0
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.throttle_time_ms == 0
    end

    test "parses V7 response with error" do
      response = %Kayrock.Produce.V7.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              # 6 = not_leader_for_partition
              %{
                partition: 0,
                error_code: 6,
                base_offset: -1,
                log_append_time: -1,
                log_start_offset: -1
              }
            ]
          }
        ]
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :not_leader_for_partition
    end

    test "returns error for empty responses" do
      response = %Kayrock.Produce.V7.Response{throttle_time_ms: 0, responses: []}

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :empty_response
    end
  end

  describe "V8 Response implementation" do
    test "parses successful V8 response with all fields" do
      response = %Kayrock.Produce.V8.Response{
        throttle_time_ms: 5,
        responses: [
          %{
            topic: "v8-topic",
            partition_responses: [
              %{
                partition: 2,
                error_code: 0,
                base_offset: 8000,
                log_append_time: 1_702_500_000_000,
                log_start_offset: 2000,
                record_errors: [],
                error_message: nil
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "v8-topic"
      assert produce.partition == 2
      assert produce.base_offset == 8000
      assert produce.log_append_time == 1_702_500_000_000
      assert produce.log_start_offset == 2000
      assert produce.throttle_time_ms == 5
    end

    test "parses V8 response with zero offsets" do
      response = %Kayrock.Produce.V8.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{
                partition: 0,
                error_code: 0,
                base_offset: 0,
                log_append_time: -1,
                log_start_offset: 0,
                record_errors: [],
                error_message: nil
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.base_offset == 0
      assert produce.log_start_offset == 0
    end

    test "parses V8 response with error (record_errors and error_message present)" do
      response = %Kayrock.Produce.V8.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{
                partition: 0,
                # 87 = invalid_record (example error that triggers record_errors)
                error_code: 87,
                base_offset: -1,
                log_append_time: -1,
                log_start_offset: -1,
                record_errors: [
                  %{batch_index: 0, batch_index_error_message: "Invalid timestamp"},
                  %{batch_index: 2, batch_index_error_message: "Record too large"}
                ],
                error_message: "One or more records in the batch were invalid"
              }
            ]
          }
        ]
      }

      # The error path returns an error based on the error code;
      # record_errors and error_message are not exposed in the Error struct
      assert {:error, %Error{} = error} = Response.parse_response(response)
      assert error.error == :invalid_record
    end

    test "parses V8 response with standard error code" do
      response = %Kayrock.Produce.V8.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              # 19 = not_enough_replicas
              %{
                partition: 0,
                error_code: 19,
                base_offset: -1,
                log_append_time: -1,
                log_start_offset: -1,
                record_errors: [],
                error_message: nil
              }
            ]
          }
        ]
      }

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :not_enough_replicas
    end

    test "returns error for empty responses" do
      response = %Kayrock.Produce.V8.Response{throttle_time_ms: 0, responses: []}

      assert {:error, error} = Response.parse_response(response)
      assert error.error == :empty_response
    end
  end

  describe "Any fallback response implementation (forward compatibility)" do
    # The @fallback_to_any true on the Response protocol means any struct type
    # without an explicit implementation gets the Any fallback, which dynamically
    # checks for available fields.

    defmodule FakeV9Response do
      defstruct [:throttle_time_ms, :responses]
    end

    defmodule FakeMinimalResponse do
      defstruct [:responses]
    end

    test "Any fallback extracts all available fields" do
      response = %FakeV9Response{
        throttle_time_ms: 42,
        responses: [
          %{
            topic: "future-topic",
            partition_responses: [
              %{
                partition: 0,
                error_code: 0,
                base_offset: 9000,
                log_append_time: 1_702_600_000_000,
                log_start_offset: 3000
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "future-topic"
      assert produce.base_offset == 9000
      assert produce.log_append_time == 1_702_600_000_000
      assert produce.log_start_offset == 3000
      assert produce.throttle_time_ms == 42
    end

    test "Any fallback handles response without throttle_time_ms" do
      response = %FakeMinimalResponse{
        responses: [
          %{
            topic: "minimal-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 100}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = Response.parse_response(response)
      assert produce.topic == "minimal-topic"
      assert produce.base_offset == 100
      assert produce.throttle_time_ms == nil
    end

    test "Any fallback handles error code from unknown struct" do
      response = %FakeV9Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{partition: 0, error_code: 7, base_offset: -1}
            ]
          }
        ]
      }

      assert {:error, %Error{} = error} = Response.parse_response(response)
      assert error.error == :request_timed_out
    end
  end

  describe "parse_response via KayrockProtocol" do
    alias KafkaEx.Protocol.KayrockProtocol

    test "dispatches V0 response through parse_response" do
      response = %Kayrock.Produce.V0.Response{
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 42}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = KayrockProtocol.parse_response(:produce, response)
      assert produce.base_offset == 42
    end

    test "dispatches V3 response through parse_response" do
      response = %Kayrock.Produce.V3.Response{
        throttle_time_ms: 10,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 100, log_append_time: -1}
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = KayrockProtocol.parse_response(:produce, response)
      assert produce.throttle_time_ms == 10
    end

    test "dispatches V5 response through parse_response" do
      response = %Kayrock.Produce.V5.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{
                partition: 0,
                error_code: 0,
                base_offset: 200,
                log_append_time: -1,
                log_start_offset: 50
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = KayrockProtocol.parse_response(:produce, response)
      assert produce.log_start_offset == 50
    end

    test "dispatches V8 response through parse_response" do
      response = %Kayrock.Produce.V8.Response{
        throttle_time_ms: 5,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              %{
                partition: 0,
                error_code: 0,
                base_offset: 300,
                log_append_time: -1,
                log_start_offset: 100,
                record_errors: [],
                error_message: nil
              }
            ]
          }
        ]
      }

      assert {:ok, %RecordMetadata{} = produce} = KayrockProtocol.parse_response(:produce, response)
      assert produce.base_offset == 300
      assert produce.throttle_time_ms == 5
    end
  end

  describe "consistency across versions" do
    test "V0-V8 all produce consistent base fields for equivalent data" do
      # Build equivalent responses for each version, all with the same
      # topic, partition, and base_offset
      base_partition = %{partition: 0, error_code: 0, base_offset: 1000}

      v0_resp = %Kayrock.Produce.V0.Response{
        responses: [%{topic: "test", partition_responses: [base_partition]}]
      }

      v1_resp = %Kayrock.Produce.V1.Response{
        throttle_time_ms: 0,
        responses: [%{topic: "test", partition_responses: [base_partition]}]
      }

      v2_resp = %Kayrock.Produce.V2.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [Map.put(base_partition, :log_append_time, -1)]
          }
        ]
      }

      v5_resp = %Kayrock.Produce.V5.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              base_partition |> Map.put(:log_append_time, -1) |> Map.put(:log_start_offset, 0)
            ]
          }
        ]
      }

      v8_resp = %Kayrock.Produce.V8.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test",
            partition_responses: [
              base_partition
              |> Map.put(:log_append_time, -1)
              |> Map.put(:log_start_offset, 0)
              |> Map.put(:record_errors, [])
              |> Map.put(:error_message, nil)
            ]
          }
        ]
      }

      {:ok, r0} = Response.parse_response(v0_resp)
      {:ok, r1} = Response.parse_response(v1_resp)
      {:ok, r2} = Response.parse_response(v2_resp)
      {:ok, r5} = Response.parse_response(v5_resp)
      {:ok, r8} = Response.parse_response(v8_resp)

      # All versions produce the same core fields
      for r <- [r0, r1, r2, r5, r8] do
        assert %RecordMetadata{} = r
        assert r.topic == "test"
        assert r.partition == 0
        assert r.base_offset == 1000
      end

      # V0 has no throttle_time_ms
      assert r0.throttle_time_ms == nil
      # V1+ have throttle_time_ms
      assert r1.throttle_time_ms == 0
      assert r2.throttle_time_ms == 0

      # V0-V1 have no log_append_time
      assert r0.log_append_time == nil
      assert r1.log_append_time == nil
      # V2+ have log_append_time
      assert r2.log_append_time == -1

      # V0-V4 have no log_start_offset
      assert r0.log_start_offset == nil
      assert r1.log_start_offset == nil
      assert r2.log_start_offset == nil
      # V5+ have log_start_offset
      assert r5.log_start_offset == 0
      assert r8.log_start_offset == 0
    end
  end
end
