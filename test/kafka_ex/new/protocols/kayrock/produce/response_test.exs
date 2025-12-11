defmodule KafkaEx.New.Protocols.Kayrock.Produce.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.Produce.Response
  alias KafkaEx.New.Protocols.Kayrock.Produce.ResponseHelpers
  alias KafkaEx.New.Structs.Error

  describe "ResponseHelpers.extract_first_partition_response/1" do
    test "extracts first topic and partition" do
      response = %{
        responses: [
          %{
            topic: "first-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, base_offset: 10}
            ]
          }
        ]
      }

      assert {:ok, "first-topic", partition_response} =
               ResponseHelpers.extract_first_partition_response(response)

      assert partition_response.partition == 0
      assert partition_response.base_offset == 10
    end

    test "returns error for empty responses" do
      assert {:error, :empty_response} =
               ResponseHelpers.extract_first_partition_response(%{responses: []})
    end

    test "returns error for missing responses key" do
      assert {:error, :empty_response} =
               ResponseHelpers.extract_first_partition_response(%{})
    end
  end

  describe "ResponseHelpers.check_error/2" do
    test "returns ok for no error" do
      partition_resp = %{partition: 0, error_code: 0, base_offset: 100}

      assert {:ok, ^partition_resp} = ResponseHelpers.check_error("test-topic", partition_resp)
    end

    test "returns error for non-zero error code" do
      partition_resp = %{partition: 0, error_code: 3, base_offset: -1}

      assert {:error, error} = ResponseHelpers.check_error("test-topic", partition_resp)
      assert %Error{} = error
      assert error.error == :unknown_topic_or_partition
    end
  end

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

      assert {:ok, produce} = Response.parse_response(response)
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

      assert {:ok, produce} = Response.parse_response(response)
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

      assert {:ok, produce} = Response.parse_response(response)
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

      assert {:ok, produce} = Response.parse_response(response)
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

      assert {:ok, produce} = Response.parse_response(response)
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

      assert {:ok, produce} = Response.parse_response(response)
      assert produce.topic == "transactions"
      assert produce.base_offset == 1000
      assert produce.log_append_time == 1_702_000_000_000
      assert produce.throttle_time_ms == 10
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

      assert {:ok, produce} = Response.parse_response(response)
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

      assert {:ok, produce} = Response.parse_response(response)
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

      assert {:ok, produce} = Response.parse_response(response)
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

      assert {:ok, produce} = Response.parse_response(response)
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
end
