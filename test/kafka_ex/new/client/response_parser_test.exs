defmodule KafkaEx.New.Client.ResponseParserTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Client.ResponseParser
  alias KafkaEx.New.Structs.Heartbeat
  alias KafkaEx.New.Structs.Offset
  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "offset_fetch_response/1" do
    test "parses successful OffsetFetch v1 response" do
      response = %Kayrock.OffsetFetch.V1.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, offset: 100, metadata: "meta", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = ResponseParser.offset_fetch_response(response)

      assert result == %Offset{
               topic: "test-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 100,
                   metadata: "meta",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses OffsetFetch v2 response with error_code" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        responses: [
          %{
            topic: "topic-a",
            partition_responses: [
              %{partition: 0, offset: 200, metadata: "", error_code: 0},
              %{partition: 1, offset: 300, metadata: "data", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = ResponseParser.offset_fetch_response(response)
      assert length(results) == 2
    end

    test "returns error for failed OffsetFetch response" do
      response = %Kayrock.OffsetFetch.V1.Response{
        responses: [
          %{
            topic: "error-topic",
            partition_responses: [
              %{partition: 0, offset: -1, metadata: "", error_code: 15}
            ]
          }
        ]
      }

      assert {:error, error} = ResponseParser.offset_fetch_response(response)
      assert error.error == :coordinator_not_available
    end
  end

  describe "offset_commit_response/1" do
    test "parses successful OffsetCommit v2 response" do
      response = %Kayrock.OffsetCommit.V2.Response{
        responses: [
          %{
            topic: "commit-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = ResponseParser.offset_commit_response(response)

      assert result == %Offset{
               topic: "commit-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   error_code: :no_error,
                   offset: nil,
                   metadata: nil,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses OffsetCommit v3 response with throttle_time_ms" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 100,
        responses: [
          %{
            topic: "throttled-topic",
            partition_responses: [
              %{partition: 0, error_code: 0},
              %{partition: 1, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = ResponseParser.offset_commit_response(response)
      assert length(results) == 2
    end

    test "returns error for failed OffsetCommit response" do
      response = %Kayrock.OffsetCommit.V2.Response{
        responses: [
          %{
            topic: "error-topic",
            partition_responses: [
              %{partition: 0, error_code: 16}
            ]
          }
        ]
      }

      assert {:error, error} = ResponseParser.offset_commit_response(response)
      assert error.error == :not_coordinator
    end
  end

  describe "heartbeat_response/1" do
    test "parses successful Heartbeat v0 response" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 0
      }

      assert {:ok, :no_error} = ResponseParser.heartbeat_response(response)
    end

    test "parses successful Heartbeat v1 response with throttle_time_ms" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 0,
        throttle_time_ms: 100
      }

      assert {:ok, heartbeat} = ResponseParser.heartbeat_response(response)
      assert %Heartbeat{throttle_time_ms: 100} = heartbeat
    end

    test "parses Heartbeat v1 response with zero throttle_time_ms" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 0,
        throttle_time_ms: 0
      }

      assert {:ok, heartbeat} = ResponseParser.heartbeat_response(response)
      assert heartbeat.throttle_time_ms == 0
    end

    test "returns error for unknown_member_id (v0)" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 25
      }

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :unknown_member_id
    end

    test "returns error for illegal_generation (v0)" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 22
      }

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :illegal_generation
    end

    test "returns error for rebalance_in_progress (v1)" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 27,
        throttle_time_ms: 50
      }

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "returns error for not_coordinator (v1)" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 16,
        throttle_time_ms: 0
      }

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :not_coordinator
    end

    test "returns error for coordinator_not_available (v0)" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 15
      }

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :coordinator_not_available
    end

    test "handles generic error with unknown error code (v0)" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 999
      }

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :unknown
    end

    test "handles generic error with unknown error code (v1)" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 999,
        throttle_time_ms: 10
      }

      assert {:error, error} = ResponseParser.heartbeat_response(response)
      assert error.error == :unknown
    end
  end
end
