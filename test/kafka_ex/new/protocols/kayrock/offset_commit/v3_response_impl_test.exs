defmodule KafkaEx.New.Protocols.Kayrock.OffsetCommit.V3ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetCommit
  alias KafkaEx.New.Structs.Offset
  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "parse_response/1 for V3" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 100,
        responses: [
          %{
            topic: "v3-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "v3-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   error_code: :no_error,
                   offset: nil,
                   timestamp: nil,
                   metadata: nil
                 }
               ]
             }
    end

    test "parses response with zero throttle_time_ms" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "no-throttle-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)
      assert result.topic == "no-throttle-topic"
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 50,
        responses: [
          %{
            topic: "multi-v3",
            partition_responses: [
              %{partition: 0, error_code: 0},
              %{partition: 1, error_code: 0},
              %{partition: 2, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 3
    end

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 25,
        responses: [
          %{
            topic: "error-v3-topic",
            partition_responses: [
              %{partition: 0, error_code: 14}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :coordinator_load_in_progress
      assert error.metadata.partition == 0
    end

    test "handles high throttle_time_ms values" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 5000,
        responses: [
          %{
            topic: "throttled-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)
      assert result.topic == "throttled-topic"
    end

    test "handles multiple topics with throttle time" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 150,
        responses: [
          %{
            topic: "topic-1",
            partition_responses: [%{partition: 0, error_code: 0}]
          },
          %{
            topic: "topic-2",
            partition_responses: [%{partition: 0, error_code: 0}]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 2
    end
  end
end
