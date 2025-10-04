defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch.V3ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch
  alias KafkaEx.New.Structs.Offset
  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "parse_response/1 for V3" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 100,
        error_code: 0,
        responses: [
          %{
            topic: "v3-topic",
            partition_responses: [
              %{partition: 0, offset: 5000, metadata: "v3-consumer", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "v3-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 5000,
                   metadata: "v3-consumer",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses response with zero throttle_time_ms" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        responses: [
          %{
            topic: "no-throttle-topic",
            partition_responses: [
              %{partition: 0, offset: 200, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      assert result.topic == "no-throttle-topic"
    end

    test "returns error when top-level error_code is non-zero" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 50,
        error_code: 16,
        responses: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 25,
        error_code: 0,
        responses: [
          %{
            topic: "multi-v3",
            partition_responses: [
              %{partition: 0, offset: 100, metadata: "meta-0", error_code: 0},
              %{partition: 1, offset: 200, metadata: "meta-1", error_code: 0},
              %{partition: 2, offset: 300, metadata: "meta-2", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 3
    end

    test "returns partition-level error even when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 10,
        error_code: 0,
        responses: [
          %{
            topic: "partition-error-v3",
            partition_responses: [
              %{partition: 0, offset: -1, metadata: "", error_code: 3}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :unknown_topic_or_partition
      assert error.metadata.partition == 0
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        responses: [
          %{
            topic: "nil-meta-v3",
            partition_responses: [
              %{partition: 0, offset: 888, metadata: nil, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
    end

    test "handles high throttle_time_ms values" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 5000,
        error_code: 0,
        responses: [
          %{
            topic: "throttled-topic",
            partition_responses: [
              %{partition: 0, offset: 1000, metadata: "throttled", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      assert result.topic == "throttled-topic"
    end
  end
end
