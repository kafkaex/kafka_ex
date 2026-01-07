defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetFetch
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.Offset.PartitionOffset

  describe "V0 Response implementation" do
    test "parses successful response with single partition" do
      response = %Kayrock.OffsetFetch.V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, offset: 42, metadata: "consumer-1", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "test-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 42,
                   metadata: "consumer-1",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, offset: 100, metadata: nil, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetFetch.V0.Response{
        responses: [
          %{
            topic: "multi-partition",
            partition_responses: [
              %{partition: 0, offset: 10, metadata: "meta-0", error_code: 0},
              %{partition: 1, offset: 20, metadata: "meta-1", error_code: 0},
              %{partition: 2, offset: 30, metadata: "meta-2", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 3

      # Results are accumulated in reverse order
      assert Enum.at(results, 2).partition_offsets |> hd() |> Map.get(:partition) == 0
      assert Enum.at(results, 1).partition_offsets |> hd() |> Map.get(:partition) == 1
      assert Enum.at(results, 0).partition_offsets |> hd() |> Map.get(:partition) == 2
    end

    test "parses response with multiple topics" do
      response = %Kayrock.OffsetFetch.V0.Response{
        responses: [
          %{
            topic: "topic-1",
            partition_responses: [
              %{partition: 0, offset: 100, metadata: "", error_code: 0}
            ]
          },
          %{
            topic: "topic-2",
            partition_responses: [
              %{partition: 0, offset: 200, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 2

      # Results are accumulated in reverse order
      assert Enum.at(results, 1).topic == "topic-1"
      assert Enum.at(results, 0).topic == "topic-2"
    end

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetFetch.V0.Response{
        responses: [
          %{
            topic: "error-topic",
            partition_responses: [
              %{partition: 0, offset: -1, metadata: "", error_code: 3}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :unknown_topic_or_partition
      assert error.metadata.topic == "error-topic"
      assert error.metadata.partition == 0
    end

    test "returns error on first partition error with multiple partitions" do
      response = %Kayrock.OffsetFetch.V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, offset: 100, metadata: "", error_code: 0},
              %{partition: 1, offset: -1, metadata: "", error_code: 15},
              %{partition: 2, offset: 200, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :coordinator_not_available
      assert error.metadata.partition == 1
    end

    test "parses response with offset -1 (no committed offset)" do
      response = %Kayrock.OffsetFetch.V0.Response{
        responses: [
          %{
            topic: "new-consumer-topic",
            partition_responses: [
              %{partition: 0, offset: -1, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.offset == -1
      assert partition_offset.error_code == :no_error
    end
  end

  describe "V1 Response implementation" do
    test "parses successful response with single partition" do
      response = %Kayrock.OffsetFetch.V1.Response{
        responses: [
          %{
            topic: "v1-topic",
            partition_responses: [
              %{partition: 0, offset: 999, metadata: "v1-consumer", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "v1-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 999,
                   metadata: "v1-consumer",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V1.Response{
        responses: [
          %{
            topic: "no-metadata-topic",
            partition_responses: [
              %{partition: 0, offset: 500, metadata: nil, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetFetch.V1.Response{
        responses: [
          %{
            topic: "multi-part-v1",
            partition_responses: [
              %{partition: 0, offset: 100, metadata: "part-0", error_code: 0},
              %{partition: 1, offset: 200, metadata: "part-1", error_code: 0},
              %{partition: 2, offset: 300, metadata: "part-2", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 3

      # Results are accumulated in reverse order
      assert Enum.at(results, 2).partition_offsets |> hd() |> Map.get(:offset) == 100
      assert Enum.at(results, 1).partition_offsets |> hd() |> Map.get(:offset) == 200
      assert Enum.at(results, 0).partition_offsets |> hd() |> Map.get(:offset) == 300
    end

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetFetch.V1.Response{
        responses: [
          %{
            topic: "error-v1-topic",
            partition_responses: [
              %{partition: 0, offset: -1, metadata: "", error_code: 14}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :coordinator_load_in_progress
      assert error.metadata.topic == "error-v1-topic"
      assert error.metadata.partition == 0
    end

    test "parses response with coordinator-based offset" do
      response = %Kayrock.OffsetFetch.V1.Response{
        responses: [
          %{
            topic: "coordinator-topic",
            partition_responses: [
              %{partition: 0, offset: 12_345, metadata: "coordinator-consumer-id", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.offset == 12_345
      assert partition_offset.metadata == "coordinator-consumer-id"
    end
  end

  describe "V2 Response implementation" do
    test "parses successful response with top-level error_code 0" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        responses: [
          %{
            topic: "v2-topic",
            partition_responses: [
              %{partition: 0, offset: 1000, metadata: "v2-metadata", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "v2-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 1000,
                   metadata: "v2-metadata",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "returns error when top-level error_code is non-zero" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 16,
        responses: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses response with multiple partitions when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        responses: [
          %{
            topic: "multi-v2",
            partition_responses: [
              %{partition: 0, offset: 10, metadata: "", error_code: 0},
              %{partition: 1, offset: 20, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 2
    end

    test "returns partition-level error even when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        responses: [
          %{
            topic: "partition-error-topic",
            partition_responses: [
              %{partition: 0, offset: -1, metadata: "", error_code: 9}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :replica_not_available
      assert error.metadata.partition == 0
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        responses: [
          %{
            topic: "nil-meta-v2",
            partition_responses: [
              %{partition: 0, offset: 777, metadata: nil, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
    end

    test "handles broker-level coordinator errors" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 15,
        responses: [
          %{
            topic: "some-topic",
            partition_responses: [
              %{partition: 0, offset: 100, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end
  end

  describe "V3 Response implementation" do
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
