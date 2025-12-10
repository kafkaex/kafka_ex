defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch.V0ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch
  alias KafkaEx.New.Structs.Offset
  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "parse_response/1 for V0" do
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
end
