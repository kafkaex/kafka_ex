defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch.V1ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch
  alias KafkaEx.New.Structs.Offset
  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "parse_response/1 for V1" do
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
end
