defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseHelpers
  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.Offset

  describe "parse_response_without_top_level_error/1" do
    test "parses successful response" do
      response = %{
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, error_code: 0, committed_offset: 100, metadata: "meta"}
            ]
          }
        ]
      }

      assert {:ok, [%Offset{} = offset]} = ResponseHelpers.parse_response_without_top_level_error(response)
      assert offset.topic == "test-topic"
      [partition] = offset.partition_offsets
      assert partition.partition == 0
      assert partition.offset == 100
      assert partition.metadata == "meta"
      assert partition.error_code == :no_error
    end

    test "handles nil metadata" do
      response = %{
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, error_code: 0, committed_offset: 50, metadata: nil}
            ]
          }
        ]
      }

      assert {:ok, [offset]} = ResponseHelpers.parse_response_without_top_level_error(response)
      [partition] = offset.partition_offsets
      assert partition.metadata == ""
    end

    test "parses response with multiple topics" do
      response = %{
        topics: [
          %{
            name: "topic1",
            partitions: [%{partition_index: 0, error_code: 0, committed_offset: 100, metadata: ""}]
          },
          %{
            name: "topic2",
            partitions: [%{partition_index: 0, error_code: 0, committed_offset: 200, metadata: ""}]
          }
        ]
      }

      assert {:ok, offsets} = ResponseHelpers.parse_response_without_top_level_error(response)
      assert length(offsets) == 2
    end

    test "returns error for partition-level error" do
      response = %{
        topics: [
          %{
            name: "test-topic",
            partitions: [%{partition_index: 0, error_code: 3, committed_offset: -1, metadata: ""}]
          }
        ]
      }

      assert {:error, %Error{metadata: metadata}} = ResponseHelpers.parse_response_without_top_level_error(response)
      assert metadata.topic == "test-topic"
      assert metadata.partition == 0
    end
  end

  describe "parse_response_with_top_level_error/1" do
    test "returns error for non-zero top-level error_code" do
      response = %{error_code: 16, responses: []}

      assert {:error, %Error{}} = ResponseHelpers.parse_response_with_top_level_error(response)
    end

    test "parses successful response when error_code is 0" do
      response = %{
        error_code: 0,
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, error_code: 0, committed_offset: 100, metadata: ""}
            ]
          }
        ]
      }

      assert {:ok, [%Offset{}]} = ResponseHelpers.parse_response_with_top_level_error(response)
    end

    test "returns partition error even when top-level error is 0" do
      response = %{
        error_code: 0,
        topics: [
          %{
            name: "test-topic",
            partitions: [%{partition_index: 0, error_code: 3, committed_offset: -1, metadata: ""}]
          }
        ]
      }

      assert {:error, %Error{metadata: metadata}} = ResponseHelpers.parse_response_with_top_level_error(response)
      assert metadata.topic == "test-topic"
      assert metadata.partition == 0
    end
  end
end
