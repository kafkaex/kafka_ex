defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseHelpers
  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.Offset

  describe "parse_response/1" do
    test "parses successful response with single topic and partition" do
      response = %{
        topics: [
          %{
            name: "test-topic",
            partitions: [%{partition_index: 0, error_code: 0}]
          }
        ]
      }

      assert {:ok, [%Offset{} = offset]} = ResponseHelpers.parse_response(response)
      assert offset.topic == "test-topic"
      [partition] = offset.partition_offsets
      assert partition.partition == 0
      assert partition.error_code == :no_error
    end

    test "parses response with multiple topics" do
      response = %{
        topics: [
          %{
            name: "topic1",
            partitions: [%{partition_index: 0, error_code: 0}]
          },
          %{
            name: "topic2",
            partitions: [%{partition_index: 0, error_code: 0}]
          }
        ]
      }

      assert {:ok, offsets} = ResponseHelpers.parse_response(response)
      assert length(offsets) == 2
      topics = Enum.map(offsets, & &1.topic)
      assert "topic1" in topics
      assert "topic2" in topics
    end

    test "parses response with multiple partitions" do
      response = %{
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, offsets} = ResponseHelpers.parse_response(response)
      assert length(offsets) == 2
    end

    test "returns error for non-zero error code" do
      response = %{
        topics: [
          %{
            name: "test-topic",
            partitions: [%{partition_index: 0, error_code: 25}]
          }
        ]
      }

      assert {:error, %Error{metadata: metadata}} = ResponseHelpers.parse_response(response)
      assert metadata.topic == "test-topic"
      assert metadata.partition == 0
    end

    test "returns error on first partition with error" do
      response = %{
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 3}
            ]
          }
        ]
      }

      assert {:error, %Error{metadata: metadata}} = ResponseHelpers.parse_response(response)
      assert metadata.topic == "test-topic"
      assert metadata.partition == 1
    end
  end
end
