defmodule KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ListOffsets.ResponseHelpers
  alias KafkaEx.Messages.Offset

  describe "extract_v0_offset/2" do
    test "extracts first offset from offsets array" do
      partition_resp = %{partition: 0, error_code: 0, offsets: [100, 50, 0]}

      assert {:ok, %Offset{} = offset} = ResponseHelpers.extract_v0_offset("test-topic", partition_resp)

      assert offset.topic == "test-topic"
      [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == 100
      assert partition_offset.error_code == :no_error
    end

    test "returns 0 for empty offsets array" do
      partition_resp = %{partition: 0, error_code: 0, offsets: []}

      assert {:ok, %Offset{} = offset} = ResponseHelpers.extract_v0_offset("test-topic", partition_resp)

      [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 0
    end

    test "returns error for non-zero error code" do
      partition_resp = %{partition: 0, error_code: 3, offsets: []}

      assert {:error, {3, "test-topic", 0}} = ResponseHelpers.extract_v0_offset("test-topic", partition_resp)
    end
  end

  describe "extract_v1_offset/2" do
    test "extracts single offset field" do
      partition_resp = %{partition: 0, error_code: 0, offset: 100}

      assert {:ok, %Offset{} = offset} = ResponseHelpers.extract_v1_offset("test-topic", partition_resp)

      [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == 100
      assert partition_offset.error_code == :no_error
    end

    test "returns error for non-zero error code" do
      partition_resp = %{partition: 1, error_code: 3, offset: -1}

      assert {:error, {3, "test-topic", 1}} = ResponseHelpers.extract_v1_offset("test-topic", partition_resp)
    end
  end

  describe "extract_v2_offset/2" do
    test "extracts offset with timestamp" do
      partition_resp = %{partition: 0, error_code: 0, offset: 100, timestamp: 1_234_567_890}

      assert {:ok, %Offset{} = offset} = ResponseHelpers.extract_v2_offset("test-topic", partition_resp)

      [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == 100
      assert partition_offset.timestamp == 1_234_567_890
      assert partition_offset.error_code == :no_error
    end

    test "returns error for non-zero error code" do
      partition_resp = %{partition: 2, error_code: 5, offset: -1, timestamp: -1}

      assert {:error, {5, "test-topic", 2}} = ResponseHelpers.extract_v2_offset("test-topic", partition_resp)
    end
  end

  describe "parse_response/2" do
    test "parses response with single topic and partition" do
      response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [%{partition: 0, error_code: 0, offset: 100}]
          }
        ]
      }

      extractor = &ResponseHelpers.extract_v1_offset/2

      assert {:ok, [%Offset{} = offset]} = ResponseHelpers.parse_response(response, extractor)
      assert offset.topic == "test-topic"
    end

    test "parses response with multiple topics" do
      response = %{
        responses: [
          %{
            topic: "topic1",
            partition_responses: [%{partition: 0, error_code: 0, offset: 100}]
          },
          %{
            topic: "topic2",
            partition_responses: [%{partition: 0, error_code: 0, offset: 200}]
          }
        ]
      }

      extractor = &ResponseHelpers.extract_v1_offset/2

      assert {:ok, offsets} = ResponseHelpers.parse_response(response, extractor)
      assert length(offsets) == 2

      topics = Enum.map(offsets, & &1.topic)
      assert "topic1" in topics
      assert "topic2" in topics
    end

    test "returns error on first partition error" do
      response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 100},
              %{partition: 1, error_code: 3, offset: -1}
            ]
          }
        ]
      }

      extractor = &ResponseHelpers.extract_v1_offset/2

      assert {:error, _} = ResponseHelpers.parse_response(response, extractor)
    end
  end
end
