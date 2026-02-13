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

  describe "extract_v4_offset/2" do
    test "extracts offset with timestamp and leader_epoch" do
      partition_resp = %{
        partition: 0,
        error_code: 0,
        offset: 250,
        timestamp: 1_700_000_000_000,
        leader_epoch: 7
      }

      assert {:ok, %Offset{} = offset} =
               ResponseHelpers.extract_v4_offset("test-topic", partition_resp)

      assert offset.topic == "test-topic"
      [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == 250
      assert partition_offset.timestamp == 1_700_000_000_000
      assert partition_offset.error_code == :no_error
      assert partition_offset.leader_epoch == 7
    end

    test "extracts offset with leader_epoch -1 (unknown)" do
      partition_resp = %{
        partition: 3,
        error_code: 0,
        offset: 42,
        timestamp: 999,
        leader_epoch: -1
      }

      assert {:ok, %Offset{} = offset} =
               ResponseHelpers.extract_v4_offset("my-topic", partition_resp)

      [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 3
      assert partition_offset.offset == 42
      assert partition_offset.timestamp == 999
      assert partition_offset.error_code == :no_error
      assert partition_offset.leader_epoch == -1
    end

    test "extracts offset when leader_epoch is missing from map" do
      partition_resp = %{
        partition: 0,
        error_code: 0,
        offset: 100,
        timestamp: 500
      }

      assert {:ok, %Offset{} = offset} =
               ResponseHelpers.extract_v4_offset("test-topic", partition_resp)

      [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 100
      assert partition_offset.leader_epoch == nil
    end

    test "returns error for non-zero error code" do
      partition_resp = %{
        partition: 2,
        error_code: 6,
        offset: -1,
        timestamp: -1,
        leader_epoch: -1
      }

      assert {:error, {6, "test-topic", 2}} =
               ResponseHelpers.extract_v4_offset("test-topic", partition_resp)
    end

    test "returns error for non-zero error code regardless of leader_epoch" do
      partition_resp = %{
        partition: 0,
        error_code: 1,
        offset: -1,
        timestamp: -1,
        leader_epoch: 5
      }

      assert {:error, {1, "test-topic", 0}} =
               ResponseHelpers.extract_v4_offset("test-topic", partition_resp)
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

    test "parses response with v4 extractor (leader_epoch)" do
      response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 300, timestamp: 5000, leader_epoch: 2}
            ]
          }
        ]
      }

      extractor = &ResponseHelpers.extract_v4_offset/2

      assert {:ok, [%Offset{} = offset]} = ResponseHelpers.parse_response(response, extractor)
      assert offset.topic == "test-topic"
      [po] = offset.partition_offsets
      assert po.offset == 300
      assert po.timestamp == 5000
      assert po.leader_epoch == 2
    end

    test "parses response with v4 extractor and multiple partitions" do
      response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 100, timestamp: 1000, leader_epoch: 1},
              %{partition: 1, error_code: 0, offset: 200, timestamp: 2000, leader_epoch: 1}
            ]
          }
        ]
      }

      extractor = &ResponseHelpers.extract_v4_offset/2

      assert {:ok, offsets} = ResponseHelpers.parse_response(response, extractor)
      assert length(offsets) == 2
    end

    test "parses empty responses list" do
      response = %{responses: []}

      extractor = &ResponseHelpers.extract_v1_offset/2

      assert {:ok, []} = ResponseHelpers.parse_response(response, extractor)
    end

    test "returns error on first topic error with v4 extractor" do
      response = %{
        responses: [
          %{
            topic: "topic-a",
            partition_responses: [
              %{partition: 0, error_code: 6, offset: -1, timestamp: -1, leader_epoch: -1}
            ]
          },
          %{
            topic: "topic-b",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 100, timestamp: 500, leader_epoch: 1}
            ]
          }
        ]
      }

      extractor = &ResponseHelpers.extract_v4_offset/2

      assert {:error, _} = ResponseHelpers.parse_response(response, extractor)
    end
  end
end
