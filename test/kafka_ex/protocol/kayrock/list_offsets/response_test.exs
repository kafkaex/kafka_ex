defmodule KafkaEx.Protocol.Kayrock.ListOffsets.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ListOffsets.Response, as: ListOffsetsResponse

  alias Kayrock.ListOffsets.V0
  alias Kayrock.ListOffsets.V1
  alias Kayrock.ListOffsets.V2
  alias Kayrock.ListOffsets.V3
  alias Kayrock.ListOffsets.V4
  alias Kayrock.ListOffsets.V5

  describe "parse_response/1" do
    @expected_offset %KafkaEx.Messages.Offset{
      topic: "test-topic",
      partition_offsets: [
        %KafkaEx.Messages.Offset.PartitionOffset{
          partition: 1,
          error_code: :no_error,
          offset: 0,
          timestamp: -1
        }
      ]
    }

    test "for api version 0 - returns response if all groups succeeded" do
      response = %V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [%{partition: 1, error_code: 0, offsets: [0]}]
          }
        ],
        correlation_id: 3
      }

      assert {:ok, [@expected_offset]} == ListOffsetsResponse.parse_response(response)
    end

    test "for api version 0 - returns error if any group failed" do
      response = %V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [%{partition: 0, error_code: 11, offsets: []}]
          }
        ],
        correlation_id: 3
      }

      expected_error = %KafkaEx.Client.Error{
        error: :stale_controller_epoch,
        metadata: %{partition: 0, topic: "test-topic"}
      }

      assert {:error, expected_error} == ListOffsetsResponse.parse_response(response)
    end

    test "for api version 1 - returns response if all groups succeeded" do
      response = %V1.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: 0, partition: 1, error_code: 0}
            ]
          }
        ],
        correlation_id: 3
      }

      assert {:ok, [@expected_offset]} == ListOffsetsResponse.parse_response(response)
    end

    test "for api version 1 - returns error if any group failed" do
      response = %V1.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: 0, partition: 1, error_code: 1, timestamp: 1_000_000_000_000}
            ]
          }
        ],
        correlation_id: 3
      }

      expected_error = %KafkaEx.Client.Error{
        error: :offset_out_of_range,
        metadata: %{partition: 1, topic: "test-topic"}
      }

      assert {:error, expected_error} == ListOffsetsResponse.parse_response(response)
    end

    test "for api version 2 - returns response if all groups succeeded" do
      response = %V2.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: 0, partition: 1, error_code: 0, timestamp: 1_000_000_000_000}
            ]
          }
        ],
        correlation_id: 3
      }

      p_offset = @expected_offset.partition_offsets |> hd() |> Map.put(:timestamp, 1_000_000_000_000)
      result_with_timestamp = Map.put(@expected_offset, :partition_offsets, [p_offset])
      assert {:ok, [result_with_timestamp]} == ListOffsetsResponse.parse_response(response)
    end

    test "for api version 2 - returns error if any group failed" do
      response = %V2.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: 0, partition: 1, error_code: 1}
            ]
          }
        ],
        correlation_id: 3
      }

      expected_error = %KafkaEx.Client.Error{
        error: :offset_out_of_range,
        metadata: %{partition: 1, topic: "test-topic"}
      }

      assert {:error, expected_error} == ListOffsetsResponse.parse_response(response)
    end

    # -------------------------------------------------------------------------
    # V3 Response: identical schema to V2 (uses extract_v2_offset/2)
    # -------------------------------------------------------------------------

    test "for api version 3 - returns response if all groups succeeded" do
      response = %V3.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: 0, partition: 1, error_code: 0, timestamp: 1_000_000_000_000}
            ]
          }
        ],
        correlation_id: 3
      }

      p_offset = @expected_offset.partition_offsets |> hd() |> Map.put(:timestamp, 1_000_000_000_000)
      result_with_timestamp = Map.put(@expected_offset, :partition_offsets, [p_offset])
      assert {:ok, [result_with_timestamp]} == ListOffsetsResponse.parse_response(response)
    end

    test "for api version 3 - returns error if any group failed" do
      response = %V3.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: 0, partition: 1, error_code: 1}
            ]
          }
        ],
        correlation_id: 3
      }

      expected_error = %KafkaEx.Client.Error{
        error: :offset_out_of_range,
        metadata: %{partition: 1, topic: "test-topic"}
      }

      assert {:error, expected_error} == ListOffsetsResponse.parse_response(response)
    end

    test "for api version 3 - handles multiple partitions with mixed results" do
      response = %V3.Response{
        throttle_time_ms: 10,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: 100, partition: 0, error_code: 0, timestamp: 500},
              %{offset: 200, partition: 1, error_code: 3}
            ]
          }
        ],
        correlation_id: 5
      }

      # Should fail fast on the first error encountered
      assert {:error, %KafkaEx.Client.Error{}} = ListOffsetsResponse.parse_response(response)
    end

    # -------------------------------------------------------------------------
    # V4 Response: adds leader_epoch in partition responses
    # -------------------------------------------------------------------------

    test "for api version 4 - returns response if all groups succeeded" do
      response = %V4.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{
                offset: 0,
                partition: 1,
                error_code: 0,
                timestamp: 1_000_000_000_000,
                leader_epoch: 5
              }
            ]
          }
        ],
        correlation_id: 3
      }

      assert {:ok, [%KafkaEx.Messages.Offset{} = offset]} =
               ListOffsetsResponse.parse_response(response)

      [po] = offset.partition_offsets
      assert po.partition == 1
      assert po.offset == 0
      assert po.timestamp == 1_000_000_000_000
      assert po.error_code == :no_error
      assert po.leader_epoch == 5
    end

    test "for api version 4 - returns response with leader_epoch -1 (unknown)" do
      response = %V4.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{
                offset: 42,
                partition: 0,
                error_code: 0,
                timestamp: 999,
                leader_epoch: -1
              }
            ]
          }
        ],
        correlation_id: 7
      }

      assert {:ok, [%KafkaEx.Messages.Offset{} = offset]} =
               ListOffsetsResponse.parse_response(response)

      assert offset.topic == "test-topic"
      [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == 42
      assert partition_offset.timestamp == 999
      assert partition_offset.error_code == :no_error
      assert partition_offset.leader_epoch == -1
    end

    test "for api version 4 - returns error if any group failed" do
      response = %V4.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: -1, partition: 1, error_code: 1, timestamp: -1, leader_epoch: -1}
            ]
          }
        ],
        correlation_id: 3
      }

      expected_error = %KafkaEx.Client.Error{
        error: :offset_out_of_range,
        metadata: %{partition: 1, topic: "test-topic"}
      }

      assert {:error, expected_error} == ListOffsetsResponse.parse_response(response)
    end

    test "for api version 4 - handles multiple topics" do
      response = %V4.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "topic-a",
            partition_responses: [
              %{offset: 100, partition: 0, error_code: 0, timestamp: 1000, leader_epoch: 1}
            ]
          },
          %{
            topic: "topic-b",
            partition_responses: [
              %{offset: 200, partition: 0, error_code: 0, timestamp: 2000, leader_epoch: 2}
            ]
          }
        ],
        correlation_id: 8
      }

      assert {:ok, offsets} = ListOffsetsResponse.parse_response(response)
      assert length(offsets) == 2

      topics = Enum.map(offsets, & &1.topic)
      assert "topic-a" in topics
      assert "topic-b" in topics
    end

    # -------------------------------------------------------------------------
    # V5 Response: identical to V4 (uses extract_v4_offset/2)
    # -------------------------------------------------------------------------

    test "for api version 5 - returns response if all groups succeeded" do
      response = %V5.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{
                offset: 0,
                partition: 1,
                error_code: 0,
                timestamp: 1_000_000_000_000,
                leader_epoch: 3
              }
            ]
          }
        ],
        correlation_id: 3
      }

      assert {:ok, [%KafkaEx.Messages.Offset{} = offset]} =
               ListOffsetsResponse.parse_response(response)

      [po] = offset.partition_offsets
      assert po.partition == 1
      assert po.offset == 0
      assert po.timestamp == 1_000_000_000_000
      assert po.error_code == :no_error
      assert po.leader_epoch == 3
    end

    test "for api version 5 - returns error if any group failed" do
      response = %V5.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: -1, partition: 0, error_code: 6, timestamp: -1, leader_epoch: -1}
            ]
          }
        ],
        correlation_id: 3
      }

      expected_error = %KafkaEx.Client.Error{
        error: :not_leader_for_partition,
        metadata: %{partition: 0, topic: "test-topic"}
      }

      assert {:error, expected_error} == ListOffsetsResponse.parse_response(response)
    end

    test "for api version 5 - handles multiple partitions" do
      response = %V5.Response{
        throttle_time_ms: 5,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{offset: 100, partition: 0, error_code: 0, timestamp: 1000, leader_epoch: 1},
              %{offset: 200, partition: 1, error_code: 0, timestamp: 2000, leader_epoch: 1}
            ]
          }
        ],
        correlation_id: 9
      }

      assert {:ok, offsets} = ListOffsetsResponse.parse_response(response)
      # Both partitions from the same topic produce two Offset structs
      # because each partition is wrapped in its own Offset via from_list_offset
      assert length(offsets) == 2
    end

    # -------------------------------------------------------------------------
    # Any fallback implementation tests
    # -------------------------------------------------------------------------

    test "Any fallback - detects V0 path via :offsets field" do
      # A plain map with :offsets matches Any since it's not a known struct
      fake_response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [%{partition: 0, error_code: 0, offsets: [50]}]
          }
        ]
      }

      assert {:ok, [%KafkaEx.Messages.Offset{} = offset]} =
               ListOffsetsResponse.parse_response(fake_response)

      [po] = offset.partition_offsets
      assert po.offset == 50
    end

    test "Any fallback - detects V4+ path via :leader_epoch field" do
      fake_response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 100, timestamp: 500, leader_epoch: 3}
            ]
          }
        ]
      }

      assert {:ok, [%KafkaEx.Messages.Offset{} = offset]} =
               ListOffsetsResponse.parse_response(fake_response)

      [po] = offset.partition_offsets
      assert po.offset == 100
      assert po.timestamp == 500
      assert po.leader_epoch == 3
    end

    test "Any fallback - detects V2 path via :timestamp field (no leader_epoch)" do
      fake_response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 100, timestamp: 500}
            ]
          }
        ]
      }

      assert {:ok, [%KafkaEx.Messages.Offset{} = offset]} =
               ListOffsetsResponse.parse_response(fake_response)

      [po] = offset.partition_offsets
      assert po.offset == 100
      assert po.timestamp == 500
    end

    test "Any fallback - detects V1 path (no special fields)" do
      fake_response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 100}
            ]
          }
        ]
      }

      assert {:ok, [%KafkaEx.Messages.Offset{} = offset]} =
               ListOffsetsResponse.parse_response(fake_response)

      [po] = offset.partition_offsets
      assert po.offset == 100
    end
  end
end
