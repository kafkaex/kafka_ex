defmodule KafkaEx.Protocol.Kayrock.ListOffsets.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ListOffsets.Response, as: ListOffsetsResponse

  alias Kayrock.ListOffsets.V0
  alias Kayrock.ListOffsets.V1
  alias Kayrock.ListOffsets.V2

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
  end
end
