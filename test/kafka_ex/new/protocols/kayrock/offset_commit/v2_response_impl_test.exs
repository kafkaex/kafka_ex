defmodule KafkaEx.New.Protocols.Kayrock.OffsetCommit.V2ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetCommit
  alias KafkaEx.New.Structs.Offset
  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "parse_response/1 for V2" do
    test "parses successful commit response" do
      response = %Kayrock.OffsetCommit.V2.Response{
        responses: [
          %{
            topic: "v2-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "v2-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   error_code: :no_error,
                   offset: nil,
                   timestamp: nil,
                   metadata: nil
                 }
               ]
             }
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetCommit.V2.Response{
        responses: [
          %{
            topic: "multi-part-v2",
            partition_responses: [
              %{partition: 0, error_code: 0},
              %{partition: 1, error_code: 0},
              %{partition: 2, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 3
    end

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetCommit.V2.Response{
        responses: [
          %{
            topic: "error-v2-topic",
            partition_responses: [
              %{partition: 0, error_code: 27}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
      assert error.metadata.topic == "error-v2-topic"
      assert error.metadata.partition == 0
    end

    test "handles various v2-specific errors" do
      error_codes = [
        {12, :offset_metadata_too_large},
        {22, :illegal_generation},
        {25, :unknown_member_id},
        {27, :rebalance_in_progress}
      ]

      for {code, expected_error} <- error_codes do
        response = %Kayrock.OffsetCommit.V2.Response{
          responses: [
            %{
              topic: "test-topic",
              partition_responses: [%{partition: 0, error_code: code}]
            }
          ]
        }

        assert {:error, error} = OffsetCommit.Response.parse_response(response)
        assert error.error == expected_error
      end
    end
  end
end
