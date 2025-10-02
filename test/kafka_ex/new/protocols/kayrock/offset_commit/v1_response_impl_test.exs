defmodule KafkaEx.New.Protocols.Kayrock.OffsetCommit.V1ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetCommit
  alias KafkaEx.New.Structs.Offset
  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "parse_response/1 for V1" do
    test "parses successful commit response" do
      response = %Kayrock.OffsetCommit.V1.Response{
        responses: [
          %{
            topic: "v1-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "v1-topic",
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
      response = %Kayrock.OffsetCommit.V1.Response{
        responses: [
          %{
            topic: "multi-part-v1",
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
      response = %Kayrock.OffsetCommit.V1.Response{
        responses: [
          %{
            topic: "error-v1-topic",
            partition_responses: [
              %{partition: 0, error_code: 22}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :illegal_generation
      assert error.metadata.topic == "error-v1-topic"
      assert error.metadata.partition == 0
    end

    test "handles coordinator errors" do
      error_codes = [
        {14, :coordinator_load_in_progress},
        {15, :coordinator_not_available},
        {16, :not_coordinator},
        {25, :unknown_member_id}
      ]

      for {code, expected_error} <- error_codes do
        response = %Kayrock.OffsetCommit.V1.Response{
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
