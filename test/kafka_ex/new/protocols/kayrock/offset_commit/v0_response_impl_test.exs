defmodule KafkaEx.New.Protocols.Kayrock.OffsetCommit.V0ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetCommit
  alias KafkaEx.New.Structs.Offset
  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "parse_response/1 for V0" do
    test "parses successful commit response with single partition" do
      response = %Kayrock.OffsetCommit.V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "test-topic",
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

    test "parses successful commit with multiple partitions" do
      response = %Kayrock.OffsetCommit.V0.Response{
        responses: [
          %{
            topic: "multi-partition",
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

      # Results are accumulated in reverse order
      assert Enum.at(results, 2).partition_offsets |> hd() |> Map.get(:partition) == 0
      assert Enum.at(results, 1).partition_offsets |> hd() |> Map.get(:partition) == 1
      assert Enum.at(results, 0).partition_offsets |> hd() |> Map.get(:partition) == 2
    end

    test "parses successful commit with multiple topics" do
      response = %Kayrock.OffsetCommit.V0.Response{
        responses: [
          %{
            topic: "topic-1",
            partition_responses: [%{partition: 0, error_code: 0}]
          },
          %{
            topic: "topic-2",
            partition_responses: [%{partition: 0, error_code: 0}]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 2

      # Results are accumulated in reverse order
      assert Enum.at(results, 1).topic == "topic-1"
      assert Enum.at(results, 0).topic == "topic-2"
    end

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetCommit.V0.Response{
        responses: [
          %{
            topic: "error-topic",
            partition_responses: [
              %{partition: 0, error_code: 12}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :offset_metadata_too_large
      assert error.metadata.topic == "error-topic"
      assert error.metadata.partition == 0
    end

    test "returns error on first partition error with multiple partitions" do
      response = %Kayrock.OffsetCommit.V0.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0},
              %{partition: 1, error_code: 14},
              %{partition: 2, error_code: 0}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :coordinator_load_in_progress
      assert error.metadata.partition == 1
    end

    test "handles various error codes" do
      error_codes = [
        {12, :offset_metadata_too_large},
        {14, :coordinator_load_in_progress},
        {15, :coordinator_not_available},
        {16, :not_coordinator}
      ]

      for {code, expected_error} <- error_codes do
        response = %Kayrock.OffsetCommit.V0.Response{
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

    test "parses response with empty partition list" do
      response = %Kayrock.OffsetCommit.V0.Response{
        responses: [
          %{
            topic: "empty-topic",
            partition_responses: []
          }
        ]
      }

      assert {:ok, []} = OffsetCommit.Response.parse_response(response)
    end
  end
end
