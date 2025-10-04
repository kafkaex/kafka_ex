defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch.V2ResponseImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch
  alias KafkaEx.New.Structs.Offset
  alias KafkaEx.New.Structs.Offset.PartitionOffset

  describe "parse_response/1 for V2" do
    test "parses successful response with top-level error_code 0" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        responses: [
          %{
            topic: "v2-topic",
            partition_responses: [
              %{partition: 0, offset: 1000, metadata: "v2-metadata", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "v2-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 1000,
                   metadata: "v2-metadata",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "returns error when top-level error_code is non-zero" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 16,
        responses: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses response with multiple partitions when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        responses: [
          %{
            topic: "multi-v2",
            partition_responses: [
              %{partition: 0, offset: 10, metadata: "", error_code: 0},
              %{partition: 1, offset: 20, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 2
    end

    test "returns partition-level error even when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        responses: [
          %{
            topic: "partition-error-topic",
            partition_responses: [
              %{partition: 0, offset: -1, metadata: "", error_code: 9}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :replica_not_available
      assert error.metadata.partition == 0
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        responses: [
          %{
            topic: "nil-meta-v2",
            partition_responses: [
              %{partition: 0, offset: 777, metadata: nil, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
    end

    test "handles broker-level coordinator errors" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 15,
        responses: [
          %{
            topic: "some-topic",
            partition_responses: [
              %{partition: 0, offset: 100, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end
  end
end
