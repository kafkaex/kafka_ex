defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetCommit
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.Offset.PartitionOffset

  describe "V0 Response implementation" do
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

  describe "V1 Response implementation" do
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

  describe "V2 Response implementation" do
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

  describe "V3 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 100,
        responses: [
          %{
            topic: "v3-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "v3-topic",
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

    test "parses response with zero throttle_time_ms" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 0,
        responses: [
          %{
            topic: "no-throttle-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)
      assert result.topic == "no-throttle-topic"
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 50,
        responses: [
          %{
            topic: "multi-v3",
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
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 25,
        responses: [
          %{
            topic: "error-v3-topic",
            partition_responses: [
              %{partition: 0, error_code: 14}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :coordinator_load_in_progress
      assert error.metadata.partition == 0
    end

    test "handles high throttle_time_ms values" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 5000,
        responses: [
          %{
            topic: "throttled-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)
      assert result.topic == "throttled-topic"
    end

    test "handles multiple topics with throttle time" do
      response = %Kayrock.OffsetCommit.V3.Response{
        throttle_time_ms: 150,
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
    end
  end
end
