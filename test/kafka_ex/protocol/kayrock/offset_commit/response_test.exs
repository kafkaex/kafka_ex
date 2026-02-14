defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetCommit
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.Offset.PartitionOffset

  describe "V0 Response implementation" do
    test "parses successful commit response with single partition" do
      response = %Kayrock.OffsetCommit.V0.Response{
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
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
        topics: [
          %{
            name: "multi-partition",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 0},
              %{partition_index: 2, error_code: 0}
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
        topics: [
          %{
            name: "topic-1",
            partitions: [%{partition_index: 0, error_code: 0}]
          },
          %{
            name: "topic-2",
            partitions: [%{partition_index: 0, error_code: 0}]
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
        topics: [
          %{
            name: "error-topic",
            partitions: [
              %{partition_index: 0, error_code: 12}
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
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 14},
              %{partition_index: 2, error_code: 0}
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
          topics: [
            %{
              name: "test-topic",
              partitions: [%{partition_index: 0, error_code: code}]
            }
          ]
        }

        assert {:error, error} = OffsetCommit.Response.parse_response(response)
        assert error.error == expected_error
      end
    end

    test "parses response with empty partition list" do
      response = %Kayrock.OffsetCommit.V0.Response{
        topics: [
          %{
            name: "empty-topic",
            partitions: []
          }
        ]
      }

      assert {:ok, []} = OffsetCommit.Response.parse_response(response)
    end
  end

  describe "V1 Response implementation" do
    test "parses successful commit response" do
      response = %Kayrock.OffsetCommit.V1.Response{
        topics: [
          %{
            name: "v1-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
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
        topics: [
          %{
            name: "multi-part-v1",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 0},
              %{partition_index: 2, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 3
    end

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetCommit.V1.Response{
        topics: [
          %{
            name: "error-v1-topic",
            partitions: [
              %{partition_index: 0, error_code: 22}
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
          topics: [
            %{
              name: "test-topic",
              partitions: [%{partition_index: 0, error_code: code}]
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
        topics: [
          %{
            name: "v2-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
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
        topics: [
          %{
            name: "multi-part-v2",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 0},
              %{partition_index: 2, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 3
    end

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetCommit.V2.Response{
        topics: [
          %{
            name: "error-v2-topic",
            partitions: [
              %{partition_index: 0, error_code: 27}
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
          topics: [
            %{
              name: "test-topic",
              partitions: [%{partition_index: 0, error_code: code}]
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
        topics: [
          %{
            name: "v3-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
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
        topics: [
          %{
            name: "no-throttle-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
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
        topics: [
          %{
            name: "multi-v3",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 0},
              %{partition_index: 2, error_code: 0}
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
        topics: [
          %{
            name: "error-v3-topic",
            partitions: [
              %{partition_index: 0, error_code: 14}
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
        topics: [
          %{
            name: "throttled-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
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
        topics: [
          %{
            name: "topic-1",
            partitions: [%{partition_index: 0, error_code: 0}]
          },
          %{
            name: "topic-2",
            partitions: [%{partition_index: 0, error_code: 0}]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 2
    end
  end

  describe "V4 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.OffsetCommit.V4.Response{
        throttle_time_ms: 50,
        topics: [
          %{
            name: "v4-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "v4-topic",
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

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetCommit.V4.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "error-v4-topic",
            partitions: [
              %{partition_index: 0, error_code: 14}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :coordinator_load_in_progress
      assert error.metadata.partition == 0
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetCommit.V4.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "multi-v4",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 0},
              %{partition_index: 2, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 3
    end

    test "parses response with multiple topics" do
      response = %Kayrock.OffsetCommit.V4.Response{
        throttle_time_ms: 25,
        topics: [
          %{
            name: "topic-1",
            partitions: [%{partition_index: 0, error_code: 0}]
          },
          %{
            name: "topic-2",
            partitions: [%{partition_index: 0, error_code: 0}]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 2
    end
  end

  describe "V5 Response implementation" do
    test "parses successful response" do
      response = %Kayrock.OffsetCommit.V5.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "v5-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "v5-topic",
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

    test "returns error for non-zero error code" do
      response = %Kayrock.OffsetCommit.V5.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "error-v5-topic",
            partitions: [
              %{partition_index: 0, error_code: 22}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetCommit.V5.Response{
        throttle_time_ms: 100,
        topics: [
          %{
            name: "multi-v5",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 2
    end
  end

  describe "V6 Response implementation" do
    test "parses successful response" do
      response = %Kayrock.OffsetCommit.V6.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "v6-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "v6-topic",
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

    test "returns error for rebalance_in_progress" do
      response = %Kayrock.OffsetCommit.V6.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "error-v6-topic",
            partitions: [
              %{partition_index: 0, error_code: 27}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "parses response with empty partition list" do
      response = %Kayrock.OffsetCommit.V6.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "empty-topic",
            partitions: []
          }
        ]
      }

      assert {:ok, []} = OffsetCommit.Response.parse_response(response)
    end
  end

  describe "V7 Response implementation" do
    test "parses successful response" do
      response = %Kayrock.OffsetCommit.V7.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "v7-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "v7-topic",
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

    test "returns error for unknown_member_id" do
      response = %Kayrock.OffsetCommit.V7.Response{
        throttle_time_ms: 0,
        topics: [
          %{
            name: "error-v7-topic",
            partitions: [
              %{partition_index: 0, error_code: 25}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses response with multiple topics and partitions" do
      response = %Kayrock.OffsetCommit.V7.Response{
        throttle_time_ms: 200,
        topics: [
          %{
            name: "topic-1",
            partitions: [
              %{partition_index: 0, error_code: 0},
              %{partition_index: 1, error_code: 0}
            ]
          },
          %{
            name: "topic-2",
            partitions: [
              %{partition_index: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 3
    end
  end

  describe "V8 Response implementation (flexible version)" do
    test "parses successful response with tagged_fields" do
      response = %Kayrock.OffsetCommit.V8.Response{
        throttle_time_ms: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "v8-topic",
            tagged_fields: [],
            partitions: [
              %{partition_index: 0, error_code: 0, tagged_fields: []}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)

      assert result == %Offset{
               topic: "v8-topic",
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

    test "returns error for coordinator_not_available" do
      response = %Kayrock.OffsetCommit.V8.Response{
        throttle_time_ms: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "error-v8-topic",
            tagged_fields: [],
            partitions: [
              %{partition_index: 0, error_code: 15, tagged_fields: []}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses response with non-empty tagged_fields" do
      response = %Kayrock.OffsetCommit.V8.Response{
        throttle_time_ms: 0,
        tagged_fields: [{0, <<1, 2, 3>>}],
        topics: [
          %{
            name: "tagged-topic",
            tagged_fields: [{1, <<4, 5>>}],
            partitions: [
              %{partition_index: 0, error_code: 0, tagged_fields: [{2, <<6>>}]}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)
      assert result.topic == "tagged-topic"
    end

    test "parses response with multiple partitions and topics" do
      response = %Kayrock.OffsetCommit.V8.Response{
        throttle_time_ms: 50,
        tagged_fields: [],
        topics: [
          %{
            name: "topic-1",
            tagged_fields: [],
            partitions: [
              %{partition_index: 0, error_code: 0, tagged_fields: []},
              %{partition_index: 1, error_code: 0, tagged_fields: []}
            ]
          },
          %{
            name: "topic-2",
            tagged_fields: [],
            partitions: [
              %{partition_index: 0, error_code: 0, tagged_fields: []}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetCommit.Response.parse_response(response)
      assert length(results) == 3
    end

    test "returns error on first partition error" do
      response = %Kayrock.OffsetCommit.V8.Response{
        throttle_time_ms: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "mixed-v8",
            tagged_fields: [],
            partitions: [
              %{partition_index: 0, error_code: 0, tagged_fields: []},
              %{partition_index: 1, error_code: 16, tagged_fields: []},
              %{partition_index: 2, error_code: 0, tagged_fields: []}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :not_coordinator
      assert error.metadata.partition == 1
    end

    test "handles various V8 error codes" do
      error_codes = [
        {12, :offset_metadata_too_large},
        {14, :coordinator_load_in_progress},
        {15, :coordinator_not_available},
        {16, :not_coordinator},
        {22, :illegal_generation},
        {25, :unknown_member_id},
        {27, :rebalance_in_progress}
      ]

      for {code, expected_error} <- error_codes do
        response = %Kayrock.OffsetCommit.V8.Response{
          throttle_time_ms: 0,
          tagged_fields: [],
          topics: [
            %{
              name: "test-topic",
              tagged_fields: [],
              partitions: [%{partition_index: 0, error_code: code, tagged_fields: []}]
            }
          ]
        }

        assert {:error, error} = OffsetCommit.Response.parse_response(response)
        assert error.error == expected_error
      end
    end
  end

  describe "V8 Response - edge cases" do
    test "parses response with empty partition list" do
      response = %Kayrock.OffsetCommit.V8.Response{
        throttle_time_ms: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "empty-v8-topic",
            tagged_fields: [],
            partitions: []
          }
        ]
      }

      assert {:ok, []} = OffsetCommit.Response.parse_response(response)
    end

    test "returns error on first topic when first topic has partition error" do
      response = %Kayrock.OffsetCommit.V8.Response{
        throttle_time_ms: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "error-topic",
            tagged_fields: [],
            partitions: [
              %{partition_index: 0, error_code: 16, tagged_fields: []}
            ]
          },
          %{
            name: "ok-topic",
            tagged_fields: [],
            partitions: [
              %{partition_index: 0, error_code: 0, tagged_fields: []}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :not_coordinator
      assert error.metadata.topic == "error-topic"
    end
  end

  describe "Any Response fallback implementation" do
    test "parses successful response via plain map" do
      response = %{
        topics: [
          %{
            name: "any-topic",
            partitions: [
              %{partition_index: 0, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)
      assert result.topic == "any-topic"
      partition_offset = hd(result.partition_offsets)
      assert partition_offset.partition == 0
      assert partition_offset.error_code == :no_error
    end

    test "returns error for partition error via plain map" do
      response = %{
        topics: [
          %{
            name: "any-error-topic",
            partitions: [
              %{partition_index: 0, error_code: 25}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetCommit.Response.parse_response(response)
      assert error.error == :unknown_member_id
      assert error.metadata.topic == "any-error-topic"
    end

    test "parses response with tagged_fields via plain map (V8-like)" do
      response = %{
        tagged_fields: [{0, <<1, 2>>}],
        topics: [
          %{
            name: "any-v8-topic",
            tagged_fields: [],
            partitions: [
              %{partition_index: 0, error_code: 0, tagged_fields: []}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)
      assert result.topic == "any-v8-topic"
    end

    test "parses empty topics via plain map" do
      response = %{topics: []}

      assert {:ok, []} = OffsetCommit.Response.parse_response(response)
    end
  end

  describe "Cross-version response consistency" do
    test "V3 through V8 all parse identical success responses the same way" do
      versions = [
        %Kayrock.OffsetCommit.V3.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "test-topic", partitions: [%{partition_index: 0, error_code: 0}]}
          ]
        },
        %Kayrock.OffsetCommit.V4.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "test-topic", partitions: [%{partition_index: 0, error_code: 0}]}
          ]
        },
        %Kayrock.OffsetCommit.V5.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "test-topic", partitions: [%{partition_index: 0, error_code: 0}]}
          ]
        },
        %Kayrock.OffsetCommit.V6.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "test-topic", partitions: [%{partition_index: 0, error_code: 0}]}
          ]
        },
        %Kayrock.OffsetCommit.V7.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "test-topic", partitions: [%{partition_index: 0, error_code: 0}]}
          ]
        },
        %Kayrock.OffsetCommit.V8.Response{
          throttle_time_ms: 0,
          tagged_fields: [],
          topics: [
            %{
              name: "test-topic",
              tagged_fields: [],
              partitions: [%{partition_index: 0, error_code: 0, tagged_fields: []}]
            }
          ]
        }
      ]

      expected = %Offset{
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

      for response <- versions do
        assert {:ok, [result]} = OffsetCommit.Response.parse_response(response)
        assert result == expected, "Failed for #{inspect(response.__struct__)}"
      end
    end

    test "V3 through V8 all return same error for same error code" do
      versions = [
        %Kayrock.OffsetCommit.V3.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "err-topic", partitions: [%{partition_index: 0, error_code: 25}]}
          ]
        },
        %Kayrock.OffsetCommit.V4.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "err-topic", partitions: [%{partition_index: 0, error_code: 25}]}
          ]
        },
        %Kayrock.OffsetCommit.V5.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "err-topic", partitions: [%{partition_index: 0, error_code: 25}]}
          ]
        },
        %Kayrock.OffsetCommit.V6.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "err-topic", partitions: [%{partition_index: 0, error_code: 25}]}
          ]
        },
        %Kayrock.OffsetCommit.V7.Response{
          throttle_time_ms: 0,
          topics: [
            %{name: "err-topic", partitions: [%{partition_index: 0, error_code: 25}]}
          ]
        },
        %Kayrock.OffsetCommit.V8.Response{
          throttle_time_ms: 0,
          tagged_fields: [],
          topics: [
            %{
              name: "err-topic",
              tagged_fields: [],
              partitions: [%{partition_index: 0, error_code: 25, tagged_fields: []}]
            }
          ]
        }
      ]

      for response <- versions do
        assert {:error, error} = OffsetCommit.Response.parse_response(response),
               "Failed for #{inspect(response.__struct__)}"

        assert error.error == :unknown_member_id
      end
    end
  end
end
