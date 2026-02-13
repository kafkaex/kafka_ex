defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetFetch
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.Offset.PartitionOffset

  describe "V0 Response implementation" do
    test "parses successful response with single partition" do
      response = %Kayrock.OffsetFetch.V0.Response{
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 42, metadata: "consumer-1", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "test-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 42,
                   metadata: "consumer-1",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V0.Response{
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: nil, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetFetch.V0.Response{
        topics: [
          %{
            name: "multi-partition",
            partitions: [
              %{partition_index: 0, committed_offset: 10, metadata: "meta-0", error_code: 0},
              %{partition_index: 1, committed_offset: 20, metadata: "meta-1", error_code: 0},
              %{partition_index: 2, committed_offset: 30, metadata: "meta-2", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 3

      # Results are accumulated in reverse order
      assert Enum.at(results, 2).partition_offsets |> hd() |> Map.get(:partition) == 0
      assert Enum.at(results, 1).partition_offsets |> hd() |> Map.get(:partition) == 1
      assert Enum.at(results, 0).partition_offsets |> hd() |> Map.get(:partition) == 2
    end

    test "parses response with multiple topics" do
      response = %Kayrock.OffsetFetch.V0.Response{
        topics: [
          %{
            name: "topic-1",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "", error_code: 0}
            ]
          },
          %{
            name: "topic-2",
            partitions: [
              %{partition_index: 0, committed_offset: 200, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 2

      # Results are accumulated in reverse order
      assert Enum.at(results, 1).topic == "topic-1"
      assert Enum.at(results, 0).topic == "topic-2"
    end

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetFetch.V0.Response{
        topics: [
          %{
            name: "error-topic",
            partitions: [
              %{partition_index: 0, committed_offset: -1, metadata: "", error_code: 3}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :unknown_topic_or_partition
      assert error.metadata.topic == "error-topic"
      assert error.metadata.partition == 0
    end

    test "returns error on first partition error with multiple partitions" do
      response = %Kayrock.OffsetFetch.V0.Response{
        topics: [
          %{
            name: "test-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "", error_code: 0},
              %{partition_index: 1, committed_offset: -1, metadata: "", error_code: 15},
              %{partition_index: 2, committed_offset: 200, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :coordinator_not_available
      assert error.metadata.partition == 1
    end

    test "parses response with offset -1 (no committed offset)" do
      response = %Kayrock.OffsetFetch.V0.Response{
        topics: [
          %{
            name: "new-consumer-topic",
            partitions: [
              %{partition_index: 0, committed_offset: -1, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.offset == -1
      assert partition_offset.error_code == :no_error
    end
  end

  describe "V1 Response implementation" do
    test "parses successful response with single partition" do
      response = %Kayrock.OffsetFetch.V1.Response{
        topics: [
          %{
            name: "v1-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 999, metadata: "v1-consumer", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "v1-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 999,
                   metadata: "v1-consumer",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V1.Response{
        topics: [
          %{
            name: "no-metadata-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 500, metadata: nil, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetFetch.V1.Response{
        topics: [
          %{
            name: "multi-part-v1",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "part-0", error_code: 0},
              %{partition_index: 1, committed_offset: 200, metadata: "part-1", error_code: 0},
              %{partition_index: 2, committed_offset: 300, metadata: "part-2", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 3

      # Results are accumulated in reverse order
      assert Enum.at(results, 2).partition_offsets |> hd() |> Map.get(:offset) == 100
      assert Enum.at(results, 1).partition_offsets |> hd() |> Map.get(:offset) == 200
      assert Enum.at(results, 0).partition_offsets |> hd() |> Map.get(:offset) == 300
    end

    test "returns error when partition has error code" do
      response = %Kayrock.OffsetFetch.V1.Response{
        topics: [
          %{
            name: "error-v1-topic",
            partitions: [
              %{partition_index: 0, committed_offset: -1, metadata: "", error_code: 14}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :coordinator_load_in_progress
      assert error.metadata.topic == "error-v1-topic"
      assert error.metadata.partition == 0
    end

    test "parses response with coordinator-based offset" do
      response = %Kayrock.OffsetFetch.V1.Response{
        topics: [
          %{
            name: "coordinator-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 12_345, metadata: "coordinator-consumer-id", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.offset == 12_345
      assert partition_offset.metadata == "coordinator-consumer-id"
    end
  end

  describe "V2 Response implementation" do
    test "parses successful response with top-level error_code 0" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        topics: [
          %{
            name: "v2-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 1000, metadata: "v2-metadata", error_code: 0}
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
        topics: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses response with multiple partitions when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V2.Response{
        error_code: 0,
        topics: [
          %{
            name: "multi-v2",
            partitions: [
              %{partition_index: 0, committed_offset: 10, metadata: "", error_code: 0},
              %{partition_index: 1, committed_offset: 20, metadata: "", error_code: 0}
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
        topics: [
          %{
            name: "partition-error-topic",
            partitions: [
              %{partition_index: 0, committed_offset: -1, metadata: "", error_code: 9}
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
        topics: [
          %{
            name: "nil-meta-v2",
            partitions: [
              %{partition_index: 0, committed_offset: 777, metadata: nil, error_code: 0}
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
        topics: [
          %{
            name: "some-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end
  end

  describe "V3 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 100,
        error_code: 0,
        topics: [
          %{
            name: "v3-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 5000, metadata: "v3-consumer", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "v3-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 5000,
                   metadata: "v3-consumer",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "parses response with zero throttle_time_ms" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "no-throttle-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 200, metadata: "", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      assert result.topic == "no-throttle-topic"
    end

    test "returns error when top-level error_code is non-zero" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 50,
        error_code: 16,
        topics: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 25,
        error_code: 0,
        topics: [
          %{
            name: "multi-v3",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "meta-0", error_code: 0},
              %{partition_index: 1, committed_offset: 200, metadata: "meta-1", error_code: 0},
              %{partition_index: 2, committed_offset: 300, metadata: "meta-2", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 3
    end

    test "returns partition-level error even when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 10,
        error_code: 0,
        topics: [
          %{
            name: "partition-error-v3",
            partitions: [
              %{partition_index: 0, committed_offset: -1, metadata: "", error_code: 3}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :unknown_topic_or_partition
      assert error.metadata.partition == 0
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "nil-meta-v3",
            partitions: [
              %{partition_index: 0, committed_offset: 888, metadata: nil, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
    end

    test "handles high throttle_time_ms values" do
      response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 5000,
        error_code: 0,
        topics: [
          %{
            name: "throttled-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 1000, metadata: "throttled", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      assert result.topic == "throttled-topic"
    end
  end

  describe "V4 Response implementation" do
    test "parses successful response (identical to V3)" do
      response = %Kayrock.OffsetFetch.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "v4-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 6000, metadata: "v4-consumer", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "v4-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 6000,
                   metadata: "v4-consumer",
                   error_code: :no_error,
                   timestamp: nil
                 }
               ]
             }
    end

    test "returns error when top-level error_code is non-zero" do
      response = %Kayrock.OffsetFetch.V4.Response{
        throttle_time_ms: 0,
        error_code: 16,
        topics: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses response with multiple partitions" do
      response = %Kayrock.OffsetFetch.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "multi-v4",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "meta-0", error_code: 0},
              %{partition_index: 1, committed_offset: 200, metadata: "meta-1", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 2
    end

    test "returns partition-level error even when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "partition-error-v4",
            partitions: [
              %{partition_index: 0, committed_offset: -1, metadata: "", error_code: 3}
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :unknown_topic_or_partition
      assert error.metadata.partition == 0
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "nil-meta-v4",
            partitions: [
              %{partition_index: 0, committed_offset: 444, metadata: nil, error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
    end
  end

  describe "V5 Response implementation" do
    test "parses successful response with committed_leader_epoch" do
      response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "v5-topic",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 7000,
                committed_leader_epoch: 5,
                metadata: "v5-consumer",
                error_code: 0
              }
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "v5-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 7000,
                   metadata: "v5-consumer",
                   error_code: :no_error,
                   timestamp: nil,
                   leader_epoch: 5
                 }
               ]
             }
    end

    test "parses committed_leader_epoch of -1 (unknown)" do
      response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "v5-unknown-epoch",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 500,
                committed_leader_epoch: -1,
                metadata: "",
                error_code: 0
              }
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.leader_epoch == -1
    end

    test "parses committed_leader_epoch of 0" do
      response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "v5-zero-epoch",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_leader_epoch: 0,
                metadata: "meta",
                error_code: 0
              }
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.leader_epoch == 0
    end

    test "returns error when top-level error_code is non-zero" do
      response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 50,
        error_code: 16,
        topics: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses response with multiple partitions each with leader_epoch" do
      response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "multi-v5",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_leader_epoch: 3,
                metadata: "meta-0",
                error_code: 0
              },
              %{
                partition_index: 1,
                committed_offset: 200,
                committed_leader_epoch: 4,
                metadata: "meta-1",
                error_code: 0
              },
              %{
                partition_index: 2,
                committed_offset: 300,
                committed_leader_epoch: 5,
                metadata: "meta-2",
                error_code: 0
              }
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 3

      # Results are accumulated in reverse order
      leader_epochs = Enum.map(results, fn r -> hd(r.partition_offsets).leader_epoch end)
      assert leader_epochs == [5, 4, 3]
    end

    test "returns partition-level error even when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "partition-error-v5",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: -1,
                committed_leader_epoch: -1,
                metadata: "",
                error_code: 3
              }
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :unknown_topic_or_partition
      assert error.metadata.partition == 0
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "nil-meta-v5",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 555,
                committed_leader_epoch: 2,
                metadata: nil,
                error_code: 0
              }
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
      assert partition_offset.leader_epoch == 2
    end

    test "parses response with multiple topics" do
      response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "topic-1",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_leader_epoch: 1,
                metadata: "",
                error_code: 0
              }
            ]
          },
          %{
            name: "topic-2",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 200,
                committed_leader_epoch: 2,
                metadata: "",
                error_code: 0
              }
            ]
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 2

      # Results are accumulated in reverse order
      assert Enum.at(results, 1).topic == "topic-1"
      assert Enum.at(results, 0).topic == "topic-2"
    end
  end

  describe "V6 Response implementation (flexible version)" do
    test "parses successful response with committed_leader_epoch" do
      response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 0,
        error_code: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "v6-topic",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 8000,
                committed_leader_epoch: 7,
                metadata: "v6-consumer",
                error_code: 0,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result == %Offset{
               topic: "v6-topic",
               partition_offsets: [
                 %PartitionOffset{
                   partition: 0,
                   offset: 8000,
                   metadata: "v6-consumer",
                   error_code: :no_error,
                   timestamp: nil,
                   leader_epoch: 7
                 }
               ]
             }
    end

    test "returns error when top-level error_code is non-zero" do
      response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 0,
        error_code: 16,
        tagged_fields: [],
        topics: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses response with multiple partitions each with leader_epoch" do
      response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 10,
        error_code: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "multi-v6",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_leader_epoch: 10,
                metadata: "m0",
                error_code: 0,
                tagged_fields: []
              },
              %{
                partition_index: 1,
                committed_offset: 200,
                committed_leader_epoch: 11,
                metadata: "m1",
                error_code: 0,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          }
        ]
      }

      assert {:ok, results} = OffsetFetch.Response.parse_response(response)
      assert length(results) == 2

      # Results accumulated in reverse order
      epochs = Enum.map(results, fn r -> hd(r.partition_offsets).leader_epoch end)
      assert epochs == [11, 10]
    end

    test "returns partition-level error even when top-level error is 0" do
      response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 0,
        error_code: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "partition-error-v6",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: -1,
                committed_leader_epoch: -1,
                metadata: "",
                error_code: 3,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :unknown_topic_or_partition
      assert error.metadata.partition == 0
    end

    test "parses response with nil metadata" do
      response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 0,
        error_code: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "nil-meta-v6",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 666,
                committed_leader_epoch: 3,
                metadata: nil,
                error_code: 0,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.metadata == ""
      assert partition_offset.leader_epoch == 3
    end

    test "handles coordinator_load_in_progress error" do
      response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 0,
        error_code: 14,
        tagged_fields: [],
        topics: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :coordinator_load_in_progress
    end
  end

  describe "Any Response fallback implementation" do
    test "handles V0/V1-style response without top-level error_code via plain map" do
      response = %{
        topics: [
          %{
            name: "any-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 42, metadata: "any-meta", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      assert result.topic == "any-topic"
      partition_offset = hd(result.partition_offsets)
      assert partition_offset.offset == 42
      assert partition_offset.metadata == "any-meta"
    end

    test "handles V2-V4-style response with top-level error_code via plain map" do
      response = %{
        error_code: 0,
        topics: [
          %{
            name: "any-v2-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "meta", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      assert result.topic == "any-v2-topic"
    end

    test "handles V2-V4-style response with top-level error via plain map" do
      response = %{
        error_code: 16,
        topics: []
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "handles V5+-style response with committed_leader_epoch via plain map" do
      response = %{
        error_code: 0,
        topics: [
          %{
            name: "any-v5-topic",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 500,
                committed_leader_epoch: 8,
                metadata: "v5-meta",
                error_code: 0
              }
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)

      partition_offset = hd(result.partition_offsets)
      assert partition_offset.offset == 500
      assert partition_offset.leader_epoch == 8
    end

    test "handles V5+-style response with top-level error via plain map" do
      response = %{
        error_code: 15,
        topics: [
          %{
            name: "any-v5-error",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_leader_epoch: 1,
                metadata: "",
                error_code: 0
              }
            ]
          }
        ]
      }

      assert {:error, error} = OffsetFetch.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "handles empty topics with error_code in plain map" do
      response = %{
        error_code: 0,
        topics: []
      }

      # No committed_leader_epoch to detect, should use V2+ path
      assert {:ok, []} = OffsetFetch.Response.parse_response(response)
    end

    test "handles topics with empty partitions list" do
      response = %{
        error_code: 0,
        topics: [
          %{
            name: "empty-partitions-topic",
            partitions: []
          }
        ]
      }

      assert {:ok, []} = OffsetFetch.Response.parse_response(response)
    end

    test "handles V0/V1-style response with empty topics (no error_code key)" do
      response = %{topics: []}

      assert {:ok, []} = OffsetFetch.Response.parse_response(response)
    end

    test "V2-V4-style response produces no leader_epoch in result" do
      response = %{
        error_code: 0,
        topics: [
          %{
            name: "no-epoch-topic",
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "meta", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      partition_offset = hd(result.partition_offsets)
      assert partition_offset.leader_epoch == nil
    end
  end

  describe "Cross-version consistency" do
    test "V4 response produces same result as V3 for identical topic/partition data" do
      topics = [
        %{
          name: "consistency-topic",
          partitions: [
            %{partition_index: 0, committed_offset: 100, metadata: "meta", error_code: 0},
            %{partition_index: 1, committed_offset: 200, metadata: "", error_code: 0}
          ]
        }
      ]

      v3_response = %Kayrock.OffsetFetch.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: topics
      }

      v4_response = %Kayrock.OffsetFetch.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: topics
      }

      assert {:ok, v3_result} = OffsetFetch.Response.parse_response(v3_response)
      assert {:ok, v4_result} = OffsetFetch.Response.parse_response(v4_response)

      assert v3_result == v4_result
    end

    test "V4 response does not include leader_epoch in PartitionOffset" do
      response = %Kayrock.OffsetFetch.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: "no-epoch",
            partitions: [
              %{partition_index: 0, committed_offset: 500, metadata: "test", error_code: 0}
            ]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      partition_offset = hd(result.partition_offsets)
      assert partition_offset.leader_epoch == nil
    end

    test "V5 and V6 responses produce same result for identical topic/partition data" do
      topics = [
        %{
          name: "v5-v6-topic",
          partitions: [
            %{
              partition_index: 0,
              committed_offset: 100,
              committed_leader_epoch: 5,
              metadata: "meta",
              error_code: 0
            }
          ]
        }
      ]

      v5_response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: topics
      }

      # V6 topics/partitions may have tagged_fields at topic/partition level,
      # but the parser ignores them -- only uses the standard fields
      v6_topics =
        Enum.map(topics, fn topic ->
          Map.put(topic, :tagged_fields, [])
          |> Map.update!(:partitions, fn parts ->
            Enum.map(parts, &Map.put(&1, :tagged_fields, []))
          end)
        end)

      v6_response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 0,
        error_code: 0,
        tagged_fields: [],
        topics: v6_topics
      }

      assert {:ok, v5_result} = OffsetFetch.Response.parse_response(v5_response)
      assert {:ok, v6_result} = OffsetFetch.Response.parse_response(v6_response)

      assert v5_result == v6_result
    end

    test "V5 response includes leader_epoch while V4 does not for same topic data" do
      topic_name = "epoch-comparison"

      v4_response = %Kayrock.OffsetFetch.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: topic_name,
            partitions: [
              %{partition_index: 0, committed_offset: 100, metadata: "m", error_code: 0}
            ]
          }
        ]
      }

      v5_response = %Kayrock.OffsetFetch.V5.Response{
        throttle_time_ms: 0,
        error_code: 0,
        topics: [
          %{
            name: topic_name,
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_leader_epoch: 3,
                metadata: "m",
                error_code: 0
              }
            ]
          }
        ]
      }

      assert {:ok, [v4_result]} = OffsetFetch.Response.parse_response(v4_response)
      assert {:ok, [v5_result]} = OffsetFetch.Response.parse_response(v5_response)

      v4_po = hd(v4_result.partition_offsets)
      v5_po = hd(v5_result.partition_offsets)

      assert v4_po.leader_epoch == nil
      assert v5_po.leader_epoch == 3

      # All other fields should match
      assert v4_po.partition == v5_po.partition
      assert v4_po.offset == v5_po.offset
      assert v4_po.error_code == v5_po.error_code
      assert v4_po.metadata == v5_po.metadata
    end
  end

  describe "V6 Response - tagged_fields edge cases" do
    test "parses response with non-empty tagged_fields at top level" do
      response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 0,
        error_code: 0,
        tagged_fields: [{0, <<1, 2, 3>>}],
        topics: [
          %{
            name: "tagged-topic",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_leader_epoch: 2,
                metadata: "meta",
                error_code: 0,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      partition_offset = hd(result.partition_offsets)
      assert partition_offset.offset == 100
      assert partition_offset.leader_epoch == 2
    end

    test "parses response with non-empty tagged_fields at partition level" do
      response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 0,
        error_code: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "partition-tagged",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 200,
                committed_leader_epoch: 4,
                metadata: "",
                error_code: 0,
                tagged_fields: [{0, <<10, 20>>}, {1, <<30>>}]
              }
            ],
            tagged_fields: []
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      partition_offset = hd(result.partition_offsets)
      assert partition_offset.offset == 200
      assert partition_offset.leader_epoch == 4
      assert partition_offset.metadata == ""
    end

    test "parses response with non-empty tagged_fields at topic level" do
      response = %Kayrock.OffsetFetch.V6.Response{
        throttle_time_ms: 0,
        error_code: 0,
        tagged_fields: [],
        topics: [
          %{
            name: "topic-tagged",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 300,
                committed_leader_epoch: 6,
                metadata: "x",
                error_code: 0,
                tagged_fields: []
              }
            ],
            tagged_fields: [{0, <<99>>}]
          }
        ]
      }

      assert {:ok, [result]} = OffsetFetch.Response.parse_response(response)
      partition_offset = hd(result.partition_offsets)
      assert partition_offset.offset == 300
      assert partition_offset.leader_epoch == 6
    end
  end
end
