defmodule KafkaEx.New.Adapter.ListOffsetsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.Offset.Response, as: OffsetResponse
  alias Kayrock.ListOffsets

  describe "list_offsets_request/3 - creates Kayrock V1 request" do
    test "creates request for latest offset" do
      request = Adapter.list_offsets_request("test-topic", 0, :latest)

      assert request == %ListOffsets.V1.Request{
               replica_id: -1,
               topics: [
                 %{
                   topic: "test-topic",
                   partitions: [%{partition: 0, timestamp: -1}]
                 }
               ]
             }
    end

    test "creates request for earliest offset" do
      request = Adapter.list_offsets_request("test-topic", 0, :earliest)

      assert request == %ListOffsets.V1.Request{
               replica_id: -1,
               topics: [
                 %{
                   topic: "test-topic",
                   partitions: [%{partition: 0, timestamp: -2}]
                 }
               ]
             }
    end

    test "creates request with specific timestamp" do
      # Timestamp for 2020-01-01 00:00:00 UTC in milliseconds
      timestamp = {{2020, 1, 1}, {0, 0, 0}}
      expected_millis = 1_577_836_800_000

      request = Adapter.list_offsets_request("test-topic", 0, timestamp)

      [topic] = request.topics
      [partition] = topic.partitions
      assert partition.timestamp == expected_millis
    end

    test "creates request for different partitions" do
      request_p0 = Adapter.list_offsets_request("topic", 0, :latest)
      request_p1 = Adapter.list_offsets_request("topic", 1, :latest)
      request_p5 = Adapter.list_offsets_request("topic", 5, :latest)

      [topic_p0] = request_p0.topics
      [partition_p0] = topic_p0.partitions
      assert partition_p0.partition == 0

      [topic_p1] = request_p1.topics
      [partition_p1] = topic_p1.partitions
      assert partition_p1.partition == 1

      [topic_p5] = request_p5.topics
      [partition_p5] = topic_p5.partitions
      assert partition_p5.partition == 5
    end

    test "sets replica_id to -1" do
      request = Adapter.list_offsets_request("topic", 0, :latest)

      assert request.replica_id == -1
    end

    test "handles various topic names" do
      topics = ["simple", "with-dashes", "with.dots", "with_underscores"]

      for topic_name <- topics do
        request = Adapter.list_offsets_request(topic_name, 0, :latest)

        [topic] = request.topics
        assert topic.topic == topic_name
      end
    end
  end

  describe "list_offsets_response/1 - new to legacy API" do
    test "converts successful V1 response to legacy format" do
      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 12345, timestamp: 1_234_567_890}
            ]
          }
        ]
      }

      [legacy_response] = Adapter.list_offsets_response(kayrock_response)

      assert %OffsetResponse{
               topic: "test-topic",
               partition_offsets: [
                 %{
                   partition: 0,
                   error_code: :no_error,
                   offset: [12345]
                 }
               ]
             } = legacy_response
    end

    test "converts response with offset wrapped in list" do
      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 999, timestamp: 0}
            ]
          }
        ]
      }

      [response] = Adapter.list_offsets_response(kayrock_response)

      [partition_offset] = response.partition_offsets
      assert partition_offset.offset == [999]
    end

    test "converts response with multiple partitions" do
      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "multi-partition-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 100, timestamp: 0},
              %{partition: 1, error_code: 0, offset: 200, timestamp: 0},
              %{partition: 2, error_code: 0, offset: 300, timestamp: 0}
            ]
          }
        ]
      }

      [response] = Adapter.list_offsets_response(kayrock_response)

      assert response.topic == "multi-partition-topic"
      assert length(response.partition_offsets) == 3

      offsets =
        response.partition_offsets
        |> Enum.sort_by(& &1.partition)
        |> Enum.map(& &1.offset)

      assert offsets == [[100], [200], [300]]
    end

    test "converts response with multiple topics" do
      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "topic-1",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 100, timestamp: 0}
            ]
          },
          %{
            topic: "topic-2",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 200, timestamp: 0}
            ]
          }
        ]
      }

      responses = Adapter.list_offsets_response(kayrock_response)

      assert length(responses) == 2
      topics = Enum.map(responses, & &1.topic) |> Enum.sort()
      assert topics == ["topic-1", "topic-2"]
    end

    test "converts error response (unknown_topic_or_partition)" do
      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "unknown-topic",
            partition_responses: [
              %{partition: 0, error_code: 3, offset: -1, timestamp: -1}
            ]
          }
        ]
      }

      [response] = Adapter.list_offsets_response(kayrock_response)

      [partition_offset] = response.partition_offsets
      assert partition_offset.error_code == :unknown_topic_or_partition
    end

    test "converts error response (leader_not_available)" do
      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 5, offset: -1, timestamp: -1}
            ]
          }
        ]
      }

      [response] = Adapter.list_offsets_response(kayrock_response)

      [partition_offset] = response.partition_offsets
      assert partition_offset.error_code == :leader_not_available
    end

    test "converts error response (not_leader_for_partition)" do
      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 6, offset: -1, timestamp: -1}
            ]
          }
        ]
      }

      [response] = Adapter.list_offsets_response(kayrock_response)

      [partition_offset] = response.partition_offsets
      assert partition_offset.error_code == :not_leader_for_partition
    end

    test "offset 0 is preserved" do
      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "empty-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 0, timestamp: 0}
            ]
          }
        ]
      }

      [response] = Adapter.list_offsets_response(kayrock_response)

      [partition_offset] = response.partition_offsets
      assert partition_offset.offset == [0]
    end

    test "large offset is preserved" do
      large_offset = 9_223_372_036_854_775_807

      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "large-offset-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: large_offset, timestamp: 0}
            ]
          }
        ]
      }

      [response] = Adapter.list_offsets_response(kayrock_response)

      [partition_offset] = response.partition_offsets
      assert partition_offset.offset == [large_offset]
    end
  end

  describe "round-trip conversion" do
    test "request -> response -> extract offset" do
      # Create request
      request = Adapter.list_offsets_request("test-topic", 0, :latest)

      assert request.topics == [
               %{topic: "test-topic", partitions: [%{partition: 0, timestamp: -1}]}
             ]

      # Simulate response
      kayrock_response = %ListOffsets.V1.Response{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0, offset: 12345, timestamp: 0}
            ]
          }
        ]
      }

      legacy_responses = Adapter.list_offsets_response(kayrock_response)

      # Use legacy Response.extract_offset
      offset = OffsetResponse.extract_offset(legacy_responses)
      assert offset == 12345
    end
  end
end
