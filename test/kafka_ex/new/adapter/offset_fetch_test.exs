defmodule KafkaEx.New.Adapter.OffsetFetchTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.OffsetFetch.Request, as: OffsetFetchRequest
  alias KafkaEx.Protocol.OffsetFetch.Response, as: OffsetFetchResponse
  alias Kayrock.OffsetFetch

  describe "offset_fetch_request/2 - legacy to new API" do
    test "converts legacy OffsetFetchRequest to Kayrock request with V0" do
      legacy_request = %OffsetFetchRequest{
        consumer_group: "test-group",
        topic: "test-topic",
        partition: 0,
        api_version: 0
      }

      {kayrock_request, consumer_group} = Adapter.offset_fetch_request(legacy_request, nil)

      assert consumer_group == "test-group"

      assert kayrock_request == %OffsetFetch.V0.Request{
               group_id: "test-group",
               topics: [
                 %{
                   topic: "test-topic",
                   partitions: [%{partition: 0}]
                 }
               ]
             }
    end

    test "converts request with V1 api_version" do
      legacy_request = %OffsetFetchRequest{
        consumer_group: "v1-group",
        topic: "v1-topic",
        partition: 1,
        api_version: 1
      }

      {kayrock_request, consumer_group} = Adapter.offset_fetch_request(legacy_request, nil)

      assert consumer_group == "v1-group"
      assert %OffsetFetch.V1.Request{} = kayrock_request
      assert kayrock_request.group_id == "v1-group"
    end

    test "converts request with V2 api_version" do
      legacy_request = %OffsetFetchRequest{
        consumer_group: "v2-group",
        topic: "v2-topic",
        partition: 2,
        api_version: 2
      }

      {kayrock_request, consumer_group} = Adapter.offset_fetch_request(legacy_request, nil)

      assert consumer_group == "v2-group"
      assert %OffsetFetch.V2.Request{} = kayrock_request
    end

    test "uses client_consumer_group when request consumer_group is nil" do
      legacy_request = %OffsetFetchRequest{
        consumer_group: nil,
        topic: "topic",
        partition: 0,
        api_version: 0
      }

      {kayrock_request, consumer_group} =
        Adapter.offset_fetch_request(legacy_request, "client-default-group")

      assert consumer_group == "client-default-group"
      assert kayrock_request.group_id == "client-default-group"
    end

    test "prefers request consumer_group over client_consumer_group" do
      legacy_request = %OffsetFetchRequest{
        consumer_group: "request-group",
        topic: "topic",
        partition: 0,
        api_version: 0
      }

      {kayrock_request, consumer_group} =
        Adapter.offset_fetch_request(legacy_request, "client-default-group")

      assert consumer_group == "request-group"
      assert kayrock_request.group_id == "request-group"
    end

    test "handles different partitions" do
      for partition <- [0, 1, 5, 99] do
        legacy_request = %OffsetFetchRequest{
          consumer_group: "group",
          topic: "topic",
          partition: partition,
          api_version: 0
        }

        {kayrock_request, _} = Adapter.offset_fetch_request(legacy_request, nil)

        [topic] = kayrock_request.topics
        [part] = topic.partitions
        assert part.partition == partition
      end
    end

    test "handles various topic names" do
      topics = ["simple", "with-dashes", "with.dots", "with_underscores"]

      for topic_name <- topics do
        legacy_request = %OffsetFetchRequest{
          consumer_group: "group",
          topic: topic_name,
          partition: 0,
          api_version: 0
        }

        {kayrock_request, _} = Adapter.offset_fetch_request(legacy_request, nil)

        [topic] = kayrock_request.topics
        assert topic.topic == topic_name
      end
    end
  end

  describe "offset_fetch_response/1 - new to legacy API" do
    test "converts successful response to legacy format" do
      kayrock_response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{
                partition: 0,
                offset: 12345,
                metadata: "consumer-metadata",
                error_code: 0
              }
            ]
          }
        ]
      }

      [legacy_response] = Adapter.offset_fetch_response(kayrock_response)

      assert %OffsetFetchResponse{
               topic: "test-topic",
               partitions: [
                 %{
                   partition: 0,
                   offset: 12345,
                   metadata: "consumer-metadata",
                   error_code: :no_error
                 }
               ]
             } = legacy_response
    end

    test "converts response with offset -1 (no committed offset)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "new-consumer-topic",
            partition_responses: [
              %{
                partition: 0,
                offset: -1,
                metadata: "",
                error_code: 0
              }
            ]
          }
        ]
      }

      [response] = Adapter.offset_fetch_response(kayrock_response)

      [partition] = response.partitions
      assert partition.offset == -1
      assert partition.error_code == :no_error
    end

    test "converts response with empty metadata" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{
                partition: 0,
                offset: 100,
                metadata: "",
                error_code: 0
              }
            ]
          }
        ]
      }

      [response] = Adapter.offset_fetch_response(kayrock_response)

      [partition] = response.partitions
      assert partition.metadata == ""
    end

    test "converts error response (unknown_topic_or_partition)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "unknown-topic",
            partition_responses: [
              %{
                partition: 0,
                offset: -1,
                metadata: "",
                error_code: 3
              }
            ]
          }
        ]
      }

      [response] = Adapter.offset_fetch_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :unknown_topic_or_partition
    end

    test "converts error response (coordinator_not_available)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{
                partition: 0,
                offset: -1,
                metadata: "",
                error_code: 15
              }
            ]
          }
        ]
      }

      [response] = Adapter.offset_fetch_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :coordinator_not_available
    end

    test "converts error response (not_coordinator)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{
                partition: 0,
                offset: -1,
                metadata: "",
                error_code: 16
              }
            ]
          }
        ]
      }

      [response] = Adapter.offset_fetch_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :not_coordinator
    end

    test "preserves metadata string" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{
                partition: 0,
                offset: 100,
                metadata: "custom-consumer-metadata-123",
                error_code: 0
              }
            ]
          }
        ]
      }

      [response] = Adapter.offset_fetch_response(kayrock_response)

      [partition] = response.partitions
      assert partition.metadata == "custom-consumer-metadata-123"
    end

    test "handles large offsets" do
      large_offset = 9_223_372_036_854_775_807

      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{
                partition: 0,
                offset: large_offset,
                metadata: "",
                error_code: 0
              }
            ]
          }
        ]
      }

      [response] = Adapter.offset_fetch_response(kayrock_response)

      [partition] = response.partitions
      assert partition.offset == large_offset
    end
  end

  describe "round-trip conversion" do
    test "legacy request -> kayrock -> legacy response maintains structure" do
      # Start with legacy request
      legacy_request = %OffsetFetchRequest{
        consumer_group: "test-group",
        topic: "test-topic",
        partition: 0,
        api_version: 0
      }

      # Convert to Kayrock format
      {kayrock_request, consumer_group} = Adapter.offset_fetch_request(legacy_request, nil)

      # Verify conversion
      assert consumer_group == "test-group"
      assert kayrock_request.group_id == "test-group"
      assert [%{topic: "test-topic", partitions: [%{partition: 0}]}] = kayrock_request.topics

      # Simulate successful Kayrock response
      kayrock_response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{
                partition: 0,
                offset: 42,
                metadata: "test-metadata",
                error_code: 0
              }
            ]
          }
        ]
      }

      # Convert back to legacy
      [legacy_response] = Adapter.offset_fetch_response(kayrock_response)

      assert %OffsetFetchResponse{
               topic: "test-topic",
               partitions: [
                 %{
                   partition: 0,
                   offset: 42,
                   metadata: "test-metadata",
                   error_code: :no_error
                 }
               ]
             } = legacy_response
    end

    test "Response.last_offset/1 works correctly with converted response" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{
                partition: 0,
                offset: 12345,
                metadata: "",
                error_code: 0
              }
            ]
          }
        ]
      }

      legacy_responses = Adapter.offset_fetch_response(kayrock_response)

      offset = OffsetFetchResponse.last_offset(legacy_responses)
      assert offset == 12345
    end

    test "Response.last_offset/1 returns 0 for empty response" do
      offset = OffsetFetchResponse.last_offset([])
      assert offset == 0
    end
  end
end
