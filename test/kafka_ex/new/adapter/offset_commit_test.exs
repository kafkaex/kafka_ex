defmodule KafkaEx.New.Adapter.OffsetCommitTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.OffsetCommit.Response, as: OffsetCommitResponse
  alias Kayrock.OffsetCommit

  describe "offset_commit_request/2 - legacy to new API (V0)" do
    test "converts legacy OffsetCommitRequest to Kayrock V0 request" do
      legacy_request = %OffsetCommitRequest{
        consumer_group: "test-group",
        topic: "test-topic",
        partition: 0,
        offset: 12345,
        metadata: "",
        api_version: 0
      }

      {kayrock_request, consumer_group} = Adapter.offset_commit_request(legacy_request, nil)

      assert consumer_group == "test-group"

      assert %OffsetCommit.V0.Request{} = kayrock_request
      assert kayrock_request.group_id == "test-group"
      assert [topic] = kayrock_request.topics
      assert topic.topic == "test-topic"
      assert [partition] = topic.partitions
      assert partition.partition == 0
      assert partition.offset == 12345
      assert partition.metadata == ""
    end

    test "uses client_consumer_group when request consumer_group is nil" do
      legacy_request = %OffsetCommitRequest{
        consumer_group: nil,
        topic: "topic",
        partition: 0,
        offset: 100,
        api_version: 0
      }

      {kayrock_request, consumer_group} =
        Adapter.offset_commit_request(legacy_request, "client-default-group")

      assert consumer_group == "client-default-group"
      assert kayrock_request.group_id == "client-default-group"
    end

    test "prefers request consumer_group over client_consumer_group" do
      legacy_request = %OffsetCommitRequest{
        consumer_group: "request-group",
        topic: "topic",
        partition: 0,
        offset: 100,
        api_version: 0
      }

      {kayrock_request, consumer_group} =
        Adapter.offset_commit_request(legacy_request, "client-default-group")

      assert consumer_group == "request-group"
      assert kayrock_request.group_id == "request-group"
    end

    test "handles different partitions" do
      for partition <- [0, 1, 5, 99] do
        legacy_request = %OffsetCommitRequest{
          consumer_group: "group",
          topic: "topic",
          partition: partition,
          offset: 100,
          api_version: 0
        }

        {kayrock_request, _} = Adapter.offset_commit_request(legacy_request, nil)

        [topic] = kayrock_request.topics
        [part] = topic.partitions
        assert part.partition == partition
      end
    end

    test "handles large offsets" do
      large_offset = 9_223_372_036_854_775_807

      legacy_request = %OffsetCommitRequest{
        consumer_group: "group",
        topic: "topic",
        partition: 0,
        offset: large_offset,
        api_version: 0
      }

      {kayrock_request, _} = Adapter.offset_commit_request(legacy_request, nil)

      [topic] = kayrock_request.topics
      [partition] = topic.partitions
      assert partition.offset == large_offset
    end
  end

  describe "offset_commit_request/2 - legacy to new API (V1)" do
    test "converts legacy OffsetCommitRequest to Kayrock V1 request with timestamp" do
      legacy_request = %OffsetCommitRequest{
        consumer_group: "v1-group",
        topic: "v1-topic",
        partition: 1,
        offset: 500,
        metadata: "",
        api_version: 1,
        generation_id: 5,
        member_id: "consumer-123",
        timestamp: 1_234_567_890_000
      }

      {kayrock_request, consumer_group} = Adapter.offset_commit_request(legacy_request, nil)

      assert consumer_group == "v1-group"
      assert %OffsetCommit.V1.Request{} = kayrock_request
      assert kayrock_request.group_id == "v1-group"
      assert kayrock_request.generation_id == 5
      assert kayrock_request.member_id == "consumer-123"

      [topic] = kayrock_request.topics
      [partition] = topic.partitions
      assert partition.timestamp == 1_234_567_890_000
    end

    test "V1 request uses current time when timestamp is 0 or negative" do
      legacy_request = %OffsetCommitRequest{
        consumer_group: "group",
        topic: "topic",
        partition: 0,
        offset: 100,
        api_version: 1,
        generation_id: 1,
        member_id: "member",
        timestamp: 0
      }

      before_time = :os.system_time(:millisecond)
      {kayrock_request, _} = Adapter.offset_commit_request(legacy_request, nil)
      after_time = :os.system_time(:millisecond)

      [topic] = kayrock_request.topics
      [partition] = topic.partitions
      assert partition.timestamp >= before_time
      assert partition.timestamp <= after_time
    end
  end

  describe "offset_commit_request/2 - legacy to new API (V2+)" do
    test "converts legacy OffsetCommitRequest to Kayrock V2 request" do
      legacy_request = %OffsetCommitRequest{
        consumer_group: "v2-group",
        topic: "v2-topic",
        partition: 2,
        offset: 1000,
        metadata: "",
        api_version: 2,
        generation_id: 10,
        member_id: "consumer-v2"
      }

      {kayrock_request, consumer_group} = Adapter.offset_commit_request(legacy_request, nil)

      assert consumer_group == "v2-group"
      assert %OffsetCommit.V2.Request{} = kayrock_request
      assert kayrock_request.group_id == "v2-group"
      assert kayrock_request.generation_id == 10
      assert kayrock_request.member_id == "consumer-v2"
      assert kayrock_request.retention_time == -1
    end

    test "V2 request does not include timestamp in partitions" do
      legacy_request = %OffsetCommitRequest{
        consumer_group: "group",
        topic: "topic",
        partition: 0,
        offset: 100,
        api_version: 2,
        generation_id: 1,
        member_id: "member"
      }

      {kayrock_request, _} = Adapter.offset_commit_request(legacy_request, nil)

      [topic] = kayrock_request.topics
      [partition] = topic.partitions
      refute Map.has_key?(partition, :timestamp)
    end
  end

  describe "offset_commit_response/1 - new to legacy API" do
    test "converts successful response to legacy format" do
      kayrock_response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      [legacy_response] = Adapter.offset_commit_response(kayrock_response)

      assert %OffsetCommitResponse{
               topic: "test-topic",
               partitions: [
                 %{
                   partition: 0,
                   error_code: :no_error
                 }
               ]
             } = legacy_response
    end

    test "converts error response (offset_metadata_too_large)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 12}
            ]
          }
        ]
      }

      [response] = Adapter.offset_commit_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :offset_metadata_too_large
    end

    test "converts error response (coordinator_load_in_progress)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 14}
            ]
          }
        ]
      }

      [response] = Adapter.offset_commit_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :coordinator_load_in_progress
    end

    test "converts error response (coordinator_not_available)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 15}
            ]
          }
        ]
      }

      [response] = Adapter.offset_commit_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :coordinator_not_available
    end

    test "converts error response (not_coordinator)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 16}
            ]
          }
        ]
      }

      [response] = Adapter.offset_commit_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :not_coordinator
    end

    test "converts error response (illegal_generation)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 22}
            ]
          }
        ]
      }

      [response] = Adapter.offset_commit_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :illegal_generation
    end

    test "converts error response (unknown_member_id)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 25}
            ]
          }
        ]
      }

      [response] = Adapter.offset_commit_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :unknown_member_id
    end

    test "converts error response (rebalance_in_progress)" do
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 27}
            ]
          }
        ]
      }

      [response] = Adapter.offset_commit_response(kayrock_response)

      [partition] = response.partitions
      assert partition.error_code == :rebalance_in_progress
    end
  end

  describe "round-trip conversion" do
    test "legacy request -> kayrock -> legacy response maintains structure" do
      # Start with legacy request
      legacy_request = %OffsetCommitRequest{
        consumer_group: "test-group",
        topic: "test-topic",
        partition: 0,
        offset: 12345,
        api_version: 0
      }

      # Convert to Kayrock format
      {kayrock_request, consumer_group} = Adapter.offset_commit_request(legacy_request, nil)

      # Verify conversion
      assert consumer_group == "test-group"
      assert kayrock_request.group_id == "test-group"

      [topic] = kayrock_request.topics
      assert topic.topic == "test-topic"

      [partition] = topic.partitions
      assert partition.partition == 0
      assert partition.offset == 12345

      # Simulate successful Kayrock response
      kayrock_response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      # Convert back to legacy
      [legacy_response] = Adapter.offset_commit_response(kayrock_response)

      assert %OffsetCommitResponse{
               topic: "test-topic",
               partitions: [
                 %{
                   partition: 0,
                   error_code: :no_error
                 }
               ]
             } = legacy_response
    end

    test "V2 request maintains generation and member info through round-trip" do
      # V2 request with consumer group coordination
      legacy_request = %OffsetCommitRequest{
        consumer_group: "coordinated-group",
        topic: "topic",
        partition: 0,
        offset: 500,
        api_version: 2,
        generation_id: 42,
        member_id: "member-uuid-123"
      }

      {kayrock_request, consumer_group} = Adapter.offset_commit_request(legacy_request, nil)

      # Verify coordination info is preserved
      assert consumer_group == "coordinated-group"
      assert kayrock_request.generation_id == 42
      assert kayrock_request.member_id == "member-uuid-123"

      # Simulate successful response
      kayrock_response = %{
        responses: [
          %{
            topic: "topic",
            partition_responses: [
              %{partition: 0, error_code: 0}
            ]
          }
        ]
      }

      [legacy_response] = Adapter.offset_commit_response(kayrock_response)

      assert legacy_response.topic == "topic"
      [partition] = legacy_response.partitions
      assert partition.error_code == :no_error
    end
  end
end
