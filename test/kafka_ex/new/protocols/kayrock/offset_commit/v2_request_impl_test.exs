defmodule KafkaEx.New.Protocols.Kayrock.OffsetCommit.V2RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetCommit
  alias Kayrock.OffsetCommit.V2

  describe "build_request/2 for V2" do
    test "builds request with retention_time" do
      request = %V2.Request{}

      opts = [
        group_id: "v2-group",
        generation_id: 10,
        member_id: "member-v2",
        retention_time: 86_400_000,
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v2-group",
               generation_id: 10,
               member_id: "member-v2",
               retention_time: 86_400_000,
               topics: [
                 %{
                   topic: "test-topic",
                   partitions: [
                     %{partition: 0, offset: 100, metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with default retention_time" do
      request = %V2.Request{}

      opts = [
        group_id: "default-retention-group",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %V2.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "default-retention-group",
               generation_id: -1,
               member_id: "",
               retention_time: -1,
               topics: [
                 %{
                   topic: "topic-1",
                   partitions: [
                     %{partition: 0, offset: 42, metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request without timestamp field (managed by Kafka)" do
      request = %V2.Request{}

      opts = [
        group_id: "kafka-managed-group",
        generation_id: 5,
        member_id: "consumer-1",
        retention_time: 3_600_000,
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 200, metadata: "meta-1"}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      partition = hd(hd(result.topics).partitions)
      assert partition.partition == 0
      assert partition.offset == 200
      assert partition.metadata == "meta-1"
      # Note: V2 does not have timestamp field
      refute Map.has_key?(partition, :timestamp)
    end

    test "builds request with multiple partitions" do
      request = %V2.Request{}

      opts = [
        group_id: "multi-v2",
        generation_id: 8,
        member_id: "member-8",
        retention_time: 7_200_000,
        topics: [
          {"topic-1",
           [
             %{partition_num: 0, offset: 10},
             %{partition_num: 1, offset: 20},
             %{partition_num: 2, offset: 30}
           ]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.retention_time == 7_200_000
      topic = hd(result.topics)
      assert length(topic.partitions) == 3
    end

    test "builds request with multiple topics" do
      request = %V2.Request{}

      opts = [
        group_id: "multi-topic-v2",
        generation_id: 12,
        member_id: "member-12",
        retention_time: 86_400_000,
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100}]},
          {"topic-b", [%{partition_num: 0, offset: 200}, %{partition_num: 1, offset: 300}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert length(result.topics) == 2
      assert Enum.at(result.topics, 0).topic == "topic-a"
      assert Enum.at(result.topics, 1).topic == "topic-b"
    end
  end
end
