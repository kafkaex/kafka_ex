defmodule KafkaEx.New.Protocols.Kayrock.OffsetCommit.V1RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetCommit
  alias Kayrock.OffsetCommit.V1

  describe "build_request/2 for V1" do
    test "builds request with generation_id and member_id" do
      request = %V1.Request{}

      opts = [
        group_id: "consumer-group",
        generation_id: 5,
        member_id: "consumer-1",
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 100, timestamp: 1_234_567_890}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group",
               generation_id: 5,
               member_id: "consumer-1",
               topics: [
                 %{
                   topic: "test-topic",
                   partitions: [
                     %{partition: 0, offset: 100, timestamp: 1_234_567_890, metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with default generation_id and member_id" do
      request = %V1.Request{}

      opts = [
        group_id: "simple-group",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "simple-group",
               generation_id: -1,
               member_id: "",
               topics: [
                 %{
                   topic: "topic-1",
                   partitions: [
                     %{partition: 0, offset: 42, timestamp: -1, metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with timestamp and metadata" do
      request = %V1.Request{}

      opts = [
        group_id: "v1-group",
        generation_id: 10,
        member_id: "member-abc",
        topics: [
          {"topic-1",
           [%{partition_num: 0, offset: 500, timestamp: 9_999_999, metadata: "custom-meta"}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v1-group",
               generation_id: 10,
               member_id: "member-abc",
               topics: [
                 %{
                   topic: "topic-1",
                   partitions: [
                     %{partition: 0, offset: 500, timestamp: 9_999_999, metadata: "custom-meta"}
                   ]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %V1.Request{}

      opts = [
        group_id: "multi-part-group",
        generation_id: 3,
        member_id: "consumer-x",
        topics: [
          {"topic-1",
           [
             %{partition_num: 0, offset: 10, timestamp: 100},
             %{partition_num: 1, offset: 20, timestamp: 200},
             %{partition_num: 2, offset: 30, timestamp: 300}
           ]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result.generation_id == 3
      assert result.member_id == "consumer-x"
      topic = hd(result.topics)
      assert length(topic.partitions) == 3
      assert Enum.at(topic.partitions, 0) == %{partition: 0, offset: 10, timestamp: 100, metadata: ""}
      assert Enum.at(topic.partitions, 1) == %{partition: 1, offset: 20, timestamp: 200, metadata: ""}
      assert Enum.at(topic.partitions, 2) == %{partition: 2, offset: 30, timestamp: 300, metadata: ""}
    end

    test "builds request with multiple topics" do
      request = %V1.Request{}

      opts = [
        group_id: "multi-topic-v1",
        generation_id: 7,
        member_id: "member-123",
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100, timestamp: 1000}]},
          {"topic-b", [%{partition_num: 0, offset: 200, timestamp: 2000}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert length(result.topics) == 2
      assert Enum.at(result.topics, 0).topic == "topic-a"
      assert Enum.at(result.topics, 1).topic == "topic-b"
    end
  end
end
