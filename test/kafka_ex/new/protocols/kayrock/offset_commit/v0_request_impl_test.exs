defmodule KafkaEx.New.Protocols.Kayrock.OffsetCommit.V0RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetCommit
  alias Kayrock.OffsetCommit.V0

  describe "build_request/2 for V0" do
    test "builds request with single topic and partition" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group",
        topics: [
          {"test-topic", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
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

    test "builds request with metadata" do
      request = %V0.Request{}

      opts = [
        group_id: "consumer-group",
        topics: [
          {"topic-1", [%{partition_num: 0, offset: 42, metadata: "consumer-instance-1"}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group",
               topics: [
                 %{
                   topic: "topic-1",
                   partitions: [
                     %{partition: 0, offset: 42, metadata: "consumer-instance-1"}
                   ]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %V0.Request{}

      opts = [
        group_id: "multi-partition-group",
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

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-partition-group",
               topics: [
                 %{
                   topic: "topic-1",
                   partitions: [
                     %{partition: 0, offset: 10, metadata: ""},
                     %{partition: 1, offset: 20, metadata: ""},
                     %{partition: 2, offset: 30, metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %V0.Request{}

      opts = [
        group_id: "multi-topic-group",
        topics: [
          {"topic-a", [%{partition_num: 0, offset: 100}]},
          {"topic-b", [%{partition_num: 0, offset: 200}, %{partition_num: 1, offset: 300}]}
        ]
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-topic-group",
               topics: [
                 %{
                   topic: "topic-a",
                   partitions: [%{partition: 0, offset: 100, metadata: ""}]
                 },
                 %{
                   topic: "topic-b",
                   partitions: [
                     %{partition: 0, offset: 200, metadata: ""},
                     %{partition: 1, offset: 300, metadata: ""}
                   ]
                 }
               ]
             }
    end

    test "builds request with empty topics list" do
      request = %V0.Request{}

      opts = [
        group_id: "empty-group",
        topics: []
      ]

      result = OffsetCommit.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "empty-group",
               topics: []
             }
    end
  end
end
