defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch.V1RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch
  alias Kayrock.OffsetFetch.V1

  describe "build_request/2 for V1" do
    test "builds request with single topic and partition" do
      request = %V1.Request{}

      opts = [
        group_id: "coordinator-group",
        topics: [
          {"kafka-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "coordinator-group",
               topics: [
                 %{
                   topic: "kafka-topic",
                   partitions: [%{partition: 0}]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %V1.Request{}

      opts = [
        group_id: "v1-group",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v1-group",
               topics: [
                 %{
                   topic: "topic-1",
                   partitions: [
                     %{partition: 0},
                     %{partition: 1},
                     %{partition: 2}
                   ]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %V1.Request{}

      opts = [
        group_id: "multi-v1-group",
        topics: [
          {"topic-a", [%{partition_num: 0}]},
          {"topic-b", [%{partition_num: 0}, %{partition_num: 1}]},
          {"topic-c", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-v1-group",
               topics: [
                 %{
                   topic: "topic-a",
                   partitions: [%{partition: 0}]
                 },
                 %{
                   topic: "topic-b",
                   partitions: [%{partition: 0}, %{partition: 1}]
                 },
                 %{
                   topic: "topic-c",
                   partitions: [%{partition: 0}]
                 }
               ]
             }
    end
  end
end
