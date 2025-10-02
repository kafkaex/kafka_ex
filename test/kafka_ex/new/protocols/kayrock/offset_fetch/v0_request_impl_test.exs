defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch.V0RequestImplTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch
  alias Kayrock.OffsetFetch.V0

  describe "build_request/2 for V0" do
    test "builds request with single topic and partition" do
      request = %V0.Request{}

      opts = [
        group_id: "test-group",
        topics: [
          {"test-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               topics: [
                 %{
                   topic: "test-topic",
                   partitions: [%{partition: 0}]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %V0.Request{}

      opts = [
        group_id: "consumer-group",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group",
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
      request = %V0.Request{}

      opts = [
        group_id: "multi-topic-group",
        topics: [
          {"topic-1", [%{partition_num: 0}]},
          {"topic-2", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-topic-group",
               topics: [
                 %{
                   topic: "topic-1",
                   partitions: [%{partition: 0}]
                 },
                 %{
                   topic: "topic-2",
                   partitions: [%{partition: 0}, %{partition: 1}]
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

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "empty-group",
               topics: []
             }
    end
  end
end
