defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetFetch
  alias KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts group_id from opts" do
      opts = [group_id: "consumer-group", other: "value"]

      result = RequestHelpers.extract_common_fields(opts)

      assert %{group_id: "consumer-group"} = result
    end

    test "raises when group_id is missing" do
      opts = [other: "value"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end
  end

  describe "RequestHelpers.build_partition_indexes/1" do
    test "builds partition index list with partition numbers" do
      partitions = [
        %{partition_num: 0},
        %{partition_num: 1},
        %{partition_num: 5}
      ]

      result = RequestHelpers.build_partition_indexes(partitions)

      assert [0, 1, 5] = result
    end

    test "handles single partition" do
      partitions = [%{partition_num: 0}]

      result = RequestHelpers.build_partition_indexes(partitions)

      assert [0] = result
    end

    test "ignores extra fields in partition data" do
      partitions = [
        %{partition_num: 0, offset: 100, metadata: "ignored"}
      ]

      result = RequestHelpers.build_partition_indexes(partitions)

      assert [0] = result
    end
  end

  describe "RequestHelpers.build_topics/1" do
    test "builds topics structure with partitions" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0}]},
          {"topic2", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{
                 name: "topic1",
                 partition_indexes: [0]
               },
               %{
                 name: "topic2",
                 partition_indexes: [0, 1]
               }
             ] = result
    end

    test "builds topics with single topic and multiple partitions" do
      opts = [
        topics: [
          {"my-topic",
           [
             %{partition_num: 0},
             %{partition_num: 1},
             %{partition_num: 2}
           ]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{
                 name: "my-topic",
                 partition_indexes: [0, 1, 2]
               }
             ] = result
    end

    test "builds topics with single topic and single partition" do
      opts = [
        topics: [
          {"simple-topic", [%{partition_num: 0}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{
                 name: "simple-topic",
                 partition_indexes: [0]
               }
             ] = result
    end

    test "handles empty partitions list" do
      opts = [
        topics: [
          {"empty-topic", []}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{
                 name: "empty-topic",
                 partition_indexes: []
               }
             ] = result
    end

    test "raises when topics key is missing" do
      opts = [group_id: "test-group"]

      assert_raise KeyError, fn ->
        RequestHelpers.build_topics(opts)
      end
    end

    test "handles topics with various partition numbers" do
      opts = [
        topics: [
          {"topic-a", [%{partition_num: 10}, %{partition_num: 20}]},
          {"topic-b", [%{partition_num: 99}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert [
               %{name: "topic-a", partition_indexes: [10, 20]},
               %{name: "topic-b", partition_indexes: [99]}
             ] = result
    end
  end

  describe "V0 Request implementation" do
    test "builds request with single topic and partition" do
      request = %Kayrock.OffsetFetch.V0.Request{}

      opts = [
        group_id: "test-group",
        topics: [
          {"test-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "test-group",
               topics: [
                 %{
                   name: "test-topic",
                   partition_indexes: [0]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetFetch.V0.Request{}

      opts = [
        group_id: "consumer-group",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "consumer-group",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0, 1, 2]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetFetch.V0.Request{}

      opts = [
        group_id: "multi-topic-group",
        topics: [
          {"topic-1", [%{partition_num: 0}]},
          {"topic-2", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-topic-group",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0]
                 },
                 %{
                   name: "topic-2",
                   partition_indexes: [0, 1]
                 }
               ]
             }
    end

    test "builds request with empty topics list" do
      request = %Kayrock.OffsetFetch.V0.Request{}

      opts = [
        group_id: "empty-group",
        topics: []
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V0.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "empty-group",
               topics: []
             }
    end
  end

  describe "V1 Request implementation" do
    test "builds request with single topic and partition" do
      request = %Kayrock.OffsetFetch.V1.Request{}

      opts = [
        group_id: "coordinator-group",
        topics: [
          {"kafka-topic", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "coordinator-group",
               topics: [
                 %{
                   name: "kafka-topic",
                   partition_indexes: [0]
                 }
               ]
             }
    end

    test "builds request with multiple partitions" do
      request = %Kayrock.OffsetFetch.V1.Request{}

      opts = [
        group_id: "v1-group",
        topics: [
          {"topic-1", [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "v1-group",
               topics: [
                 %{
                   name: "topic-1",
                   partition_indexes: [0, 1, 2]
                 }
               ]
             }
    end

    test "builds request with multiple topics" do
      request = %Kayrock.OffsetFetch.V1.Request{}

      opts = [
        group_id: "multi-v1-group",
        topics: [
          {"topic-a", [%{partition_num: 0}]},
          {"topic-b", [%{partition_num: 0}, %{partition_num: 1}]},
          {"topic-c", [%{partition_num: 0}]}
        ]
      ]

      result = OffsetFetch.Request.build_request(request, opts)

      assert result == %Kayrock.OffsetFetch.V1.Request{
               client_id: nil,
               correlation_id: nil,
               group_id: "multi-v1-group",
               topics: [
                 %{
                   name: "topic-a",
                   partition_indexes: [0]
                 },
                 %{
                   name: "topic-b",
                   partition_indexes: [0, 1]
                 },
                 %{
                   name: "topic-c",
                   partition_indexes: [0]
                 }
               ]
             }
    end
  end
end
