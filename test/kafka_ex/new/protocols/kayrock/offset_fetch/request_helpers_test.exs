defmodule KafkaEx.New.Protocols.Kayrock.OffsetFetch.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetFetch.RequestHelpers

  describe "extract_common_fields/1" do
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

  describe "build_partitions/1" do
    test "builds partition list with partition numbers" do
      partitions = [
        %{partition_num: 0},
        %{partition_num: 1},
        %{partition_num: 5}
      ]

      result = RequestHelpers.build_partitions(partitions)

      assert [
               %{partition: 0},
               %{partition: 1},
               %{partition: 5}
             ] = result
    end

    test "handles single partition" do
      partitions = [%{partition_num: 0}]

      result = RequestHelpers.build_partitions(partitions)

      assert [%{partition: 0}] = result
    end

    test "ignores extra fields in partition data" do
      partitions = [
        %{partition_num: 0, offset: 100, metadata: "ignored"}
      ]

      result = RequestHelpers.build_partitions(partitions)

      assert [%{partition: 0}] = result
      refute Map.has_key?(hd(result), :offset)
      refute Map.has_key?(hd(result), :metadata)
    end
  end

  describe "build_topics/1" do
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
                 topic: "topic1",
                 partitions: [%{partition: 0}]
               },
               %{
                 topic: "topic2",
                 partitions: [%{partition: 0}, %{partition: 1}]
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
                 topic: "my-topic",
                 partitions: [
                   %{partition: 0},
                   %{partition: 1},
                   %{partition: 2}
                 ]
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
                 topic: "simple-topic",
                 partitions: [%{partition: 0}]
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
                 topic: "empty-topic",
                 partitions: []
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
               %{topic: "topic-a", partitions: [%{partition: 10}, %{partition: 20}]},
               %{topic: "topic-b", partitions: [%{partition: 99}]}
             ] = result
    end
  end
end
