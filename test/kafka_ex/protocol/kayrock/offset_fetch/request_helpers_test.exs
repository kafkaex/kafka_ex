defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetFetch.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts group_id" do
      opts = [group_id: "my-consumer-group"]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == "my-consumer-group"
    end

    test "raises on missing group_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields([])
      end
    end
  end

  describe "build_topics/1" do
    test "builds topics structure from single topic" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0}, %{partition_num: 1}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert length(result) == 1
      [topic] = result

      assert topic.topic == "topic1"
      assert length(topic.partitions) == 2
      [p0, p1] = topic.partitions
      assert p0.partition == 0
      assert p1.partition == 1
    end

    test "builds topics structure from multiple topics" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0}]},
          {"topic2", [%{partition_num: 0}, %{partition_num: 1}]},
          {"topic3", [%{partition_num: 2}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts)

      assert length(result) == 3
      topic_names = Enum.map(result, & &1.topic)
      assert topic_names == ["topic1", "topic2", "topic3"]
    end

    test "raises on missing topics" do
      assert_raise KeyError, fn ->
        RequestHelpers.build_topics([])
      end
    end
  end

  describe "build_partitions/1" do
    test "builds partition list from partition data" do
      partitions = [
        %{partition_num: 0},
        %{partition_num: 1},
        %{partition_num: 5}
      ]

      result = RequestHelpers.build_partitions(partitions)

      assert length(result) == 3
      partition_nums = Enum.map(result, & &1.partition)
      assert partition_nums == [0, 1, 5]
    end

    test "returns empty list for empty input" do
      result = RequestHelpers.build_partitions([])

      assert result == []
    end
  end
end
