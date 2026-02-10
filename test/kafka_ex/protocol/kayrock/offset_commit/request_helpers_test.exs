defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts group_id" do
      opts = [group_id: "my-group"]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.group_id == "my-group"
    end

    test "raises on missing group_id" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields([])
      end
    end
  end

  describe "extract_coordination_fields/1" do
    test "extracts generation_id and member_id" do
      opts = [generation_id: 5, member_id: "member-123"]

      result = RequestHelpers.extract_coordination_fields(opts)

      assert result.generation_id == 5
      assert result.member_id == "member-123"
    end

    test "uses defaults when not provided" do
      result = RequestHelpers.extract_coordination_fields([])

      assert result.generation_id == -1
      assert result.member_id == ""
    end
  end

  describe "extract_retention_time/1" do
    test "extracts retention_time" do
      opts = [retention_time: 86_400_000]

      result = RequestHelpers.extract_retention_time(opts)

      assert result.retention_time == 86_400_000
    end

    test "defaults to -1 when not provided" do
      result = RequestHelpers.extract_retention_time([])

      assert result.retention_time == -1
    end
  end

  describe "build_topics/2" do
    test "builds topics structure without timestamp" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100}]},
          {"topic2", [%{partition_num: 1, offset: 200}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts, false)

      assert length(result) == 2
      [topic1, topic2] = result

      assert topic1.topic == "topic1"
      [p1] = topic1.partitions
      assert p1.partition == 0
      assert p1.offset == 100
      refute Map.has_key?(p1, :timestamp)

      assert topic2.topic == "topic2"
    end

    test "builds topics structure with timestamp" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100, timestamp: 1_234_567_890}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts, true)

      [topic] = result
      [partition] = topic.partitions
      assert partition.timestamp == 1_234_567_890
    end

    test "uses default timestamp when include_timestamp is true but not provided" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts, true)

      [topic] = result
      [partition] = topic.partitions
      assert partition.timestamp == -1
    end

    test "raises on missing topics" do
      assert_raise KeyError, fn ->
        RequestHelpers.build_topics([], false)
      end
    end
  end

  describe "build_partitions/2" do
    test "builds partition list without timestamp" do
      partitions = [
        %{partition_num: 0, offset: 100, metadata: "meta1"},
        %{partition_num: 1, offset: 200}
      ]

      result = RequestHelpers.build_partitions(partitions, false)

      assert length(result) == 2
      [p1, p2] = result

      assert p1.partition == 0
      assert p1.offset == 100
      assert p1.metadata == "meta1"

      assert p2.partition == 1
      assert p2.offset == 200
      assert p2.metadata == ""
    end

    test "builds partition list with timestamp" do
      partitions = [%{partition_num: 0, offset: 100, timestamp: 9999}]

      result = RequestHelpers.build_partitions(partitions, true)

      [p] = result
      assert p.timestamp == 9999
    end
  end

  describe "build_v2_v3_request/2" do
    test "builds complete request with all fields" do
      template = %{}

      opts = [
        group_id: "test-group",
        generation_id: 3,
        member_id: "member-1",
        retention_time: 60_000,
        topics: [{"topic1", [%{partition_num: 0, offset: 50}]}]
      ]

      result = RequestHelpers.build_v2_v3_request(template, opts)

      assert result.group_id == "test-group"
      assert result.generation_id == 3
      assert result.member_id == "member-1"
      assert result.retention_time == 60_000
      assert length(result.topics) == 1
    end

    test "uses defaults for optional fields" do
      template = %{}

      opts = [
        group_id: "test-group",
        topics: [{"topic1", [%{partition_num: 0, offset: 50}]}]
      ]

      result = RequestHelpers.build_v2_v3_request(template, opts)

      assert result.group_id == "test-group"
      assert result.generation_id == -1
      assert result.member_id == ""
      assert result.retention_time == -1
    end
  end
end
