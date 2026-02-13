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

      assert topic1.name == "topic1"
      [p1] = topic1.partitions
      assert p1.partition_index == 0
      assert p1.committed_offset == 100
      refute Map.has_key?(p1, :commit_timestamp)

      assert topic2.name == "topic2"
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
      assert partition.commit_timestamp == 1_234_567_890
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
      assert partition.commit_timestamp == -1
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

      assert p1.partition_index == 0
      assert p1.committed_offset == 100
      assert p1.committed_metadata == "meta1"

      assert p2.partition_index == 1
      assert p2.committed_offset == 200
      assert p2.committed_metadata == ""
    end

    test "builds partition list with timestamp" do
      partitions = [%{partition_num: 0, offset: 100, timestamp: 9999}]

      result = RequestHelpers.build_partitions(partitions, true)

      [p] = result
      assert p.commit_timestamp == 9999
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
      assert result.retention_time_ms == 60_000
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
      assert result.retention_time_ms == -1
    end
  end

  describe "extract_group_instance_id/1" do
    test "extracts group_instance_id when provided" do
      opts = [group_instance_id: "static-member-1"]

      result = RequestHelpers.extract_group_instance_id(opts)

      assert result == "static-member-1"
    end

    test "returns nil when not provided" do
      result = RequestHelpers.extract_group_instance_id([])

      assert result == nil
    end
  end

  describe "build_v5_request/2" do
    test "builds request without retention_time_ms" do
      template = %{}

      opts = [
        group_id: "v5-group",
        generation_id: 10,
        member_id: "member-v5",
        topics: [{"topic1", [%{partition_num: 0, offset: 100}]}]
      ]

      result = RequestHelpers.build_v5_request(template, opts)

      assert result.group_id == "v5-group"
      assert result.generation_id == 10
      assert result.member_id == "member-v5"
      refute Map.has_key?(result, :retention_time_ms)
      assert length(result.topics) == 1
    end

    test "uses defaults for optional fields" do
      template = %{}

      opts = [
        group_id: "v5-default",
        topics: [{"topic1", [%{partition_num: 0, offset: 50}]}]
      ]

      result = RequestHelpers.build_v5_request(template, opts)

      assert result.generation_id == -1
      assert result.member_id == ""
    end

    test "ignores retention_time in opts (not part of V5 schema)" do
      template = %{}

      opts = [
        group_id: "v5-ignore-retention",
        retention_time: 86_400_000,
        topics: [{"topic1", [%{partition_num: 0, offset: 50}]}]
      ]

      result = RequestHelpers.build_v5_request(template, opts)

      refute Map.has_key?(result, :retention_time_ms)
    end
  end

  describe "build_topics_with_leader_epoch/1" do
    test "builds topics with committed_leader_epoch per partition" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100, leader_epoch: 5}]},
          {"topic2", [%{partition_num: 1, offset: 200}]}
        ]
      ]

      result = RequestHelpers.build_topics_with_leader_epoch(opts)

      assert length(result) == 2
      [topic1, topic2] = result

      assert topic1.name == "topic1"
      [p1] = topic1.partitions
      assert p1.partition_index == 0
      assert p1.committed_offset == 100
      assert p1.committed_leader_epoch == 5
      assert p1.committed_metadata == ""

      assert topic2.name == "topic2"
      [p2] = topic2.partitions
      assert p2.committed_leader_epoch == -1
    end

    test "defaults committed_leader_epoch to -1 when not provided" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100}]}
        ]
      ]

      result = RequestHelpers.build_topics_with_leader_epoch(opts)

      [topic] = result
      [partition] = topic.partitions
      assert partition.committed_leader_epoch == -1
    end

    test "includes metadata when provided" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100, metadata: "custom", leader_epoch: 3}]}
        ]
      ]

      result = RequestHelpers.build_topics_with_leader_epoch(opts)

      [topic] = result
      [partition] = topic.partitions
      assert partition.committed_metadata == "custom"
      assert partition.committed_leader_epoch == 3
    end
  end

  describe "build_partitions_with_leader_epoch/1" do
    test "builds partition list with committed_leader_epoch" do
      partitions = [
        %{partition_num: 0, offset: 100, leader_epoch: 5, metadata: "meta1"},
        %{partition_num: 1, offset: 200}
      ]

      result = RequestHelpers.build_partitions_with_leader_epoch(partitions)

      assert length(result) == 2
      [p1, p2] = result

      assert p1.partition_index == 0
      assert p1.committed_offset == 100
      assert p1.committed_metadata == "meta1"
      assert p1.committed_leader_epoch == 5

      assert p2.partition_index == 1
      assert p2.committed_offset == 200
      assert p2.committed_metadata == ""
      assert p2.committed_leader_epoch == -1
    end
  end

  describe "build_v6_request/2" do
    test "builds request with committed_leader_epoch per partition" do
      template = %{}

      opts = [
        group_id: "v6-group",
        generation_id: 8,
        member_id: "member-v6",
        topics: [{"topic1", [%{partition_num: 0, offset: 100, leader_epoch: 3}]}]
      ]

      result = RequestHelpers.build_v6_request(template, opts)

      assert result.group_id == "v6-group"
      assert result.generation_id == 8
      assert result.member_id == "member-v6"
      refute Map.has_key?(result, :retention_time_ms)

      [topic] = result.topics
      [partition] = topic.partitions
      assert partition.committed_leader_epoch == 3
    end
  end

  describe "build_v7_plus_request/2" do
    test "builds request with group_instance_id and committed_leader_epoch" do
      template = %{}

      opts = [
        group_id: "v7-group",
        generation_id: 12,
        member_id: "member-v7",
        group_instance_id: "static-member-1",
        topics: [{"topic1", [%{partition_num: 0, offset: 500, leader_epoch: 7}]}]
      ]

      result = RequestHelpers.build_v7_plus_request(template, opts)

      assert result.group_id == "v7-group"
      assert result.generation_id == 12
      assert result.member_id == "member-v7"
      assert result.group_instance_id == "static-member-1"

      [topic] = result.topics
      [partition] = topic.partitions
      assert partition.committed_leader_epoch == 7
    end

    test "uses nil for group_instance_id when not provided" do
      template = %{}

      opts = [
        group_id: "v7-no-instance",
        topics: [{"topic1", [%{partition_num: 0, offset: 100}]}]
      ]

      result = RequestHelpers.build_v7_plus_request(template, opts)

      assert result.group_instance_id == nil
      assert result.generation_id == -1
      assert result.member_id == ""
    end
  end
end
