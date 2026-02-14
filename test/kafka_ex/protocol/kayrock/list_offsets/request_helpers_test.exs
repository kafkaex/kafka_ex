defmodule KafkaEx.Protocol.Kayrock.ListOffsets.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ListOffsets.RequestHelpers

  @partitions_data [
    %{partition_num: 0, timestamp: :earliest},
    %{partition_num: 1, timestamp: :latest},
    %{partition_num: 2, timestamp: 1_700_000_000_000},
    %{partition_num: 3, timestamp: ~U[2024-04-19 12:00:00.000000Z]}
  ]

  describe "build_request_v2_plus/3" do
    test "builds V2 request with default options" do
      template = %Kayrock.ListOffsets.V2.Request{}
      opts = [topics: [{"test-topic", @partitions_data}]]

      result = RequestHelpers.build_request_v2_plus(template, opts, 2)

      assert %Kayrock.ListOffsets.V2.Request{} = result
      assert result.replica_id == -1
      assert result.isolation_level == 0

      assert [%{topic: "test-topic", partitions: partitions}] = result.topics
      assert length(partitions) == 4

      [p0, p1, p2, p3] = partitions
      assert p0 == %{partition: 0, timestamp: -2}
      assert p1 == %{partition: 1, timestamp: -1}
      assert p2 == %{partition: 2, timestamp: 1_700_000_000_000}
      assert p3 == %{partition: 3, timestamp: 1_713_528_000_000}
    end

    test "builds V2 request with custom replica_id and isolation_level" do
      template = %Kayrock.ListOffsets.V2.Request{}

      opts = [
        topics: [{"test-topic", [%{partition_num: 0, timestamp: :latest}]}],
        replica_id: 5,
        isolation_level: :read_commited
      ]

      result = RequestHelpers.build_request_v2_plus(template, opts, 2)

      assert result.replica_id == 5
      assert result.isolation_level == 1
    end

    test "builds V3 request without current_leader_epoch (api_version < 4)" do
      template = %Kayrock.ListOffsets.V3.Request{}

      opts = [
        topics: [{"test-topic", [%{partition_num: 0, timestamp: :earliest}]}],
        current_leader_epoch: 7
      ]

      result = RequestHelpers.build_request_v2_plus(template, opts, 3)

      assert [%{partitions: [partition]}] = result.topics
      # V3 (api_version 3) should NOT include current_leader_epoch
      refute Map.has_key?(partition, :current_leader_epoch)
      assert partition == %{partition: 0, timestamp: -2}
    end

    test "builds V4 request with current_leader_epoch (api_version >= 4)" do
      template = %Kayrock.ListOffsets.V4.Request{}

      opts = [
        topics: [{"test-topic", [%{partition_num: 0, timestamp: :earliest}]}],
        current_leader_epoch: 7
      ]

      result = RequestHelpers.build_request_v2_plus(template, opts, 4)

      assert [%{partitions: [partition]}] = result.topics
      assert Map.has_key?(partition, :current_leader_epoch)
      assert partition == %{partition: 0, timestamp: -2, current_leader_epoch: 7}
    end

    test "builds V5 request with current_leader_epoch default -1" do
      template = %Kayrock.ListOffsets.V5.Request{}

      opts = [
        topics: [{"test-topic", [%{partition_num: 0, timestamp: :latest}]}]
      ]

      result = RequestHelpers.build_request_v2_plus(template, opts, 5)

      assert [%{partitions: [partition]}] = result.topics
      assert partition.current_leader_epoch == -1
    end

    test "handles multiple topics with multiple partitions" do
      partitions1 = [
        %{partition_num: 0, timestamp: :earliest},
        %{partition_num: 1, timestamp: :latest}
      ]

      partitions2 = [
        %{partition_num: 0, timestamp: 500}
      ]

      opts = [topics: [{"topic-a", partitions1}, {"topic-b", partitions2}]]
      template = %Kayrock.ListOffsets.V4.Request{}

      result = RequestHelpers.build_request_v2_plus(template, opts, 4)

      assert [topic_a, topic_b] = result.topics
      assert topic_a.topic == "topic-a"
      assert length(topic_a.partitions) == 2
      assert topic_b.topic == "topic-b"
      assert length(topic_b.partitions) == 1
    end

    test "unknown isolation_level defaults to 0 (READ_UNCOMMITTED)" do
      template = %Kayrock.ListOffsets.V2.Request{}

      opts = [
        topics: [{"t", [%{partition_num: 0, timestamp: :latest}]}],
        isolation_level: :unknown_level
      ]

      result = RequestHelpers.build_request_v2_plus(template, opts, 2)

      assert result.isolation_level == 0
    end

    test "nil isolation_level defaults to 0 (READ_UNCOMMITTED)" do
      template = %Kayrock.ListOffsets.V2.Request{}

      opts = [
        topics: [{"t", [%{partition_num: 0, timestamp: :latest}]}]
      ]

      result = RequestHelpers.build_request_v2_plus(template, opts, 2)

      assert result.isolation_level == 0
    end

    test "raises KeyError when topics option is missing" do
      template = %Kayrock.ListOffsets.V2.Request{}

      assert_raise KeyError, fn ->
        RequestHelpers.build_request_v2_plus(template, [], 2)
      end
    end
  end

  describe "build_topics/2" do
    test "builds topics without current_leader_epoch for api_version < 4" do
      opts = [
        topics: [{"test-topic", [%{partition_num: 0, timestamp: :earliest}]}]
      ]

      result = RequestHelpers.build_topics(opts, 2)

      assert [%{topic: "test-topic", partitions: [%{partition: 0, timestamp: -2}]}] = result
    end

    test "builds topics without current_leader_epoch for api_version 3" do
      opts = [
        topics: [{"test-topic", [%{partition_num: 0, timestamp: :latest}]}]
      ]

      result = RequestHelpers.build_topics(opts, 3)

      assert [%{topic: "test-topic", partitions: [partition]}] = result
      refute Map.has_key?(partition, :current_leader_epoch)
    end

    test "builds topics with current_leader_epoch for api_version 4" do
      opts = [
        topics: [{"test-topic", [%{partition_num: 0, timestamp: :latest}]}],
        current_leader_epoch: 10
      ]

      result = RequestHelpers.build_topics(opts, 4)

      assert [%{topic: "test-topic", partitions: [partition]}] = result
      assert partition.current_leader_epoch == 10
    end

    test "builds topics with current_leader_epoch default for api_version 5" do
      opts = [
        topics: [{"test-topic", [%{partition_num: 0, timestamp: :latest}]}]
      ]

      result = RequestHelpers.build_topics(opts, 5)

      assert [%{topic: "test-topic", partitions: [partition]}] = result
      assert partition.current_leader_epoch == -1
    end

    test "correctly parses DateTime timestamps" do
      dt = ~U[2024-01-01 00:00:00.000000Z]

      opts = [
        topics: [{"t", [%{partition_num: 0, timestamp: dt}]}]
      ]

      result = RequestHelpers.build_topics(opts, 2)

      assert [%{partitions: [%{timestamp: ts}]}] = result
      assert ts == DateTime.to_unix(dt, :millisecond)
    end

    test "correctly parses integer timestamps" do
      opts = [
        topics: [{"t", [%{partition_num: 0, timestamp: 42}]}]
      ]

      result = RequestHelpers.build_topics(opts, 2)

      assert [%{partitions: [%{timestamp: 42}]}] = result
    end

    test "correctly parses :earliest atom" do
      opts = [
        topics: [{"t", [%{partition_num: 0, timestamp: :earliest}]}]
      ]

      result = RequestHelpers.build_topics(opts, 2)

      assert [%{partitions: [%{timestamp: -2}]}] = result
    end

    test "correctly parses :latest atom" do
      opts = [
        topics: [{"t", [%{partition_num: 0, timestamp: :latest}]}]
      ]

      result = RequestHelpers.build_topics(opts, 2)

      assert [%{partitions: [%{timestamp: -1}]}] = result
    end
  end
end
