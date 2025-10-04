defmodule KafkaEx.New.Protocols.Kayrock.OffsetCommit.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.OffsetCommit.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts group_id from opts" do
      opts = [group_id: "test-group", other: "value"]

      result = RequestHelpers.extract_common_fields(opts)

      assert %{group_id: "test-group"} = result
    end

    test "raises when group_id is missing" do
      opts = [other: "value"]

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(opts)
      end
    end
  end

  describe "extract_coordination_fields/1" do
    test "extracts generation_id and member_id with defaults" do
      opts = [group_id: "test-group"]

      result = RequestHelpers.extract_coordination_fields(opts)

      assert %{generation_id: -1, member_id: ""} = result
    end

    test "extracts custom generation_id and member_id" do
      opts = [group_id: "test-group", generation_id: 5, member_id: "member-123"]

      result = RequestHelpers.extract_coordination_fields(opts)

      assert %{generation_id: 5, member_id: "member-123"} = result
    end

    test "uses default when only generation_id is provided" do
      opts = [generation_id: 10]

      result = RequestHelpers.extract_coordination_fields(opts)

      assert %{generation_id: 10, member_id: ""} = result
    end

    test "uses default when only member_id is provided" do
      opts = [member_id: "consumer-1"]

      result = RequestHelpers.extract_coordination_fields(opts)

      assert %{generation_id: -1, member_id: "consumer-1"} = result
    end
  end

  describe "extract_retention_time/1" do
    test "returns default retention_time when not provided" do
      opts = [group_id: "test-group"]

      result = RequestHelpers.extract_retention_time(opts)

      assert %{retention_time: -1} = result
    end

    test "extracts custom retention_time" do
      opts = [retention_time: 86_400_000]

      result = RequestHelpers.extract_retention_time(opts)

      assert %{retention_time: 86_400_000} = result
    end
  end

  describe "build_partitions/2" do
    test "builds partitions without timestamp" do
      partitions = [
        %{partition_num: 0, offset: 100, metadata: "meta1"},
        %{partition_num: 1, offset: 200}
      ]

      result = RequestHelpers.build_partitions(partitions, false)

      assert [
               %{partition: 0, offset: 100, metadata: "meta1"},
               %{partition: 1, offset: 200, metadata: ""}
             ] = result
    end

    test "builds partitions with timestamp" do
      partitions = [
        %{partition_num: 0, offset: 100, timestamp: 1_234_567_890},
        %{partition_num: 1, offset: 200}
      ]

      result = RequestHelpers.build_partitions(partitions, true)

      assert [
               %{partition: 0, offset: 100, metadata: "", timestamp: 1_234_567_890},
               %{partition: 1, offset: 200, metadata: "", timestamp: -1}
             ] = result
    end

    test "uses default empty string for missing metadata" do
      partitions = [%{partition_num: 0, offset: 100}]

      result = RequestHelpers.build_partitions(partitions, false)

      assert [%{partition: 0, offset: 100, metadata: ""}] = result
    end

    test "uses default -1 for missing timestamp when include_timestamp is true" do
      partitions = [%{partition_num: 0, offset: 100}]

      result = RequestHelpers.build_partitions(partitions, true)

      assert [%{partition: 0, offset: 100, metadata: "", timestamp: -1}] = result
    end
  end

  describe "build_topics/2" do
    test "builds topics structure without timestamp" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100}]},
          {"topic2", [%{partition_num: 0, offset: 200}, %{partition_num: 1, offset: 300}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts, false)

      assert [
               %{
                 topic: "topic1",
                 partitions: [%{partition: 0, offset: 100, metadata: ""}]
               },
               %{
                 topic: "topic2",
                 partitions: [
                   %{partition: 0, offset: 200, metadata: ""},
                   %{partition: 1, offset: 300, metadata: ""}
                 ]
               }
             ] = result
    end

    test "builds topics structure with timestamp" do
      opts = [
        topics: [
          {"topic1", [%{partition_num: 0, offset: 100, timestamp: 1_234_567_890}]}
        ]
      ]

      result = RequestHelpers.build_topics(opts, true)

      assert [
               %{
                 topic: "topic1",
                 partitions: [%{partition: 0, offset: 100, metadata: "", timestamp: 1_234_567_890}]
               }
             ] = result
    end

    test "handles multiple partitions with mixed metadata" do
      opts = [
        topics: [
          {"topic1",
           [
             %{partition_num: 0, offset: 100, metadata: "custom"},
             %{partition_num: 1, offset: 200}
           ]}
        ]
      ]

      result = RequestHelpers.build_topics(opts, false)

      assert [
               %{
                 topic: "topic1",
                 partitions: [
                   %{partition: 0, offset: 100, metadata: "custom"},
                   %{partition: 1, offset: 200, metadata: ""}
                 ]
               }
             ] = result
    end

    test "raises when topics key is missing" do
      opts = [group_id: "test-group"]

      assert_raise KeyError, fn ->
        RequestHelpers.build_topics(opts, false)
      end
    end
  end
end
