defmodule KafkaEx.Cluster.TopicTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Cluster.Topic

  describe "from_topic_metadata/1" do
    setup do
      metadata = %{
        name: "test-topic",
        is_internal: false,
        partitions: [
          %{error_code: 0, partition_index: 123, leader_id: 321, replica_nodes: [], isr_nodes: []}
        ]
      }

      {:ok, %{metadata: metadata}}
    end

    test "sets topic name from metadata", %{metadata: metadata} do
      topic = Topic.from_topic_metadata(metadata)

      assert topic.name == "test-topic"
    end

    test "sets is_internal from metadata", %{metadata: metadata} do
      topic = Topic.from_topic_metadata(metadata)

      assert topic.is_internal == false
    end

    test "sets only valid partition leaders from metadata", %{
      metadata: metadata
    } do
      topic = Topic.from_topic_metadata(metadata)

      assert topic.partition_leaders == %{123 => 321}
    end

    test "sets partitions from metadata", %{metadata: metadata} do
      topic = Topic.from_topic_metadata(metadata)

      assert topic.partitions == [
               %KafkaEx.Cluster.PartitionInfo{
                 partition_id: 123,
                 leader: 321,
                 replicas: [],
                 isr: []
               }
             ]
    end

    test "handles multiple partitions" do
      metadata = %{
        name: "multi-partition-topic",
        is_internal: false,
        partitions: [
          %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1, 2], isr_nodes: [1, 2]},
          %{error_code: 0, partition_index: 1, leader_id: 2, replica_nodes: [2, 3], isr_nodes: [2, 3]},
          %{error_code: 0, partition_index: 2, leader_id: 3, replica_nodes: [1, 3], isr_nodes: [1, 3]}
        ]
      }

      topic = Topic.from_topic_metadata(metadata)

      assert topic.name == "multi-partition-topic"
      assert map_size(topic.partition_leaders) == 3
      assert topic.partition_leaders == %{0 => 1, 1 => 2, 2 => 3}
      assert length(topic.partitions) == 3
    end

    test "handles internal topic" do
      metadata = %{
        name: "__consumer_offsets",
        is_internal: true,
        partitions: [
          %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
        ]
      }

      topic = Topic.from_topic_metadata(metadata)

      assert topic.is_internal == true
      assert topic.name == "__consumer_offsets"
    end

    test "raises FunctionClauseError on partition with non-zero error_code" do
      metadata = %{
        name: "error-topic",
        is_internal: false,
        partitions: [
          %{error_code: 5, partition_index: 0, leader_id: -1, replica_nodes: [], isr_nodes: []}
        ]
      }

      assert_raise FunctionClauseError, fn ->
        Topic.from_topic_metadata(metadata)
      end
    end

    test "handles empty partitions list" do
      metadata = %{
        name: "empty-topic",
        is_internal: false,
        partitions: []
      }

      topic = Topic.from_topic_metadata(metadata)

      assert topic.name == "empty-topic"
      assert topic.partition_leaders == %{}
      assert topic.partitions == []
    end
  end

  describe "struct defaults" do
    test "has expected default values" do
      topic = %Topic{}

      assert topic.name == nil
      assert topic.partition_leaders == %{}
      assert topic.is_internal == false
      assert topic.partitions == []
    end
  end
end
