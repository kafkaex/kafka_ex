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
  end
end
