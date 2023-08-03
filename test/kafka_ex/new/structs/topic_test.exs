defmodule KafkaEx.New.Structs.TopicTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.Topic

  describe "from_topic_metadata/1" do
    setup do
      metadata = %{
        topic: "test-topic",
        is_internal: false,
        partition_metadata: [
          %{error_code: 0, partition: 123, leader: 321, replicas: [], isr: []}
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
               %KafkaEx.New.Structs.Partition{
                 partition_id: 123,
                 leader: 321,
                 replicas: [],
                 isr: []
               }
             ]
    end
  end
end
