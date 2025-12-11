defmodule KafkaEx.New.AdapterMetadataTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.New.Structs.{Broker, ClusterMetadata, Partition, Topic}
  alias KafkaEx.Protocol.Metadata.{PartitionMetadata, Response, TopicMetadata}

  describe "Adapter.metadata_response/1" do
    test "converts ClusterMetadata with single broker to legacy format" do
      cluster_metadata = %ClusterMetadata{
        brokers: %{
          1 => %Broker{
            node_id: 1,
            host: "broker1.local",
            port: 9092,
            socket: nil,
            rack: nil
          }
        },
        controller_id: 1,
        topics: %{},
        consumer_group_coordinators: %{}
      }

      response = Adapter.metadata_response(cluster_metadata)

      assert %Response{} = response
      assert length(response.brokers) == 1
      assert [broker] = response.brokers
      assert broker.node_id == 1
      assert broker.host == "broker1.local"
      assert broker.port == 9092
      assert broker.is_controller == true
    end

    test "converts ClusterMetadata with multiple brokers to legacy format" do
      cluster_metadata = %ClusterMetadata{
        brokers: %{
          1 => %Broker{node_id: 1, host: "broker1.local", port: 9092, socket: nil, rack: nil},
          2 => %Broker{node_id: 2, host: "broker2.local", port: 9092, socket: nil, rack: nil},
          3 => %Broker{node_id: 3, host: "broker3.local", port: 9092, socket: nil, rack: nil}
        },
        controller_id: 2,
        topics: %{},
        consumer_group_coordinators: %{}
      }

      response = Adapter.metadata_response(cluster_metadata)

      assert length(response.brokers) == 3

      controller_broker = Enum.find(response.brokers, fn b -> b.node_id == 2 end)
      assert controller_broker.is_controller == true

      non_controller = Enum.find(response.brokers, fn b -> b.node_id == 1 end)
      assert non_controller.is_controller == false
    end

    test "converts ClusterMetadata with topics to legacy format" do
      cluster_metadata = %ClusterMetadata{
        brokers: %{
          1 => %Broker{node_id: 1, host: "broker1.local", port: 9092, socket: nil, rack: nil}
        },
        controller_id: 1,
        topics: %{
          "test-topic" => %Topic{
            name: "test-topic",
            partition_leaders: %{0 => 1, 1 => 1},
            is_internal: false,
            partitions: [
              %Partition{partition_id: 0, leader: 1, replicas: [1], isr: [1]},
              %Partition{partition_id: 1, leader: 1, replicas: [1], isr: [1]}
            ]
          }
        },
        consumer_group_coordinators: %{}
      }

      response = Adapter.metadata_response(cluster_metadata)

      assert length(response.topic_metadatas) == 1
      assert [topic] = response.topic_metadatas
      assert %TopicMetadata{} = topic
      assert topic.topic == "test-topic"
      assert topic.is_internal == false
      assert length(topic.partition_metadatas) == 2
    end

    test "converts partition metadata correctly" do
      cluster_metadata = %ClusterMetadata{
        brokers: %{
          1 => %Broker{node_id: 1, host: "broker1.local", port: 9092, socket: nil, rack: nil}
        },
        controller_id: 1,
        topics: %{
          "test-topic" => %Topic{
            name: "test-topic",
            partition_leaders: %{0 => 1},
            is_internal: false,
            partitions: [
              %Partition{
                partition_id: 0,
                leader: 1,
                replicas: [1, 2, 3],
                isr: [1, 2]
              }
            ]
          }
        },
        consumer_group_coordinators: %{}
      }

      response = Adapter.metadata_response(cluster_metadata)

      [topic] = response.topic_metadatas
      [partition] = topic.partition_metadatas

      assert %PartitionMetadata{} = partition
      assert partition.partition_id == 0
      assert partition.leader == 1
      assert partition.replicas == [1, 2, 3]
      assert partition.isrs == [1, 2]
    end

    test "converts internal topics correctly" do
      cluster_metadata = %ClusterMetadata{
        brokers: %{
          1 => %Broker{node_id: 1, host: "broker1.local", port: 9092, socket: nil, rack: nil}
        },
        controller_id: 1,
        topics: %{
          "__consumer_offsets" => %Topic{
            name: "__consumer_offsets",
            partition_leaders: %{0 => 1},
            is_internal: true,
            partitions: [
              %Partition{partition_id: 0, leader: 1, replicas: [1], isr: [1]}
            ]
          }
        },
        consumer_group_coordinators: %{}
      }

      response = Adapter.metadata_response(cluster_metadata)

      [topic] = response.topic_metadatas
      assert topic.topic == "__consumer_offsets"
      assert topic.is_internal == true
    end

    test "handles empty brokers" do
      cluster_metadata = %ClusterMetadata{
        brokers: %{},
        controller_id: nil,
        topics: %{},
        consumer_group_coordinators: %{}
      }

      response = Adapter.metadata_response(cluster_metadata)

      assert response.brokers == []
      assert response.topic_metadatas == []
    end

    test "handles empty topics" do
      cluster_metadata = %ClusterMetadata{
        brokers: %{
          1 => %Broker{node_id: 1, host: "broker1.local", port: 9092, socket: nil, rack: nil}
        },
        controller_id: 1,
        topics: %{},
        consumer_group_coordinators: %{}
      }

      response = Adapter.metadata_response(cluster_metadata)

      assert length(response.brokers) == 1
      assert response.topic_metadatas == []
    end

    test "handles nil controller_id" do
      cluster_metadata = %ClusterMetadata{
        brokers: %{
          1 => %Broker{node_id: 1, host: "broker1.local", port: 9092, socket: nil, rack: nil}
        },
        controller_id: nil,
        topics: %{},
        consumer_group_coordinators: %{}
      }

      response = Adapter.metadata_response(cluster_metadata)

      assert [broker] = response.brokers
      assert broker.is_controller == false
    end

    test "converts multiple topics correctly" do
      cluster_metadata = %ClusterMetadata{
        brokers: %{
          1 => %Broker{node_id: 1, host: "broker1.local", port: 9092, socket: nil, rack: nil}
        },
        controller_id: 1,
        topics: %{
          "topic1" => %Topic{
            name: "topic1",
            partition_leaders: %{0 => 1},
            is_internal: false,
            partitions: [
              %Partition{partition_id: 0, leader: 1, replicas: [1], isr: [1]}
            ]
          },
          "topic2" => %Topic{
            name: "topic2",
            partition_leaders: %{0 => 1},
            is_internal: false,
            partitions: [
              %Partition{partition_id: 0, leader: 1, replicas: [1], isr: [1]}
            ]
          }
        },
        consumer_group_coordinators: %{}
      }

      response = Adapter.metadata_response(cluster_metadata)

      assert length(response.topic_metadatas) == 2
      topic_names = Enum.map(response.topic_metadatas, & &1.topic) |> Enum.sort()
      assert topic_names == ["topic1", "topic2"]
    end
  end
end
