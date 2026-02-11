defmodule KafkaEx.Cluster.ClusterMetadataTest do
  use ExUnit.Case, async: true
  alias KafkaEx.Cluster.ClusterMetadata

  describe "from_metadata_v1_response/1" do
    test "builds cluster metadata from V1 response with brokers" do
      response = %{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: "rack-a"},
          %{node_id: 2, host: "broker2.example.com", port: 9092, rack: "rack-b"}
        ],
        controller_id: 1,
        topics: []
      }

      cluster = ClusterMetadata.from_metadata_v1_response(response)

      assert map_size(cluster.brokers) == 2
      assert cluster.controller_id == 1

      broker1 = cluster.brokers[1]
      assert broker1.host == "broker1.example.com"
      assert broker1.port == 9092
      assert broker1.node_id == 1
      assert broker1.rack == "rack-a"

      broker2 = cluster.brokers[2]
      assert broker2.host == "broker2.example.com"
      assert broker2.rack == "rack-b"
    end

    test "builds cluster metadata from V1 response with topics" do
      response = %{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topics: [
          %{
            name: "test-topic",
            error_code: 0,
            is_internal: false,
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]},
              %{error_code: 0, partition_index: 1, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          }
        ]
      }

      cluster = ClusterMetadata.from_metadata_v1_response(response)

      assert map_size(cluster.topics) == 1
      assert Map.has_key?(cluster.topics, "test-topic")

      topic = cluster.topics["test-topic"]
      assert topic.name == "test-topic"
      assert map_size(topic.partition_leaders) == 2
    end

    test "filters out topics with error codes" do
      response = %{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topics: [
          %{
            name: "good-topic",
            error_code: 0,
            is_internal: false,
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          },
          %{
            name: "error-topic",
            error_code: 3,
            is_internal: false,
            partitions: []
          }
        ]
      }

      cluster = ClusterMetadata.from_metadata_v1_response(response)

      assert map_size(cluster.topics) == 1
      assert Map.has_key?(cluster.topics, "good-topic")
      refute Map.has_key?(cluster.topics, "error-topic")
    end

    test "handles empty brokers and topics" do
      response = %{
        brokers: [],
        controller_id: nil,
        topics: []
      }

      cluster = ClusterMetadata.from_metadata_v1_response(response)

      assert cluster.brokers == %{}
      assert cluster.topics == %{}
      assert cluster.controller_id == nil
    end
  end

  describe "known_topics/1" do
    test "return list of all known topics" do
      topic = %KafkaEx.Cluster.Topic{name: "test-topic"}
      cluster = %ClusterMetadata{topics: %{topic.name => topic}}

      assert ClusterMetadata.known_topics(cluster) == ["test-topic"]
    end
  end

  describe "topics_metadata/1" do
    test "returns metadata for topics we've asked for" do
      topic_1 = %KafkaEx.Cluster.Topic{name: "test-topic-one"}
      topic_2 = %KafkaEx.Cluster.Topic{name: "test-topic-two"}

      cluster = %ClusterMetadata{
        topics: %{
          topic_1.name => topic_1,
          topic_2.name => topic_2
        }
      }

      assert ClusterMetadata.topics_metadata(cluster, ["test-topic-one"]) == [
               topic_1
             ]
    end
  end

  describe "brokers/1" do
    test "returns list of brokers" do
      broker = %KafkaEx.Cluster.Broker{node_id: 1}
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      assert ClusterMetadata.brokers(cluster) == [broker]
    end
  end

  describe "select_node/2" do
    test "returns random node" do
      broker = %KafkaEx.Cluster.Broker{node_id: 1}
      node_selector = KafkaEx.Client.NodeSelector.random()
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:ok, broker.node_id}
    end

    test "returns controller node" do
      broker_1 = %KafkaEx.Cluster.Broker{node_id: 1}
      broker_2 = %KafkaEx.Cluster.Broker{node_id: 2}
      node_selector = KafkaEx.Client.NodeSelector.controller()

      cluster = %ClusterMetadata{
        controller_id: 1,
        brokers: %{1 => broker_1, 2 => broker_2}
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:ok, broker_1.node_id}
    end

    test "returns node based on node_id" do
      broker_1 = %KafkaEx.Cluster.Broker{node_id: 1}
      broker_2 = %KafkaEx.Cluster.Broker{node_id: 2}
      node_selector = KafkaEx.Client.NodeSelector.node_id(2)

      cluster = %ClusterMetadata{
        controller_id: 1,
        brokers: %{1 => broker_1, 2 => broker_2}
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:ok, broker_2.node_id}
    end

    test "returns error when node does not exist" do
      broker_1 = %KafkaEx.Cluster.Broker{node_id: 1}
      broker_2 = %KafkaEx.Cluster.Broker{node_id: 2}
      node_selector = KafkaEx.Client.NodeSelector.node_id(3)

      cluster = %ClusterMetadata{
        controller_id: 1,
        brokers: %{1 => broker_1, 2 => broker_2}
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:error, :no_such_node}
    end

    test "returns node based on topic & partition id" do
      topic_one =
        KafkaEx.Cluster.Topic.from_topic_metadata(%{
          name: "topic-one",
          is_internal: false,
          partitions: [
            %{error_code: 0, partition_index: 0, leader_id: 123, replica_nodes: [], isr_nodes: []}
          ]
        })

      topic_two =
        KafkaEx.Cluster.Topic.from_topic_metadata(%{
          name: "topic-two",
          is_internal: false,
          partitions: [
            %{error_code: 0, partition_index: 0, leader_id: 321, replica_nodes: [], isr_nodes: []}
          ]
        })

      node_selector = KafkaEx.Client.NodeSelector.topic_partition("topic-one", 0)

      cluster = %ClusterMetadata{
        topics: %{
          topic_one.name => topic_one,
          topic_two.name => topic_two
        }
      }

      assert ClusterMetadata.select_node(cluster, node_selector) == {:ok, 123}
    end

    test "returns error when topic does not exist" do
      topic =
        KafkaEx.Cluster.Topic.from_topic_metadata(%{
          name: "topic-one",
          is_internal: false,
          partitions: [
            %{error_code: 0, partition_index: 0, leader_id: 123, replica_nodes: [], isr_nodes: []}
          ]
        })

      node_selector = KafkaEx.Client.NodeSelector.topic_partition("topic-two", 0)

      cluster = %ClusterMetadata{
        topics: %{
          topic.name => topic
        }
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:error, :no_such_topic}
    end

    test "returns error when partition does not exist" do
      topic =
        KafkaEx.Cluster.Topic.from_topic_metadata(%{
          name: "topic-one",
          is_internal: false,
          partitions: [
            %{error_code: 0, partition_index: 0, leader_id: 123, replica_nodes: [], isr_nodes: []}
          ]
        })

      node_selector = KafkaEx.Client.NodeSelector.topic_partition("topic-one", 1)

      cluster = %ClusterMetadata{
        topics: %{
          topic.name => topic
        }
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:error, :no_such_partition}
    end

    test "returns node based on consumer group name" do
      node_selector = KafkaEx.Client.NodeSelector.consumer_group("consumer-group-one")

      cluster = %ClusterMetadata{
        consumer_group_coordinators: %{
          "consumer-group-one" => 1,
          "consumer-group-two" => 2
        }
      }

      assert ClusterMetadata.select_node(cluster, node_selector) == {:ok, 1}
    end

    test "returns error when consumer group does not exist" do
      node_selector = KafkaEx.Client.NodeSelector.consumer_group("consumer-group-three")

      cluster = %ClusterMetadata{
        consumer_group_coordinators: %{
          "consumer-group-one" => 1,
          "consumer-group-two" => 2
        }
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:error, :no_such_consumer_group}
    end
  end

  describe "merge_brokers/2" do
    alias KafkaEx.Cluster.Broker
    alias KafkaEx.Network.Socket

    test "preserves socket connections for matching brokers" do
      socket = %Socket{socket: :fake_socket}
      old_broker = %Broker{node_id: 1, host: "broker1.example.com", port: 9092, socket: socket}
      old_cluster = %ClusterMetadata{brokers: %{1 => old_broker}}

      new_broker = %Broker{node_id: 1, host: "broker1.example.com", port: 9092, socket: nil}
      new_cluster = %ClusterMetadata{brokers: %{1 => new_broker}}

      {merged_cluster, brokers_to_close} = ClusterMetadata.merge_brokers(old_cluster, new_cluster)

      merged_broker = merged_cluster.brokers[1]
      assert merged_broker.socket == socket
      assert brokers_to_close == []
    end

    test "returns brokers to close when they are removed" do
      socket = %Socket{socket: :fake_socket}
      old_broker1 = %Broker{node_id: 1, host: "broker1.example.com", port: 9092, socket: socket}
      old_broker2 = %Broker{node_id: 2, host: "broker2.example.com", port: 9092, socket: socket}
      old_cluster = %ClusterMetadata{brokers: %{1 => old_broker1, 2 => old_broker2}}

      new_broker = %Broker{node_id: 1, host: "broker1.example.com", port: 9092, socket: nil}
      new_cluster = %ClusterMetadata{brokers: %{1 => new_broker}}

      {merged_cluster, brokers_to_close} = ClusterMetadata.merge_brokers(old_cluster, new_cluster)

      assert map_size(merged_cluster.brokers) == 1
      assert length(brokers_to_close) == 1
      assert hd(brokers_to_close).node_id == 2
    end

    test "handles node_id changes for same host:port" do
      socket = %Socket{socket: :fake_socket}
      old_broker = %Broker{node_id: 1, host: "broker.example.com", port: 9092, socket: socket}
      old_cluster = %ClusterMetadata{brokers: %{1 => old_broker}}

      new_broker = %Broker{node_id: 2, host: "broker.example.com", port: 9092, socket: nil}
      new_cluster = %ClusterMetadata{brokers: %{2 => new_broker}}

      {merged_cluster, brokers_to_close} = ClusterMetadata.merge_brokers(old_cluster, new_cluster)

      assert map_size(merged_cluster.brokers) == 1
      assert Map.has_key?(merged_cluster.brokers, 2)
      merged_broker = merged_cluster.brokers[2]
      assert merged_broker.socket == socket
      assert brokers_to_close == []
    end

    test "adds new brokers without sockets" do
      old_cluster = %ClusterMetadata{brokers: %{}}

      new_broker = %Broker{node_id: 1, host: "broker.example.com", port: 9092, socket: nil}
      new_cluster = %ClusterMetadata{brokers: %{1 => new_broker}}

      {merged_cluster, brokers_to_close} = ClusterMetadata.merge_brokers(old_cluster, new_cluster)

      assert map_size(merged_cluster.brokers) == 1
      assert merged_cluster.brokers[1].socket == nil
      assert brokers_to_close == []
    end

    test "preserves new cluster controller_id and topics" do
      old_cluster = %ClusterMetadata{controller_id: 1, topics: %{}}

      topic = %KafkaEx.Cluster.Topic{name: "test-topic"}
      new_cluster = %ClusterMetadata{controller_id: 2, topics: %{"test-topic" => topic}}

      {merged_cluster, _} = ClusterMetadata.merge_brokers(old_cluster, new_cluster)

      assert merged_cluster.controller_id == 2
      assert Map.has_key?(merged_cluster.topics, "test-topic")
    end
  end

  describe "broker_by_node_id/1" do
    test "returns broker by its node id" do
      broker = %KafkaEx.Cluster.Broker{node_id: 1}
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      assert ClusterMetadata.broker_by_node_id(cluster, 1) == broker
    end

    test "returns nil when broker is not found" do
      broker = %KafkaEx.Cluster.Broker{node_id: 1}
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      refute ClusterMetadata.broker_by_node_id(cluster, 2)
    end
  end

  describe "update_brokers/2" do
    test "updates brokers based on given function" do
      socket = %KafkaEx.Network.Socket{}
      broker = %KafkaEx.Cluster.Broker{node_id: 1, socket: socket}
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      updated_cluster =
        ClusterMetadata.update_brokers(cluster, fn broker_to_update ->
          KafkaEx.Cluster.Broker.put_socket(broker_to_update, nil)
        end)

      updated_broker = ClusterMetadata.broker_by_node_id(updated_cluster, 1)
      refute updated_broker.socket
    end
  end

  describe "put_consumer_group_coordinator/3" do
    test "stores coordinator for new consumer group" do
      cluster = %ClusterMetadata{consumer_group_coordinators: %{}}
      updated_cluster = ClusterMetadata.put_consumer_group_coordinator(cluster, "my-group", 1)

      assert updated_cluster.consumer_group_coordinators["my-group"] == 1
    end

    test "updates coordinator for existing consumer group" do
      cluster = %ClusterMetadata{consumer_group_coordinators: %{"my-group" => 1}}
      updated_cluster = ClusterMetadata.put_consumer_group_coordinator(cluster, "my-group", 2)

      assert updated_cluster.consumer_group_coordinators["my-group"] == 2
    end

    test "preserves other consumer group coordinators" do
      cluster = %ClusterMetadata{consumer_group_coordinators: %{"group-a" => 1, "group-b" => 2}}

      updated_cluster = ClusterMetadata.put_consumer_group_coordinator(cluster, "group-c", 3)

      assert updated_cluster.consumer_group_coordinators["group-a"] == 1
      assert updated_cluster.consumer_group_coordinators["group-b"] == 2
      assert updated_cluster.consumer_group_coordinators["group-c"] == 3
    end
  end

  describe "remove_topics/2" do
    test "test removes topic based on its name" do
      topic_one =
        KafkaEx.Cluster.Topic.from_topic_metadata(%{
          name: "topic-one",
          is_internal: false,
          partitions: [
            %{error_code: 0, partition_index: 0, leader_id: 123, replica_nodes: [], isr_nodes: []}
          ]
        })

      topic_two =
        KafkaEx.Cluster.Topic.from_topic_metadata(%{
          name: "topic-two",
          is_internal: false,
          partitions: [
            %{error_code: 0, partition_index: 0, leader_id: 321, replica_nodes: [], isr_nodes: []}
          ]
        })

      cluster = %ClusterMetadata{
        topics: %{
          topic_one.name => topic_one,
          topic_two.name => topic_two
        }
      }

      updated_cluster = ClusterMetadata.remove_topics(cluster, ["topic-two"])
      assert ClusterMetadata.known_topics(updated_cluster) == ["topic-one"]
    end
  end
end
