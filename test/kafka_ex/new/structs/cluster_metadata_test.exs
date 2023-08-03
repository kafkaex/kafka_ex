defmodule KafkaEx.New.Structs.ClusterMetadataTest do
  use ExUnit.Case, async: true
  alias KafkaEx.New.Structs.ClusterMetadata

  describe "from_metadata_v1_response/1" do
    @tag skip: true
    test "builds cluster metadata based on v1 response" do
    end
  end

  describe "known_topics/1" do
    test "return list of all known topics" do
      topic = %KafkaEx.New.Structs.Topic{name: "test-topic"}
      cluster = %ClusterMetadata{topics: %{topic.name => topic}}

      assert ClusterMetadata.known_topics(cluster) == ["test-topic"]
    end
  end

  describe "topics_metadata/1" do
    test "returns metadata for topics we've asked for" do
      topic_1 = %KafkaEx.New.Structs.Topic{name: "test-topic-one"}
      topic_2 = %KafkaEx.New.Structs.Topic{name: "test-topic-two"}

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
      broker = %KafkaEx.New.Structs.Broker{node_id: 1}
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      assert ClusterMetadata.brokers(cluster) == [broker]
    end
  end

  describe "select_node/2" do
    test "returns random node" do
      broker = %KafkaEx.New.Structs.Broker{node_id: 1}
      node_selector = KafkaEx.New.Structs.NodeSelector.random()
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:ok, broker.node_id}
    end

    test "returns controller node" do
      broker_1 = %KafkaEx.New.Structs.Broker{node_id: 1}
      broker_2 = %KafkaEx.New.Structs.Broker{node_id: 2}
      node_selector = KafkaEx.New.Structs.NodeSelector.controller()

      cluster = %ClusterMetadata{
        controller_id: 1,
        brokers: %{1 => broker_1, 2 => broker_2}
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:ok, broker_1.node_id}
    end

    test "returns node based on node_id" do
      broker_1 = %KafkaEx.New.Structs.Broker{node_id: 1}
      broker_2 = %KafkaEx.New.Structs.Broker{node_id: 2}
      node_selector = KafkaEx.New.Structs.NodeSelector.node_id(2)

      cluster = %ClusterMetadata{
        controller_id: 1,
        brokers: %{1 => broker_1, 2 => broker_2}
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:ok, broker_2.node_id}
    end

    test "returns error when node does not exist" do
      broker_1 = %KafkaEx.New.Structs.Broker{node_id: 1}
      broker_2 = %KafkaEx.New.Structs.Broker{node_id: 2}
      node_selector = KafkaEx.New.Structs.NodeSelector.node_id(3)

      cluster = %ClusterMetadata{
        controller_id: 1,
        brokers: %{1 => broker_1, 2 => broker_2}
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:error, :no_such_node}
    end

    test "returns node based on topic & partition id" do
      topic_one =
        KafkaEx.New.Structs.Topic.from_topic_metadata(%{
          topic: "topic-one",
          is_internal: false,
          partition_metadata: [
            %{error_code: 0, partition: 0, leader: 123, replicas: [], isr: []}
          ]
        })

      topic_two =
        KafkaEx.New.Structs.Topic.from_topic_metadata(%{
          topic: "topic-two",
          is_internal: false,
          partition_metadata: [
            %{error_code: 0, partition: 0, leader: 321, replicas: [], isr: []}
          ]
        })

      node_selector =
        KafkaEx.New.Structs.NodeSelector.topic_partition("topic-one", 0)

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
        KafkaEx.New.Structs.Topic.from_topic_metadata(%{
          topic: "topic-one",
          is_internal: false,
          partition_metadata: [
            %{error_code: 0, partition: 0, leader: 123, replicas: [], isr: []}
          ]
        })

      node_selector =
        KafkaEx.New.Structs.NodeSelector.topic_partition("topic-two", 0)

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
        KafkaEx.New.Structs.Topic.from_topic_metadata(%{
          topic: "topic-one",
          is_internal: false,
          partition_metadata: [
            %{error_code: 0, partition: 0, leader: 123, replicas: [], isr: []}
          ]
        })

      node_selector =
        KafkaEx.New.Structs.NodeSelector.topic_partition("topic-one", 1)

      cluster = %ClusterMetadata{
        topics: %{
          topic.name => topic
        }
      }

      assert ClusterMetadata.select_node(cluster, node_selector) ==
               {:error, :no_such_partition}
    end

    test "returns node based on consumer group name" do
      node_selector =
        KafkaEx.New.Structs.NodeSelector.consumer_group("consumer-group-one")

      cluster = %ClusterMetadata{
        consumer_group_coordinators: %{
          "consumer-group-one" => 1,
          "consumer-group-two" => 2
        }
      }

      assert ClusterMetadata.select_node(cluster, node_selector) == {:ok, 1}
    end

    test "returns error when consumer group does not exist" do
      node_selector =
        KafkaEx.New.Structs.NodeSelector.consumer_group("consumer-group-three")

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
  end

  describe "broker_by_node_id/1" do
    test "returns broker by its node id" do
      broker = %KafkaEx.New.Structs.Broker{node_id: 1}
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      assert ClusterMetadata.broker_by_node_id(cluster, 1) == broker
    end

    test "returns nil when broker is not found" do
      broker = %KafkaEx.New.Structs.Broker{node_id: 1}
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      refute ClusterMetadata.broker_by_node_id(cluster, 2)
    end
  end

  describe "update_brokers/2" do
    test "updates brokers based on given function" do
      socket = %KafkaEx.Socket{}
      broker = %KafkaEx.New.Structs.Broker{node_id: 1, socket: socket}
      cluster = %ClusterMetadata{brokers: %{1 => broker}}

      updated_cluster =
        ClusterMetadata.update_brokers(cluster, fn broker_to_update ->
          KafkaEx.New.Structs.Broker.put_socket(broker_to_update, nil)
        end)

      updated_broker = ClusterMetadata.broker_by_node_id(updated_cluster, 1)
      refute updated_broker.socket
    end
  end

  describe "remove_topics/2" do
    test "test removes topic based on its name" do
      topic_one =
        KafkaEx.New.Structs.Topic.from_topic_metadata(%{
          topic: "topic-one",
          is_internal: false,
          partition_metadata: [
            %{error_code: 0, partition: 0, leader: 123, replicas: [], isr: []}
          ]
        })

      topic_two =
        KafkaEx.New.Structs.Topic.from_topic_metadata(%{
          topic: "topic-two",
          is_internal: false,
          partition_metadata: [
            %{error_code: 0, partition: 0, leader: 321, replicas: [], isr: []}
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
