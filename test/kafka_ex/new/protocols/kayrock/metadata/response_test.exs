defmodule KafkaEx.Protocol.Kayrock.Metadata.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Metadata.Response, as: MetadataResponse
  alias KafkaEx.Cluster.{Broker, ClusterMetadata, Topic}
  alias Kayrock.Metadata.V0
  alias Kayrock.Metadata.V1
  alias Kayrock.Metadata.V2

  describe "parse_response/1 for V0" do
    test "parses response with single broker and topic" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092}
        ],
        topic_metadata: [
          %{
            error_code: 0,
            topic: "test-topic",
            partition_metadata: [
              %{
                error_code: 0,
                partition: 0,
                leader: 1,
                replicas: [1, 2, 3],
                isr: [1, 2]
              }
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert %ClusterMetadata{} = cluster_metadata
      assert map_size(cluster_metadata.brokers) == 1
      assert map_size(cluster_metadata.topics) == 1
      assert cluster_metadata.controller_id == nil

      broker = cluster_metadata.brokers[1]
      assert %Broker{node_id: 1, host: "broker1.example.com", port: 9092} = broker
      assert broker.rack == nil

      topic = cluster_metadata.topics["test-topic"]
      assert %Topic{name: "test-topic", is_internal: false} = topic
      assert length(topic.partitions) == 1
      assert topic.partition_leaders == %{0 => 1}
    end

    test "parses response with multiple brokers" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092},
          %{node_id: 2, host: "broker2.example.com", port: 9092},
          %{node_id: 3, host: "broker3.example.com", port: 9092}
        ],
        topic_metadata: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.brokers) == 3
      assert cluster_metadata.brokers[1].node_id == 1
      assert cluster_metadata.brokers[2].node_id == 2
      assert cluster_metadata.brokers[3].node_id == 3
    end

    test "parses response with multiple topics and partitions" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092}
        ],
        topic_metadata: [
          %{
            error_code: 0,
            topic: "topic1",
            partition_metadata: [
              %{error_code: 0, partition: 0, leader: 1, replicas: [1], isr: [1]},
              %{error_code: 0, partition: 1, leader: 1, replicas: [1], isr: [1]}
            ]
          },
          %{
            error_code: 0,
            topic: "topic2",
            partition_metadata: [
              %{error_code: 0, partition: 0, leader: 1, replicas: [1], isr: [1]}
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.topics) == 2
      assert cluster_metadata.topics["topic1"].name == "topic1"
      assert cluster_metadata.topics["topic2"].name == "topic2"
      assert length(cluster_metadata.topics["topic1"].partitions) == 2
      assert length(cluster_metadata.topics["topic2"].partitions) == 1
    end

    test "filters out topics with errors" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092}
        ],
        topic_metadata: [
          %{
            error_code: 0,
            topic: "good-topic",
            partition_metadata: [
              %{error_code: 0, partition: 0, leader: 1, replicas: [1], isr: [1]}
            ]
          },
          %{
            error_code: 3,
            # UNKNOWN_TOPIC_OR_PARTITION
            topic: "bad-topic",
            partition_metadata: []
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.topics) == 1
      assert cluster_metadata.topics["good-topic"]
      refute cluster_metadata.topics["bad-topic"]
    end

    test "filters out partitions with errors" do
      response = %V0.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092}
        ],
        topic_metadata: [
          %{
            error_code: 0,
            topic: "test-topic",
            partition_metadata: [
              %{error_code: 0, partition: 0, leader: 1, replicas: [1], isr: [1]},
              %{error_code: 5, partition: 1, leader: -1, replicas: [], isr: []}
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      topic = cluster_metadata.topics["test-topic"]
      assert length(topic.partitions) == 1
      assert Enum.at(topic.partitions, 0).partition_id == 0
    end
  end

  describe "parse_response/1 for V1" do
    test "parses response with controller_id" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil},
          %{node_id: 2, host: "broker2.example.com", port: 9092, rack: "rack-1"}
        ],
        controller_id: 2,
        topic_metadata: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.controller_id == 2
      assert cluster_metadata.brokers[1].rack == nil
      assert cluster_metadata.brokers[2].rack == "rack-1"
    end

    test "parses response with is_internal flag" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topic_metadata: [
          %{
            error_code: 0,
            topic: "user-topic",
            is_internal: false,
            partition_metadata: [
              %{error_code: 0, partition: 0, leader: 1, replicas: [1], isr: [1]}
            ]
          },
          %{
            error_code: 0,
            topic: "__consumer_offsets",
            is_internal: true,
            partition_metadata: [
              %{error_code: 0, partition: 0, leader: 1, replicas: [1], isr: [1]}
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.topics["user-topic"].is_internal == false
      assert cluster_metadata.topics["__consumer_offsets"].is_internal == true
    end

    test "parses response with rack information" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: "us-east-1a"},
          %{node_id: 2, host: "broker2.example.com", port: 9092, rack: "us-east-1b"},
          %{node_id: 3, host: "broker3.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topic_metadata: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert cluster_metadata.brokers[1].rack == "us-east-1a"
      assert cluster_metadata.brokers[2].rack == "us-east-1b"
      assert cluster_metadata.brokers[3].rack == nil
    end
  end

  describe "parse_response/1 for V2" do
    test "parses response with cluster_id" do
      response = %V2.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        cluster_id: "kafka-cluster-prod",
        controller_id: 1,
        topic_metadata: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      # Note: cluster_id is currently not stored in ClusterMetadata
      # This test verifies parsing works even though cluster_id is not persisted
      assert %ClusterMetadata{} = cluster_metadata
      assert cluster_metadata.controller_id == 1
    end

    test "parses response with nil cluster_id" do
      response = %V2.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        cluster_id: nil,
        controller_id: 1,
        topic_metadata: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert %ClusterMetadata{} = cluster_metadata
      assert cluster_metadata.controller_id == 1
    end

    test "parses response with all V2 features" do
      response = %V2.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: "rack-a"},
          %{node_id: 2, host: "broker2.example.com", port: 9092, rack: "rack-b"}
        ],
        cluster_id: "prod-cluster-1",
        controller_id: 2,
        topic_metadata: [
          %{
            error_code: 0,
            topic: "events",
            is_internal: false,
            partition_metadata: [
              %{error_code: 0, partition: 0, leader: 1, replicas: [1, 2], isr: [1, 2]},
              %{error_code: 0, partition: 1, leader: 2, replicas: [2, 1], isr: [2, 1]}
            ]
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.brokers) == 2
      assert cluster_metadata.controller_id == 2
      assert map_size(cluster_metadata.topics) == 1

      topic = cluster_metadata.topics["events"]
      assert topic.name == "events"
      assert topic.is_internal == false
      assert length(topic.partitions) == 2
      assert topic.partition_leaders == %{0 => 1, 1 => 2}
    end
  end

  describe "parse_response/1 edge cases" do
    test "handles empty brokers list (V0)" do
      response = %V0.Response{
        brokers: [],
        topic_metadata: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.brokers) == 0
      assert map_size(cluster_metadata.topics) == 0
    end

    test "handles empty topics list (V1)" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topic_metadata: []
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      assert map_size(cluster_metadata.topics) == 0
    end

    test "handles topic with no partitions (V1)" do
      response = %V1.Response{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topic_metadata: [
          %{
            error_code: 0,
            topic: "empty-topic",
            is_internal: false,
            partition_metadata: []
          }
        ]
      }

      {:ok, cluster_metadata} = MetadataResponse.parse_response(response)

      topic = cluster_metadata.topics["empty-topic"]
      assert topic.name == "empty-topic"
      assert length(topic.partitions) == 0
      assert topic.partition_leaders == %{}
    end
  end
end
