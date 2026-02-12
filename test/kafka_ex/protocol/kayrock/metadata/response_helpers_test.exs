defmodule KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Metadata.ResponseHelpers
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.PartitionInfo
  alias KafkaEx.Cluster.Topic

  describe "to_cluster_metadata/2" do
    test "converts response to ClusterMetadata" do
      response = %{
        brokers: [
          %{node_id: 1, host: "broker1.example.com", port: 9092}
        ],
        topics: [
          %{
            name: "test-topic",
            error_code: 0,
            partitions: [
              %{partition_index: 0, error_code: 0, leader_id: 1, replica_nodes: [1, 2], isr_nodes: [1, 2]}
            ]
          }
        ]
      }

      assert {:ok, %ClusterMetadata{} = metadata} = ResponseHelpers.to_cluster_metadata(response)
      assert map_size(metadata.brokers) == 1
      assert map_size(metadata.topics) == 1
    end

    test "extracts controller_id when present" do
      response = %{
        brokers: [],
        topics: [],
        controller_id: 5
      }

      assert {:ok, %ClusterMetadata{} = metadata} = ResponseHelpers.to_cluster_metadata(response)
      assert metadata.controller_id == 5
    end

    test "handles nil controller_id" do
      response = %{
        brokers: [],
        topics: []
      }

      assert {:ok, %ClusterMetadata{} = metadata} = ResponseHelpers.to_cluster_metadata(response)
      assert metadata.controller_id == nil
    end
  end

  describe "parse_brokers/1" do
    test "parses single broker" do
      brokers = [
        %{node_id: 1, host: "broker1.example.com", port: 9092}
      ]

      result = ResponseHelpers.parse_brokers(brokers)

      assert map_size(result) == 1
      assert %Broker{} = result[1]
      assert result[1].node_id == 1
      assert result[1].host == "broker1.example.com"
      assert result[1].port == 9092
    end

    test "parses multiple brokers" do
      brokers = [
        %{node_id: 1, host: "broker1", port: 9092},
        %{node_id: 2, host: "broker2", port: 9093},
        %{node_id: 3, host: "broker3", port: 9094}
      ]

      result = ResponseHelpers.parse_brokers(brokers)

      assert map_size(result) == 3
      assert result[1].host == "broker1"
      assert result[2].host == "broker2"
      assert result[3].host == "broker3"
    end

    test "includes rack when present" do
      brokers = [
        %{node_id: 1, host: "broker1", port: 9092, rack: "us-east-1a"}
      ]

      result = ResponseHelpers.parse_brokers(brokers)

      assert result[1].rack == "us-east-1a"
    end

    test "handles nil rack" do
      brokers = [
        %{node_id: 1, host: "broker1", port: 9092}
      ]

      result = ResponseHelpers.parse_brokers(brokers)

      assert result[1].rack == nil
    end
  end

  describe "parse_topics/1" do
    test "parses single topic with partitions" do
      topics = [
        %{
          name: "test-topic",
          error_code: 0,
          partitions: [
            %{partition_index: 0, error_code: 0, leader_id: 1, replica_nodes: [1, 2], isr_nodes: [1, 2]}
          ]
        }
      ]

      result = ResponseHelpers.parse_topics(topics)

      assert map_size(result) == 1
      assert %Topic{} = result["test-topic"]
      assert result["test-topic"].name == "test-topic"
    end

    test "filters out topics with errors" do
      topics = [
        %{name: "good-topic", error_code: 0, partitions: []},
        %{name: "bad-topic", error_code: 3, partitions: []}
      ]

      result = ResponseHelpers.parse_topics(topics)

      assert map_size(result) == 1
      assert Map.has_key?(result, "good-topic")
      refute Map.has_key?(result, "bad-topic")
    end

    test "parses is_internal flag" do
      topics = [
        %{name: "__consumer_offsets", error_code: 0, is_internal: true, partitions: []},
        %{name: "user-topic", error_code: 0, is_internal: false, partitions: []}
      ]

      result = ResponseHelpers.parse_topics(topics)

      assert result["__consumer_offsets"].is_internal == true
      assert result["user-topic"].is_internal == false
    end

    test "builds partition_leaders map" do
      topics = [
        %{
          name: "test-topic",
          error_code: 0,
          partitions: [
            %{partition_index: 0, error_code: 0, leader_id: 1, replica_nodes: [], isr_nodes: []},
            %{partition_index: 1, error_code: 0, leader_id: 2, replica_nodes: [], isr_nodes: []}
          ]
        }
      ]

      result = ResponseHelpers.parse_topics(topics)

      topic = result["test-topic"]
      assert topic.partition_leaders[0] == 1
      assert topic.partition_leaders[1] == 2
    end
  end

  describe "parse_partitions/1" do
    test "parses partition metadata" do
      partitions = [
        %{partition_index: 0, error_code: 0, leader_id: 1, replica_nodes: [1, 2, 3], isr_nodes: [1, 2]}
      ]

      result = ResponseHelpers.parse_partitions(partitions)

      assert length(result) == 1
      [partition] = result
      assert %PartitionInfo{} = partition
      assert partition.partition_id == 0
      assert partition.leader == 1
      assert partition.replicas == [1, 2, 3]
      assert partition.isr == [1, 2]
    end

    test "filters out partitions with errors" do
      partitions = [
        %{partition_index: 0, error_code: 0, leader_id: 1, replica_nodes: [], isr_nodes: []},
        %{partition_index: 1, error_code: 9, leader_id: -1, replica_nodes: [], isr_nodes: []}
      ]

      result = ResponseHelpers.parse_partitions(partitions)

      assert length(result) == 1
      [partition] = result
      assert partition.partition_id == 0
    end

    test "handles nil replicas and isr" do
      partitions = [
        %{partition_index: 0, error_code: 0, leader_id: 1, replica_nodes: nil, isr_nodes: nil}
      ]

      result = ResponseHelpers.parse_partitions(partitions)

      [partition] = result
      assert partition.replicas == []
      assert partition.isr == []
    end
  end

  describe "check_for_errors/1" do
    test "returns :ok when no topic errors" do
      response = %{
        topics: [
          %{name: "topic1", error_code: 0},
          %{name: "topic2", error_code: 0}
        ]
      }

      assert :ok = ResponseHelpers.check_for_errors(response)
    end

    test "returns error tuple with topic errors" do
      response = %{
        topics: [
          %{name: "good-topic", error_code: 0},
          %{name: "unknown-topic", error_code: 3},
          %{name: "auth-topic", error_code: 29}
        ]
      }

      assert {:error, {:topic_errors, errors}} = ResponseHelpers.check_for_errors(response)
      assert length(errors) == 2
      assert {"unknown-topic", :unknown_topic_or_partition} in errors
      assert {"auth-topic", :topic_authorization_failed} in errors
    end
  end
end
