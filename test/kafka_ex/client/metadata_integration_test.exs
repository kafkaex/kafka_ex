defmodule KafkaEx.Client.MetadataIntegrationTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.{RequestBuilder, ResponseParser}
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Protocol.KayrockProtocol
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  describe "RequestBuilder.metadata_request/2" do
    test "builds request for all topics (nil)" do
      state = build_test_state()

      {:ok, request} = RequestBuilder.metadata_request([topics: nil], state)

      assert Fixtures.request_type?(request, :metadata, 1)
      assert request.topics == nil
    end

    test "builds request for all topics (empty list)" do
      state = build_test_state()

      {:ok, request} = RequestBuilder.metadata_request([topics: []], state)

      assert Fixtures.request_type?(request, :metadata, 1)
      assert request.topics == nil
    end

    test "builds request for specific topics" do
      state = build_test_state()

      {:ok, request} = RequestBuilder.metadata_request([topics: ["topic1", "topic2"]], state)

      assert Fixtures.request_type?(request, :metadata, 1)
      assert request.topics == ["topic1", "topic2"]
    end

    test "builds request with custom API version" do
      state = build_test_state()

      {:ok, request} = RequestBuilder.metadata_request([topics: nil, api_version: 0], state)

      assert Fixtures.request_type?(request, :metadata, 0)
      assert request.topics == []
    end

    test "builds request with V2 API version" do
      # Default state already supports V0-V5
      state = build_test_state()

      {:ok, request} = RequestBuilder.metadata_request([topics: ["test"], api_version: 2], state)

      assert Fixtures.request_type?(request, :metadata, 2)
      assert request.topics == ["test"]
    end

    test "returns error for unsupported API version" do
      state = build_test_state(%{3 => {0, 1}})

      result = RequestBuilder.metadata_request([topics: nil, api_version: 5], state)

      assert {:error, :api_version_no_supported} = result
    end
  end

  describe "ResponseParser.metadata_response/1" do
    test "parses V0 response with single broker and topic" do
      response = Fixtures.build_response(:metadata, 0,
        brokers: [
          %{node_id: 1, host: "broker1.local", port: 9092}
        ],
        topics: [
          %{
            error_code: 0,
            name: "test-topic",
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                replica_nodes: [1],
                isr_nodes: [1]
              }
            ]
          }
        ]
      )

      {:ok, cluster_metadata} = ResponseParser.metadata_response(response)

      assert %ClusterMetadata{} = cluster_metadata
      assert map_size(cluster_metadata.brokers) == 1
      assert map_size(cluster_metadata.topics) == 1
      assert cluster_metadata.brokers[1].host == "broker1.local"
      assert cluster_metadata.topics["test-topic"].name == "test-topic"
    end

    test "parses V1 response with controller and rack" do
      response = Fixtures.build_response(:metadata, 1,
        brokers: [
          %{node_id: 1, host: "broker1.local", port: 9092, rack: "rack-1"},
          %{node_id: 2, host: "broker2.local", port: 9092, rack: "rack-2"}
        ],
        controller_id: 2,
        topics: [
          %{
            error_code: 0,
            name: "internal-topic",
            is_internal: true,
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1, 2], isr_nodes: [1, 2]}
            ]
          }
        ]
      )

      {:ok, cluster_metadata} = ResponseParser.metadata_response(response)

      assert cluster_metadata.controller_id == 2
      assert cluster_metadata.brokers[1].rack == "rack-1"
      assert cluster_metadata.brokers[2].rack == "rack-2"
      assert cluster_metadata.topics["internal-topic"].is_internal == true
    end

    test "parses V2 response with cluster_id" do
      response = Fixtures.build_response(:metadata, 2,
        brokers: [
          %{node_id: 1, host: "broker1.local", port: 9092, rack: nil}
        ],
        cluster_id: "test-cluster-id",
        controller_id: 1,
        topics: []
      )

      {:ok, cluster_metadata} = ResponseParser.metadata_response(response)

      assert %ClusterMetadata{} = cluster_metadata
      assert cluster_metadata.controller_id == 1
    end

    test "filters out topics with errors" do
      response = Fixtures.build_response(:metadata, 1,
        brokers: [
          %{node_id: 1, host: "broker1.local", port: 9092, rack: nil}
        ],
        controller_id: 1,
        topics: [
          %{
            error_code: 0,
            name: "good-topic",
            is_internal: false,
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          },
          %{
            error_code: 3,
            # UNKNOWN_TOPIC_OR_PARTITION
            name: "bad-topic",
            is_internal: false,
            partitions: []
          }
        ]
      )

      {:ok, cluster_metadata} = ResponseParser.metadata_response(response)

      assert map_size(cluster_metadata.topics) == 1
      assert cluster_metadata.topics["good-topic"]
      refute cluster_metadata.topics["bad-topic"]
    end

    test "handles empty response" do
      response = Fixtures.build_response(:metadata, 1,
        brokers: [],
        controller_id: -1,
        topics: []
      )

      {:ok, cluster_metadata} = ResponseParser.metadata_response(response)

      assert map_size(cluster_metadata.brokers) == 0
      assert map_size(cluster_metadata.topics) == 0
      assert cluster_metadata.controller_id == -1
    end
  end

  describe "KayrockProtocol integration" do
    test "builds and parses V0 request/response" do
      # Build request
      request = KayrockProtocol.build_request(:metadata, 0, topics: ["topic1"])
      assert Fixtures.request_type?(request, :metadata, 0)
      assert request.topics == ["topic1"]

      # Parse response
      response = Fixtures.build_response(:metadata, 0,
        brokers: [%{node_id: 1, host: "localhost", port: 9092}],
        topics: [
          %{
            error_code: 0,
            name: "topic1",
            partitions: [
              %{error_code: 0, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1]}
            ]
          }
        ]
      )

      {:ok, cluster_metadata} = KayrockProtocol.parse_response(:metadata, response)
      assert %ClusterMetadata{} = cluster_metadata
      assert cluster_metadata.topics["topic1"]
    end

    test "builds and parses V1 request/response" do
      # Build request
      request = KayrockProtocol.build_request(:metadata, 1, topics: nil)
      assert Fixtures.request_type?(request, :metadata, 1)
      assert request.topics == nil

      # Parse response
      response = Fixtures.build_response(:metadata, 1,
        brokers: [%{node_id: 1, host: "localhost", port: 9092, rack: "us-east-1a"}],
        controller_id: 1,
        topics: []
      )

      {:ok, cluster_metadata} = KayrockProtocol.parse_response(:metadata, response)
      assert cluster_metadata.controller_id == 1
      assert cluster_metadata.brokers[1].rack == "us-east-1a"
    end

    test "builds and parses V2 request/response" do
      # Build request
      request = KayrockProtocol.build_request(:metadata, 2, topics: ["test"])
      assert Fixtures.request_type?(request, :metadata, 2)
      assert request.topics == ["test"]

      # Parse response
      response = Fixtures.build_response(:metadata, 2,
        brokers: [%{node_id: 1, host: "localhost", port: 9092, rack: nil}],
        cluster_id: "prod-cluster",
        controller_id: 1,
        topics: []
      )

      {:ok, cluster_metadata} = KayrockProtocol.parse_response(:metadata, response)
      assert cluster_metadata.controller_id == 1
    end
  end

  # Helper functions

  defp build_test_state(api_versions \\ %{}) do
    # API versions use api_key (integer), not atom
    # Metadata api_key = 3
    default_versions = %{
      3 => {0, 5}
    }

    versions = Map.merge(default_versions, api_versions)

    %State{
      cluster_metadata: %ClusterMetadata{},
      api_versions: versions,
      consumer_group_for_auto_commit: nil,
      allow_auto_topic_creation: false
    }
  end
end
