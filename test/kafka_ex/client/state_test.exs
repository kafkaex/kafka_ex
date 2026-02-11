defmodule KafkaEx.Client.StateTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.State
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Broker

  describe "static_init/2" do
    test "initializes state with default values" do
      state = State.static_init([], :test_worker)

      assert state.bootstrap_uris == []
      assert state.worker_name == :test_worker
      assert state.metadata_update_interval == 30_000
      assert state.consumer_group_update_interval == 30_000
      assert state.allow_auto_topic_creation == true
      assert state.use_ssl == false
      assert state.ssl_options == []
      assert state.auth == nil
      assert state.consumer_group_for_auto_commit == nil
    end

    test "initializes state with custom values" do
      args = [
        uris: [{"localhost", 9092}],
        metadata_update_interval: 60_000,
        consumer_group_update_interval: 120_000,
        allow_auto_topic_creation: false,
        use_ssl: true,
        ssl_options: [verify: :verify_peer],
        auth: {:sasl, :plain, "user", "pass"},
        consumer_group: "my-group"
      ]

      state = State.static_init(args, :custom_worker)

      assert state.bootstrap_uris == [{"localhost", 9092}]
      assert state.worker_name == :custom_worker
      assert state.metadata_update_interval == 60_000
      assert state.consumer_group_update_interval == 120_000
      assert state.allow_auto_topic_creation == false
      assert state.use_ssl == true
      assert state.ssl_options == [verify: :verify_peer]
      assert state.auth == {:sasl, :plain, "user", "pass"}
      assert state.consumer_group_for_auto_commit == "my-group"
    end
  end

  describe "increment_correlation_id/1" do
    test "increments correlation_id by 1" do
      state = %State{correlation_id: 0}

      state = State.increment_correlation_id(state)
      assert state.correlation_id == 1

      state = State.increment_correlation_id(state)
      assert state.correlation_id == 2
    end
  end

  describe "select_broker/2" do
    test "returns error when broker not found" do
      state = %State{cluster_metadata: %ClusterMetadata{}}

      selector = NodeSelector.topic_partition("unknown-topic", 0)
      assert {:error, _} = State.select_broker(state, selector)
    end

    test "returns error for unknown consumer group" do
      state = %State{cluster_metadata: %ClusterMetadata{}}

      selector = NodeSelector.consumer_group("unknown-group")
      assert {:error, :no_such_consumer_group} = State.select_broker(state, selector)
    end
  end

  describe "update_brokers/2" do
    test "applies callback to all brokers" do
      broker1 = %Broker{node_id: 1, host: "host1", port: 9092}
      broker2 = %Broker{node_id: 2, host: "host2", port: 9092}

      cluster_metadata = %ClusterMetadata{
        brokers: %{1 => broker1, 2 => broker2}
      }

      state = %State{cluster_metadata: cluster_metadata}

      updated_state =
        State.update_brokers(state, fn broker ->
          %{broker | port: 9093}
        end)

      brokers = State.brokers(updated_state)
      assert Enum.all?(brokers, fn b -> b.port == 9093 end)
    end
  end

  describe "brokers/1" do
    test "returns all brokers from cluster metadata" do
      broker1 = %Broker{node_id: 1, host: "host1", port: 9092}
      broker2 = %Broker{node_id: 2, host: "host2", port: 9092}

      cluster_metadata = %ClusterMetadata{
        brokers: %{1 => broker1, 2 => broker2}
      }

      state = %State{cluster_metadata: cluster_metadata}

      brokers = State.brokers(state)
      assert length(brokers) == 2
    end
  end

  describe "ingest_api_versions/2" do
    alias KafkaEx.Messages.ApiVersions

    test "ingests api versions from parsed ApiVersions struct" do
      state = %State{api_versions: %{}}

      api_versions = ApiVersions.build(
        api_versions: %{
          0 => %{min_version: 0, max_version: 8},
          1 => %{min_version: 0, max_version: 11},
          2 => %{min_version: 0, max_version: 5}
        }
      )

      updated_state = State.ingest_api_versions(state, api_versions)

      assert updated_state.api_versions == %{
               0 => {0, 8},
               1 => {0, 11},
               2 => {0, 5}
             }
    end

    test "ingests api versions with throttle_time_ms" do
      state = %State{api_versions: %{}}

      api_versions = ApiVersions.build(
        api_versions: %{
          3 => %{min_version: 0, max_version: 9},
          18 => %{min_version: 0, max_version: 2}
        },
        throttle_time_ms: 0
      )

      updated_state = State.ingest_api_versions(state, api_versions)

      assert updated_state.api_versions == %{
               3 => {0, 9},
               18 => {0, 2}
             }
    end

    test "replaces existing api versions" do
      state = %State{api_versions: %{0 => {0, 5}}}

      api_versions = ApiVersions.build(
        api_versions: %{
          0 => %{min_version: 0, max_version: 8}
        }
      )

      updated_state = State.ingest_api_versions(state, api_versions)

      assert updated_state.api_versions == %{0 => {0, 8}}
    end
  end

  describe "max_supported_api_version/3" do
    test "returns min of broker and kayrock version" do
      # Assume broker supports up to version 10 for produce (api_key 0)
      state = %State{api_versions: %{0 => {0, 10}}}

      # The actual max will be min(broker_max, kayrock_max)
      # This test verifies the function doesn't crash
      result = State.max_supported_api_version(state, :produce, 0)
      assert is_integer(result)
    end

    test "returns default when api not in cache" do
      state = %State{api_versions: %{}}

      result = State.max_supported_api_version(state, :produce, 0)
      assert result == 0
    end
  end

  describe "put_consumer_group_coordinator/3" do
    test "stores consumer group coordinator in cluster metadata" do
      state = %State{cluster_metadata: %ClusterMetadata{}}

      updated_state =
        State.put_consumer_group_coordinator(state, "my-group", 1)

      coordinators = updated_state.cluster_metadata.consumer_group_coordinators

      assert Map.get(coordinators, "my-group") == 1
    end
  end

  describe "remove_topics/2" do
    test "removes topics from cluster metadata" do
      topic1 = %KafkaEx.Cluster.Topic{name: "topic1", partitions: []}
      topic2 = %KafkaEx.Cluster.Topic{name: "topic2", partitions: []}

      cluster_metadata = %ClusterMetadata{
        topics: %{
          "topic1" => topic1,
          "topic2" => topic2
        }
      }

      state = %State{cluster_metadata: cluster_metadata}

      updated_state = State.remove_topics(state, ["topic1"])

      topics = updated_state.cluster_metadata.topics
      refute Map.has_key?(topics, "topic1")
      assert Map.has_key?(topics, "topic2")
    end
  end

  describe "topics_metadata/2" do
    test "delegates to ClusterMetadata" do
      state = %State{cluster_metadata: %ClusterMetadata{}}

      # Should not crash, returns empty for unknown topics
      result = State.topics_metadata(state, ["unknown"])
      assert is_list(result)
    end
  end
end
