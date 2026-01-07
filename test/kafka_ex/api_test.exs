defmodule KafkaEx.APITest do
  use ExUnit.Case, async: true

  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Cluster.Topic
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.DeleteTopics
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Messages.Heartbeat
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.Messages.SyncGroup
  alias KafkaEx.Test.MockClient

  describe "child_spec/1" do
    test "returns valid child spec" do
      spec = KafkaEx.API.child_spec(name: TestClient, brokers: [{"localhost", 9092}])

      assert spec.id == TestClient
      assert spec.type == :worker
      assert spec.restart == :permanent
      assert {KafkaEx.Client, :start_link, [opts]} = spec.start
      assert opts[:name] == TestClient
      assert opts[:brokers] == [{"localhost", 9092}]
    end

    test "uses module name as default id" do
      spec = KafkaEx.API.child_spec(brokers: [{"localhost", 9092}])

      assert spec.id == KafkaEx.API
    end
  end

  # ---------------------------------------------------------------------------
  # Offset Functions Tests
  # ---------------------------------------------------------------------------

  describe "latest_offset/4" do
    test "returns offset on success" do
      offset_response = [
        %Offset{
          topic: "test-topic",
          partition_offsets: [%Offset.PartitionOffset{partition: 0, offset: 100}]
        }
      ]

      {:ok, client} = MockClient.start_link(%{list_offsets: {:ok, offset_response}})

      assert {:ok, 100} = KafkaEx.API.latest_offset(client, "test-topic", 0)
    end

    test "returns error on failure" do
      {:ok, client} = MockClient.start_link(%{list_offsets: {:error, :unknown_topic}})

      assert {:error, :unknown_topic} = KafkaEx.API.latest_offset(client, "test-topic", 0)
    end
  end

  describe "earliest_offset/4" do
    test "returns offset on success" do
      offset_response = [
        %Offset{
          topic: "test-topic",
          partition_offsets: [%Offset.PartitionOffset{partition: 0, offset: 0}]
        }
      ]

      {:ok, client} = MockClient.start_link(%{list_offsets: {:ok, offset_response}})

      assert {:ok, 0} = KafkaEx.API.earliest_offset(client, "test-topic", 0)
    end

    test "returns error on failure" do
      {:ok, client} = MockClient.start_link(%{list_offsets: {:error, :unknown_topic}})

      assert {:error, :unknown_topic} = KafkaEx.API.earliest_offset(client, "test-topic", 0)
    end
  end

  describe "list_offsets/3" do
    test "returns offsets on success" do
      offset_response = [
        %Offset{
          topic: "test-topic",
          partition_offsets: [%Offset.PartitionOffset{partition: 0, offset: 50}]
        }
      ]

      {:ok, client} = MockClient.start_link(%{list_offsets: {:ok, offset_response}})

      topic_partitions = [{"test-topic", [%{partition_num: 0, timestamp: :latest}]}]
      assert {:ok, ^offset_response} = KafkaEx.API.list_offsets(client, topic_partitions)
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{list_offsets: {:error, %{error: :unknown_topic}}})

      topic_partitions = [{"test-topic", [%{partition_num: 0, timestamp: :latest}]}]
      assert {:error, :unknown_topic} = KafkaEx.API.list_offsets(client, topic_partitions)
    end
  end

  # ---------------------------------------------------------------------------
  # Metadata Functions Tests
  # ---------------------------------------------------------------------------

  describe "metadata/2" do
    test "returns cluster metadata on success" do
      cluster = %ClusterMetadata{brokers: %{1 => %Broker{node_id: 1, host: "localhost", port: 9092}}}
      {:ok, client} = MockClient.start_link(%{metadata: {:ok, cluster}})

      assert {:ok, ^cluster} = KafkaEx.API.metadata(client)
    end

    test "returns error on failure" do
      {:ok, client} = MockClient.start_link(%{metadata: {:error, :network_error}})

      assert {:error, :network_error} = KafkaEx.API.metadata(client)
    end
  end

  describe "cluster_metadata/1" do
    test "returns cached cluster metadata" do
      cluster = %ClusterMetadata{controller_id: 1}
      {:ok, client} = MockClient.start_link(%{cluster_metadata: {:ok, cluster}})

      assert {:ok, ^cluster} = KafkaEx.API.cluster_metadata(client)
    end
  end

  describe "topics_metadata/3" do
    test "returns topic metadata list" do
      topic = %Topic{name: "test-topic", partition_leaders: %{0 => 1}}
      {:ok, client} = MockClient.start_link(%{topic_metadata: {:ok, [topic]}})

      assert {:ok, [^topic]} = KafkaEx.API.topics_metadata(client, ["test-topic"])
    end
  end

  describe "api_versions/2" do
    test "returns api versions on success" do
      versions = %ApiVersions{api_versions: [%{api_key: 0, min_version: 0, max_version: 8}]}
      {:ok, client} = MockClient.start_link(%{api_versions: {:ok, versions}})

      assert {:ok, ^versions} = KafkaEx.API.api_versions(client)
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{api_versions: {:error, %{error: :unsupported}}})

      assert {:error, :unsupported} = KafkaEx.API.api_versions(client)
    end
  end

  describe "correlation_id/1" do
    test "returns current correlation id" do
      {:ok, client} = MockClient.start_link(%{correlation_id: {:ok, 42}})

      assert {:ok, 42} = KafkaEx.API.correlation_id(client)
    end
  end

  # ---------------------------------------------------------------------------
  # Produce Functions Tests
  # ---------------------------------------------------------------------------

  describe "produce/5" do
    test "produces messages to explicit partition" do
      metadata = %RecordMetadata{topic: "test-topic", partition: 0, base_offset: 100}
      {:ok, client} = MockClient.start_link(%{produce: {:ok, metadata}})

      messages = [%{value: "hello"}]
      assert {:ok, ^metadata} = KafkaEx.API.produce(client, "test-topic", 0, messages)

      calls = MockClient.get_calls(client)
      assert [{:produce, "test-topic", 0, ^messages}] = calls
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{produce: {:error, %{error: :unknown_topic}}})

      assert {:error, :unknown_topic} = KafkaEx.API.produce(client, "test-topic", 0, [%{value: "x"}])
    end
  end

  describe "produce_one/5" do
    test "produces single message" do
      metadata = %RecordMetadata{topic: "test-topic", partition: 0, base_offset: 100}
      {:ok, client} = MockClient.start_link(%{produce: {:ok, metadata}})

      assert {:ok, ^metadata} = KafkaEx.API.produce_one(client, "test-topic", 0, "hello")

      calls = MockClient.get_calls(client)
      assert [{:produce, "test-topic", 0, [%{value: "hello"}]}] = calls
    end

    test "produces message with key" do
      metadata = %RecordMetadata{topic: "test-topic", partition: 0, base_offset: 100}
      {:ok, client} = MockClient.start_link(%{produce: {:ok, metadata}})

      assert {:ok, _} = KafkaEx.API.produce_one(client, "test-topic", 0, "hello", key: "my-key")

      calls = MockClient.get_calls(client)
      assert [{:produce, "test-topic", 0, [%{value: "hello", key: "my-key"}]}] = calls
    end
  end

  # ---------------------------------------------------------------------------
  # Fetch Functions Tests
  # ---------------------------------------------------------------------------

  describe "fetch/5" do
    test "fetches records from partition" do
      fetch_result = %Fetch{
        topic: "test-topic",
        partition: 0,
        records: [%{offset: 0, value: "hello"}]
      }

      {:ok, client} = MockClient.start_link(%{fetch: {:ok, fetch_result}})

      assert {:ok, ^fetch_result} = KafkaEx.API.fetch(client, "test-topic", 0, 0)

      calls = MockClient.get_calls(client)
      assert [{:fetch, "test-topic", 0, 0}] = calls
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{fetch: {:error, %{error: :offset_out_of_range}}})

      assert {:error, :offset_out_of_range} = KafkaEx.API.fetch(client, "test-topic", 0, 999)
    end
  end

  describe "fetch_all/4" do
    test "fetches all records from earliest offset" do
      offset_response = [
        %Offset{
          topic: "test-topic",
          partition_offsets: [%Offset.PartitionOffset{partition: 0, offset: 0}]
        }
      ]

      fetch_result = %Fetch{topic: "test-topic", partition: 0, records: []}

      {:ok, client} =
        MockClient.start_link(%{
          list_offsets: {:ok, offset_response},
          fetch: {:ok, fetch_result}
        })

      assert {:ok, ^fetch_result} = KafkaEx.API.fetch_all(client, "test-topic", 0)
    end

    test "returns error when earliest_offset fails" do
      {:ok, client} = MockClient.start_link(%{list_offsets: {:error, :unknown_topic}})

      assert {:error, :unknown_topic} = KafkaEx.API.fetch_all(client, "test-topic", 0)
    end
  end

  # ---------------------------------------------------------------------------
  # Consumer Group Functions Tests
  # ---------------------------------------------------------------------------

  describe "describe_group/3" do
    test "returns group description on success" do
      description = %ConsumerGroupDescription{
        group_id: "my-group",
        state: "Stable"
      }

      {:ok, client} = MockClient.start_link(%{describe_groups: {:ok, [description]}})

      assert {:ok, ^description} = KafkaEx.API.describe_group(client, "my-group")
    end

    test "returns error on failure" do
      {:ok, client} = MockClient.start_link(%{describe_groups: {:error, :group_not_found}})

      assert {:error, :group_not_found} = KafkaEx.API.describe_group(client, "unknown-group")
    end
  end

  describe "join_group/4" do
    test "joins consumer group successfully" do
      result = %JoinGroup{generation_id: 1, group_protocol: "roundrobin", leader_id: "member-1", member_id: "member-1"}

      {:ok, client} = MockClient.start_link(%{join_group: {:ok, result}})

      assert {:ok, ^result} = KafkaEx.API.join_group(client, "my-group", "")

      calls = MockClient.get_calls(client)
      assert [{:join_group, "my-group", ""}] = calls
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{join_group: {:error, %{error: :unknown_member_id}}})

      assert {:error, :unknown_member_id} = KafkaEx.API.join_group(client, "my-group", "invalid")
    end
  end

  describe "sync_group/5" do
    test "synchronizes group state" do
      result = %SyncGroup{partition_assignments: []}
      {:ok, client} = MockClient.start_link(%{sync_group: {:ok, result}})

      assert {:ok, ^result} = KafkaEx.API.sync_group(client, "my-group", 1, "member-1")

      calls = MockClient.get_calls(client)
      assert [{:sync_group, "my-group", 1, "member-1"}] = calls
    end
  end

  describe "leave_group/4" do
    test "leaves consumer group" do
      {:ok, client} = MockClient.start_link(%{leave_group: {:ok, :no_error}})

      assert {:ok, :no_error} = KafkaEx.API.leave_group(client, "my-group", "member-1")

      calls = MockClient.get_calls(client)
      assert [{:leave_group, "my-group", "member-1"}] = calls
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{leave_group: {:error, %{error: :unknown_member_id}}})

      assert {:error, :unknown_member_id} = KafkaEx.API.leave_group(client, "g", "invalid")
    end
  end

  describe "heartbeat/5" do
    test "sends heartbeat successfully" do
      result = %Heartbeat{throttle_time_ms: 0}
      {:ok, client} = MockClient.start_link(%{heartbeat: {:ok, result}})

      assert {:ok, ^result} = KafkaEx.API.heartbeat(client, "my-group", "member-1", 1)

      calls = MockClient.get_calls(client)
      assert [{:heartbeat, "my-group", "member-1", 1}] = calls
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{heartbeat: {:error, %{error: :rebalance_in_progress}}})

      assert {:error, :rebalance_in_progress} = KafkaEx.API.heartbeat(client, "g", "m", 1)
    end
  end

  describe "find_coordinator/3" do
    test "finds group coordinator" do
      coordinator = %FindCoordinator{
        coordinator: %Broker{node_id: 1, host: "broker1.example.com", port: 9092},
        error_code: :no_error
      }

      {:ok, client} = MockClient.start_link(%{find_coordinator: {:ok, coordinator}})

      assert {:ok, ^coordinator} = KafkaEx.API.find_coordinator(client, "my-group")

      calls = MockClient.get_calls(client)
      assert [{:find_coordinator, "my-group"}] = calls
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{find_coordinator: {:error, %{error: :coordinator_not_available}}})

      assert {:error, :coordinator_not_available} = KafkaEx.API.find_coordinator(client, "group")
    end
  end

  # ---------------------------------------------------------------------------
  # Offset Management Functions Tests
  # ---------------------------------------------------------------------------

  describe "fetch_committed_offset/5" do
    test "fetches committed offsets" do
      offsets = [%Offset{topic: "test-topic", partition_offsets: []}]
      {:ok, client} = MockClient.start_link(%{offset_fetch: {:ok, offsets}})

      partitions = [%{partition_num: 0}]

      assert {:ok, ^offsets} = KafkaEx.API.fetch_committed_offset(client, "my-group", "test-topic", partitions)
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{offset_fetch: {:error, %{error: :group_not_found}}})

      assert {:error, :group_not_found} =
               KafkaEx.API.fetch_committed_offset(client, "unknown", "topic", [%{partition_num: 0}])
    end
  end

  describe "commit_offset/5" do
    test "commits offsets successfully" do
      offsets = [%Offset{topic: "test-topic", partition_offsets: []}]
      {:ok, client} = MockClient.start_link(%{offset_commit: {:ok, offsets}})

      partitions = [%{partition_num: 0, offset: 100}]

      assert {:ok, ^offsets} = KafkaEx.API.commit_offset(client, "my-group", "test-topic", partitions)
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{offset_commit: {:error, %{error: :illegal_generation}}})

      assert {:error, :illegal_generation} =
               KafkaEx.API.commit_offset(client, "group", "topic", [%{partition_num: 0, offset: 100}])
    end
  end

  # ---------------------------------------------------------------------------
  # Topic Management Functions Tests
  # ---------------------------------------------------------------------------

  describe "create_topics/4" do
    test "creates topics successfully" do
      result = %CreateTopics{topic_results: [%CreateTopics.TopicResult{topic: "new-topic"}]}
      {:ok, client} = MockClient.start_link(%{create_topics: {:ok, result}})

      topics = [[topic: "new-topic", num_partitions: 3, replication_factor: 1]]
      assert {:ok, ^result} = KafkaEx.API.create_topics(client, topics, 10_000)

      calls = MockClient.get_calls(client)
      assert [{:create_topics, ^topics, 10_000}] = calls
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{create_topics: {:error, %{error: :topic_exists}}})

      assert {:error, :topic_exists} = KafkaEx.API.create_topics(client, [[topic: "x"]], 10_000)
    end
  end

  describe "create_topic/3" do
    test "creates single topic with defaults" do
      result = %CreateTopics{topic_results: [%CreateTopics.TopicResult{topic: "new-topic"}]}
      {:ok, client} = MockClient.start_link(%{create_topics: {:ok, result}})

      assert {:ok, ^result} = KafkaEx.API.create_topic(client, "new-topic")

      calls = MockClient.get_calls(client)
      assert [{:create_topics, [[topic: "new-topic"]], 10_000}] = calls
    end

    test "creates topic with custom configuration" do
      result = %CreateTopics{topic_results: []}
      {:ok, client} = MockClient.start_link(%{create_topics: {:ok, result}})

      assert {:ok, _} = KafkaEx.API.create_topic(client, "new-topic", num_partitions: 6, timeout: 30_000)

      calls = MockClient.get_calls(client)
      assert [{:create_topics, [[topic: "new-topic", num_partitions: 6]], 30_000}] = calls
    end
  end

  describe "delete_topics/4" do
    test "deletes topics successfully" do
      result = %DeleteTopics{topic_results: [%DeleteTopics.TopicResult{topic: "old-topic"}]}
      {:ok, client} = MockClient.start_link(%{delete_topics: {:ok, result}})

      assert {:ok, ^result} = KafkaEx.API.delete_topics(client, ["old-topic"], 30_000)

      calls = MockClient.get_calls(client)
      assert [{:delete_topics, ["old-topic"], 30_000}] = calls
    end

    test "returns error with error_code map" do
      {:ok, client} = MockClient.start_link(%{delete_topics: {:error, %{error: :unknown_topic}}})

      assert {:error, :unknown_topic} = KafkaEx.API.delete_topics(client, ["x"], 30_000)
    end
  end

  describe "delete_topic/3" do
    test "deletes single topic with defaults" do
      result = %DeleteTopics{topic_results: [%DeleteTopics.TopicResult{topic: "old-topic"}]}
      {:ok, client} = MockClient.start_link(%{delete_topics: {:ok, result}})

      assert {:ok, ^result} = KafkaEx.API.delete_topic(client, "old-topic")

      calls = MockClient.get_calls(client)
      assert [{:delete_topics, ["old-topic"], 30_000}] = calls
    end

    test "deletes topic with custom timeout" do
      result = %DeleteTopics{topic_results: []}
      {:ok, client} = MockClient.start_link(%{delete_topics: {:ok, result}})

      assert {:ok, _} = KafkaEx.API.delete_topic(client, "old-topic", timeout: 60_000)

      calls = MockClient.get_calls(client)
      assert [{:delete_topics, ["old-topic"], 60_000}] = calls
    end
  end

  describe "set_consumer_group_for_auto_commit/2" do
    test "sets consumer group" do
      {:ok, client} = MockClient.start_link(%{set_consumer_group: :ok})

      assert :ok = KafkaEx.API.set_consumer_group_for_auto_commit(client, "my-group")

      calls = MockClient.get_calls(client)
      assert [{:set_consumer_group, "my-group"}] = calls
    end

    test "returns error for invalid consumer group" do
      {:ok, client} = MockClient.start_link(%{set_consumer_group: {:error, :invalid_consumer_group}})

      assert {:error, :invalid_consumer_group} = KafkaEx.API.set_consumer_group_for_auto_commit(client, nil)
    end
  end
end
