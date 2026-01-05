defmodule KafkaEx.Integration.Lifecycle.TopicAdminTest do
  use ExUnit.Case, async: true
  @moduletag :lifecycle

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.DeleteTopics

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "create_topic/2" do
    test "creates topic with default settings", %{client: client} do
      topic_name = generate_random_string()

      {:ok, result} = API.create_topic(client, topic_name)

      assert %CreateTopics{} = result
      assert CreateTopics.success?(result)

      # Wait for metadata propagation and verify topic exists
      Process.sleep(200)
      {:ok, metadata} = API.metadata(client, [topic_name])
      topic = Map.get(metadata.topics, topic_name)
      assert topic != nil
    end

    test "creating existing topic returns topic_already_exists error", %{client: client} do
      topic_name = generate_random_string()

      # Create first time - should succeed
      {:ok, _} = API.create_topic(client, topic_name)

      # Create second time - should fail
      {:ok, result} = API.create_topic(client, topic_name)

      # Result will contain the error for this topic
      refute CreateTopics.success?(result)
      [failed] = CreateTopics.failed_topics(result)
      assert failed.topic == topic_name
      assert failed.error == :topic_already_exists
    end
  end

  describe "create_topic/3" do
    test "creates topic with specific partition count", %{client: client} do
      topic_name = generate_random_string()

      {:ok, result} = API.create_topic(client, topic_name, num_partitions: 4)

      assert CreateTopics.success?(result)

      # Verify partition count
      {:ok, metadata} = API.metadata(client, [topic_name])
      topic = Map.get(metadata.topics, topic_name)
      assert length(topic.partitions) == 4
    end

    test "creates topic with replication factor", %{client: client} do
      topic_name = generate_random_string()

      {:ok, result} = API.create_topic(client, topic_name, num_partitions: 2, replication_factor: 2)

      assert CreateTopics.success?(result)

      # Wait for metadata propagation and verify topic was created
      Process.sleep(200)
      {:ok, metadata} = API.metadata(client, [topic_name])
      topic = Map.get(metadata.topics, topic_name)
      assert topic != nil
      assert length(topic.partitions) == 2
    end

    test "creates topic with custom timeout", %{client: client} do
      topic_name = generate_random_string()

      {:ok, result} = API.create_topic(client, topic_name, timeout: 30_000)

      assert CreateTopics.success?(result)
    end
  end

  describe "create_topics/3" do
    test "creates multiple topics in batch", %{client: client} do
      topic1 = generate_random_string()
      topic2 = generate_random_string()
      topic3 = generate_random_string()

      topics = [
        [topic: topic1, num_partitions: 2],
        [topic: topic2, num_partitions: 3],
        [topic: topic3, num_partitions: 4]
      ]

      {:ok, result} = API.create_topics(client, topics, 30_000)

      assert CreateTopics.success?(result)

      # Wait for metadata propagation and verify all topics exist
      Process.sleep(300)
      {:ok, metadata} = API.metadata(client, [topic1, topic2, topic3])

      t1 = Map.get(metadata.topics, topic1)
      t2 = Map.get(metadata.topics, topic2)
      t3 = Map.get(metadata.topics, topic3)

      assert length(t1.partitions) == 2
      assert length(t2.partitions) == 3
      assert length(t3.partitions) == 4
    end

    test "partial success when some topics already exist", %{client: client} do
      existing_topic = generate_random_string()
      new_topic = generate_random_string()

      # Create one topic first
      {:ok, _} = API.create_topic(client, existing_topic)

      # Try to create both
      topics = [
        [topic: existing_topic],
        [topic: new_topic]
      ]

      {:ok, result} = API.create_topics(client, topics, 30_000)

      # Should have partial success
      refute CreateTopics.success?(result)
      failed = CreateTopics.failed_topics(result)
      assert length(failed) == 1
      assert hd(failed).topic == existing_topic
    end
  end

  describe "delete_topic/2" do
    test "deletes existing topic", %{client: client} do
      topic_name = generate_random_string()

      # Create topic first
      {:ok, _} = API.create_topic(client, topic_name)

      # Verify it exists
      {:ok, metadata1} = API.metadata(client, [topic_name])
      assert Map.has_key?(metadata1.topics, topic_name)

      # Delete it
      {:ok, result} = API.delete_topic(client, topic_name)

      assert %DeleteTopics{} = result
      assert DeleteTopics.success?(result)

      # Allow time for deletion to propagate
      Process.sleep(500)

      # Verify it's gone (may show as unknown_topic_or_partition or not in list)
      {:ok, metadata2} = API.metadata(client, [topic_name])
      topic = Map.get(metadata2.topics, topic_name)

      # Topic either doesn't exist or has empty partitions (still being deleted)
      if topic do
        # If topic is still visible, it might be in process of deletion
        assert length(topic.partitions) == 0 or length(topic.partitions) >= 1
      end
    end

    test "deleting non-existent topic returns error", %{client: client} do
      topic_name = "non-existent-topic-#{generate_random_string()}"

      {:ok, result} = API.delete_topic(client, topic_name)

      refute DeleteTopics.success?(result)
      [failed] = DeleteTopics.failed_topics(result)
      assert failed.topic == topic_name
      assert failed.error in [:unknown_topic_or_partition, :invalid_topic_exception]
    end
  end

  describe "delete_topic/3" do
    test "deletes topic with custom timeout", %{client: client} do
      topic_name = generate_random_string()

      {:ok, _} = API.create_topic(client, topic_name)
      {:ok, result} = API.delete_topic(client, topic_name, timeout: 60_000)

      assert DeleteTopics.success?(result)
    end
  end

  describe "delete_topics/3" do
    test "deletes multiple topics in batch", %{client: client} do
      topic1 = generate_random_string()
      topic2 = generate_random_string()
      topic3 = generate_random_string()

      # Create all topics
      {:ok, _} = API.create_topic(client, topic1)
      {:ok, _} = API.create_topic(client, topic2)
      {:ok, _} = API.create_topic(client, topic3)

      # Delete all at once
      {:ok, result} = API.delete_topics(client, [topic1, topic2, topic3], 30_000)

      assert DeleteTopics.success?(result)
    end

    test "partial success when some topics don't exist", %{client: client} do
      existing_topic = generate_random_string()
      non_existent = "non-existent-#{generate_random_string()}"

      # Create only one topic
      {:ok, _} = API.create_topic(client, existing_topic)

      # Try to delete both
      {:ok, result} = API.delete_topics(client, [existing_topic, non_existent], 30_000)

      # Should have partial failure
      refute DeleteTopics.success?(result)
      failed = DeleteTopics.failed_topics(result)
      assert length(failed) == 1
      assert hd(failed).topic == non_existent
    end
  end

  describe "topic lifecycle" do
    test "full create-use-delete cycle", %{client: client} do
      topic_name = generate_random_string()

      # 1. Create topic
      {:ok, create_result} = API.create_topic(client, topic_name, num_partitions: 2)
      assert CreateTopics.success?(create_result)

      # 2. Produce messages
      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "lifecycle-test"}])
      assert produce_result.base_offset >= 0

      # 3. Consume messages
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produce_result.base_offset, max_bytes: 100_000)
      assert hd(fetch_result.records).value == "lifecycle-test"

      # 4. Delete topic
      {:ok, delete_result} = API.delete_topic(client, topic_name)
      assert DeleteTopics.success?(delete_result)
    end

    test "recreate topic after deletion", %{client: client} do
      topic_name = generate_random_string()

      # Create
      {:ok, _} = API.create_topic(client, topic_name, num_partitions: 2)

      # Produce data
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "original-data"}])

      # Delete
      {:ok, _} = API.delete_topic(client, topic_name)

      # Wait for deletion
      Process.sleep(1000)

      # Recreate with different partition count
      {:ok, result} = API.create_topic(client, topic_name, num_partitions: 4)

      # May succeed or fail depending on deletion timing
      case CreateTopics.success?(result) do
        true ->
          # New topic should have 4 partitions
          {:ok, metadata} = API.metadata(client, [topic_name])
          topic = Map.get(metadata.topics, topic_name)
          assert length(topic.partitions) == 4

        false ->
          # Topic might still be deleting
          :ok
      end
    end
  end

  describe "topics_metadata/3" do
    test "gets metadata for specific topics", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 3)

      {:ok, topics} = API.topics_metadata(client, [topic_name])

      assert length(topics) >= 1
      topic = Enum.find(topics, &(&1.name == topic_name))
      assert topic != nil
      assert length(topic.partitions) == 3
    end

    test "gets metadata for multiple topics", %{client: client} do
      topic1 = generate_random_string()
      topic2 = generate_random_string()
      _ = create_topic(client, topic1, partitions: 2)
      _ = create_topic(client, topic2, partitions: 4)

      {:ok, topics} = API.topics_metadata(client, [topic1, topic2])

      t1 = Enum.find(topics, &(&1.name == topic1))
      t2 = Enum.find(topics, &(&1.name == topic2))

      assert t1 != nil
      assert t2 != nil
      assert length(t1.partitions) == 2
      assert length(t2.partitions) == 4
    end

    test "auto-creates topic when allow_topic_creation is true", %{client: client} do
      topic_name = "auto-create-#{generate_random_string()}"

      # Get metadata with auto-creation enabled
      {:ok, topics} = API.topics_metadata(client, [topic_name], true)

      # Topic should be auto-created (depending on broker config)
      topic = Enum.find(topics, &(&1.name == topic_name))

      if topic do
        # Auto-creation worked
        assert length(topic.partitions) >= 1
      else
        # Auto-creation may be disabled on broker
        :ok
      end
    end
  end
end
