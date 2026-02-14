defmodule KafkaEx.Integration.Lifecycle.HighestSupportedVersionTest do
  use ExUnit.Case, async: true
  @moduletag :lifecycle

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.DeleteTopics
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Messages.RecordMetadata

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    {:ok, %{client: pid}}
  end

  # ---------------------------------------------------------------------------
  # ApiVersions V2
  # ---------------------------------------------------------------------------
  describe "ApiVersions V2 (highest working)" do
    test "returns supported API version ranges", %{client: client} do
      {:ok, result} = API.api_versions(client, api_version: 2)

      assert %ApiVersions{} = result
      assert is_map(result.api_versions)
      assert map_size(result.api_versions) > 0

      # Verify well-known API keys are present
      assert Map.has_key?(result.api_versions, 0)
      assert Map.has_key?(result.api_versions, 1)
      assert Map.has_key?(result.api_versions, 3)
      assert Map.has_key?(result.api_versions, 18)

      # Each entry should have min_version and max_version
      produce_version = Map.fetch!(result.api_versions, 0)
      assert is_integer(produce_version.min_version)
      assert is_integer(produce_version.max_version)
      assert produce_version.max_version >= produce_version.min_version

      # V1+ response includes throttle_time_ms
      assert is_integer(result.throttle_time_ms)
      assert result.throttle_time_ms >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # Metadata V9 (FLEX)
  # ---------------------------------------------------------------------------
  describe "Metadata V9 (highest)" do
    test "returns broker and topic metadata", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, metadata} = API.metadata(client, [topic_name], api_version: 9)

      assert map_size(metadata.brokers) >= 1

      assert Map.has_key?(metadata.topics, topic_name)
      topic = Map.fetch!(metadata.topics, topic_name)
      assert topic.name == topic_name
      assert length(topic.partitions) >= 1
    end
  end

  # ---------------------------------------------------------------------------
  # Produce V8
  # ---------------------------------------------------------------------------
  describe "Produce V8 (highest)" do
    test "produces messages and returns RecordMetadata", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [
        %{key: "key-1", value: "value-1"},
        %{key: "key-2", value: "value-2"}
      ]

      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 8)

      assert %RecordMetadata{} = result
      assert result.topic == topic_name
      assert result.partition == 0
      assert is_integer(result.base_offset)
      assert result.base_offset >= 0

      assert is_integer(result.throttle_time_ms)
      assert result.throttle_time_ms >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # Fetch V11
  # ---------------------------------------------------------------------------
  describe "Fetch V11 (highest)" do
    test "produces then fetches records with V11-specific fields", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [
        %{key: "fk1", value: "fetch-v11-hello"},
        %{key: "fk2", value: "fetch-v11-world"}
      ]

      {:ok, produce_result} = API.produce(client, topic_name, 0, messages)

      {:ok, fetch_result} =
        API.fetch(client, topic_name, 0, produce_result.base_offset, api_version: 11)

      assert %Fetch{} = fetch_result
      assert fetch_result.topic == topic_name
      assert fetch_result.partition == 0
      assert length(fetch_result.records) == 2

      values = Enum.map(fetch_result.records, & &1.value)
      assert values == ["fetch-v11-hello", "fetch-v11-world"]

      keys = Enum.map(fetch_result.records, & &1.key)
      assert keys == ["fk1", "fk2"]

      # V4+ fields
      assert is_integer(fetch_result.high_watermark)
      assert fetch_result.high_watermark >= produce_result.base_offset + 2
      assert is_integer(fetch_result.last_stable_offset)
      assert fetch_result.last_stable_offset >= 0

      # V5+ field
      assert is_integer(fetch_result.log_start_offset)
      assert fetch_result.log_start_offset >= 0

      # V11+ field: preferred_read_replica (KIP-392)
      assert is_integer(fetch_result.preferred_read_replica) or is_nil(fetch_result.preferred_read_replica)

      assert is_integer(fetch_result.throttle_time_ms)
      assert fetch_result.throttle_time_ms >= 0
    end

    test "empty topic with min_bytes: 0 returns immediately", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # min_bytes: 0 prevents the broker from waiting max_wait_time (10s)
      # which would exceed the GenServer.call timeout (5s)
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, 0, api_version: 11, min_bytes: 0)

      assert %Fetch{} = fetch_result
      assert fetch_result.records == []
    end
  end

  # ---------------------------------------------------------------------------
  # ListOffsets V5
  # ---------------------------------------------------------------------------
  describe "ListOffsets V5 (highest)" do
    test "gets earliest and latest offsets", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "offset-msg-1"}, %{value: "offset-msg-2"}])

      {:ok, earliest} = API.earliest_offset(client, topic_name, 0, api_version: 5)
      {:ok, latest} = API.latest_offset(client, topic_name, 0, api_version: 5)

      assert is_integer(earliest)
      assert earliest >= 0
      assert is_integer(latest)
      assert latest >= 2
      assert latest > earliest
    end
  end

  # ---------------------------------------------------------------------------
  # FindCoordinator V3 (FLEX)
  # ---------------------------------------------------------------------------
  describe "FindCoordinator V3 (highest)" do
    test "finds coordinator for a consumer group", %{client: client} do
      group_id = "cg-find-coord-v3-#{generate_random_string()}"

      {:ok, result} = API.find_coordinator(client, group_id, api_version: 3)

      assert %FindCoordinator{} = result
      assert FindCoordinator.success?(result)
      assert result.error_code == :no_error
      assert result.coordinator != nil
      assert is_integer(result.coordinator.node_id)
      assert result.coordinator.node_id >= 0

      assert is_integer(result.throttle_time_ms)
      assert result.throttle_time_ms >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # CreateTopics V5 (FLEX)
  # ---------------------------------------------------------------------------
  describe "CreateTopics V5 (highest)" do
    test "creates a topic with V5-specific response fields", %{client: client} do
      topic_name = "ct-v5-#{generate_random_string()}"

      topics = [
        [
          topic: topic_name,
          num_partitions: 3,
          replication_factor: 1,
          config_entries: []
        ]
      ]

      {:ok, result} = API.create_topics(client, topics, 30_000, api_version: 5)

      assert %CreateTopics{} = result
      assert CreateTopics.success?(result)

      topic_result = CreateTopics.get_topic_result(result, topic_name)
      assert topic_result != nil
      assert topic_result.topic == topic_name
      assert topic_result.error == :no_error

      # V5+ returns num_partitions and replication_factor in the response
      assert topic_result.num_partitions == 3
      assert topic_result.replication_factor == 1

      assert is_integer(result.throttle_time_ms)
      assert result.throttle_time_ms >= 0

      _ = wait_for_topic_in_metadata(client, topic_name)
    end
  end

  # ---------------------------------------------------------------------------
  # DeleteTopics V4 (FLEX)
  # ---------------------------------------------------------------------------
  describe "DeleteTopics V4 (highest)" do
    test "creates then deletes a topic", %{client: client} do
      topic_name = "dt-v4-#{generate_random_string()}"

      {:ok, _} = API.create_topic(client, topic_name, num_partitions: 1, replication_factor: 1)
      _ = wait_for_topic_in_metadata(client, topic_name)

      {:ok, result} = API.delete_topics(client, [topic_name], 30_000, api_version: 4)

      assert %DeleteTopics{} = result
      assert DeleteTopics.success?(result)

      topic_result = DeleteTopics.get_topic_result(result, topic_name)
      assert topic_result != nil
      assert topic_result.topic == topic_name
      assert topic_result.error == :no_error

      assert is_integer(result.throttle_time_ms)
      assert result.throttle_time_ms >= 0
    end
  end
end
