defmodule KafkaEx.New.KafkaExAPITest do
  use ExUnit.Case, async: true
  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  @moduletag :integration

  alias KafkaEx.New.Client
  alias KafkaEx.New.KafkaExAPI, as: API

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "latest_offset/3" do
    test "returns latest offset for given topic, partition pair", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      {:ok, value} = API.latest_offset(client, topic_name, 0)

      assert value == 1
    end
  end

  describe "latest_offset/4" do
    test "returns latest offset for given topic, partition pair", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      {:ok, value} = API.latest_offset(client, topic_name, 0, api_version: 2)

      assert value == 1
    end
  end

  describe "earliest_offset/3" do
    test "returns latest offset for given topic, partition pair", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      {:ok, value} = API.earliest_offset(client, topic_name, 0)

      assert value == 0
    end
  end

  describe "earliest_offset/4" do
    test "returns latest offset for given topic, partition pair", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      {:ok, value} = API.earliest_offset(client, topic_name, 0, api_version: 2)

      assert value == 0
    end
  end

  describe "describe_group/2" do
    test "returns consumer group description", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = join_to_group(client, topic_name, consumer_group)

      {:ok, group} = API.describe_group(client, consumer_group)

      assert group.group_id == consumer_group
    end
  end

  describe "describe_group/3" do
    test "returns consumer group description", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = join_to_group(client, topic_name, consumer_group)

      {:ok, group} = API.describe_group(client, consumer_group, api_version: 1)

      assert group.group_id == consumer_group
    end
  end

  describe "list_offsets/2" do
    test "returns latest offset for given topic, partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)
      request_data = [{topic_name, [%{partition_num: 0, timestamp: -1}]}]

      {:ok, [offset]} = API.list_offsets(client, request_data)

      assert offset.topic == topic_name
      assert offset.partition_offsets != []
    end
  end

  describe "list_offsets/3" do
    test "returns latest offset for given topic, partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)
      request_data = [{topic_name, [%{partition_num: 0, timestamp: -1}]}]

      {:ok, [offset]} = API.list_offsets(client, request_data, api_version: 2)

      assert offset.topic == topic_name
      assert offset.partition_offsets != []
    end
  end

  describe "fetch_committed_offset/4" do
    test "fetches committed offset for consumer group", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      # Commit an offset first
      partitions_commit = [%{partition_num: 0, offset: 1}]
      {:ok, _} = API.commit_offset(client, consumer_group, topic_name, partitions_commit)

      # Fetch the committed offset
      partitions_fetch = [%{partition_num: 0}]
      {:ok, [offset]} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions_fetch)

      assert offset.topic == topic_name
      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == 1
      assert partition_offset.error_code == :no_error
    end

    test "returns -1 offset for consumer group with no committed offset", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0}]
      {:ok, [offset]} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions)

      assert offset.topic == topic_name
      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.offset == -1
      assert partition_offset.error_code == :no_error
    end

    test "fetches committed offsets for multiple partitions", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 3)
      _ = partition_produce(client, topic_name, "value", 0)
      _ = partition_produce(client, topic_name, "value", 1)
      _ = partition_produce(client, topic_name, "value", 2)

      # Commit offsets for all partitions
      partitions_commit = [
        %{partition_num: 0, offset: 1},
        %{partition_num: 1, offset: 1},
        %{partition_num: 2, offset: 1}
      ]

      {:ok, _} = API.commit_offset(client, consumer_group, topic_name, partitions_commit)

      # Fetch committed offsets
      partitions_fetch = [%{partition_num: 0}, %{partition_num: 1}, %{partition_num: 2}]
      {:ok, offsets} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions_fetch)

      assert length(offsets) == 3

      Enum.each(offsets, fn offset ->
        assert offset.topic == topic_name
        assert [partition_offset] = offset.partition_offsets
        assert partition_offset.offset == 1
        assert partition_offset.error_code == :no_error
      end)
    end
  end

  describe "fetch_committed_offset/5" do
    test "supports api_version option", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      # Commit with v2
      partitions_commit = [%{partition_num: 0, offset: 1}]
      {:ok, _} = API.commit_offset(client, consumer_group, topic_name, partitions_commit, api_version: 2)

      # Fetch with v2
      partitions_fetch = [%{partition_num: 0}]
      {:ok, [offset]} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions_fetch, api_version: 2)

      assert offset.topic == topic_name
      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 1
    end
  end

  describe "commit_offset/4" do
    test "commits offset for consumer group", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      partitions = [%{partition_num: 0, offset: 1}]
      {:ok, [result]} = API.commit_offset(client, consumer_group, topic_name, partitions)

      assert result.topic == topic_name
      assert [partition_offset] = result.partition_offsets
      assert partition_offset.partition == 0
      assert partition_offset.error_code == :no_error
    end

    test "commits offsets for multiple partitions", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 3)

      partitions = [
        %{partition_num: 0, offset: 10},
        %{partition_num: 1, offset: 20},
        %{partition_num: 2, offset: 30}
      ]

      {:ok, results} = API.commit_offset(client, consumer_group, topic_name, partitions)

      assert length(results) == 3

      Enum.each(results, fn result ->
        assert result.topic == topic_name
        assert [partition_offset] = result.partition_offsets
        assert partition_offset.error_code == :no_error
      end)
    end

    test "commit-then-fetch round trip works", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)
      _ = partition_produce(client, topic_name, "value", 0)

      # Commit offset
      partitions_commit = [%{partition_num: 0, offset: 42}]
      {:ok, _} = API.commit_offset(client, consumer_group, topic_name, partitions_commit)

      # Fetch it back
      partitions_fetch = [%{partition_num: 0}]
      {:ok, [offset]} = API.fetch_committed_offset(client, consumer_group, topic_name, partitions_fetch)

      assert [partition_offset] = offset.partition_offsets
      assert partition_offset.offset == 42
    end
  end

  describe "commit_offset/5" do
    test "supports retention_time option (v2)", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0, offset: 100}]
      {:ok, [result]} = API.commit_offset(client, consumer_group, topic_name, partitions, retention_time: 86_400_000)

      assert result.topic == topic_name
      assert [partition_offset] = result.partition_offsets
      assert partition_offset.error_code == :no_error
    end

    test "supports generation_id and member_id options (v1)", %{client: client} do
      topic_name = generate_random_string()
      consumer_group = generate_random_string()
      _ = create_topic(client, topic_name)

      partitions = [%{partition_num: 0, offset: 100}]
      opts = [api_version: 1, generation_id: 1, member_id: "test-member"]
      {:ok, [result]} = API.commit_offset(client, consumer_group, topic_name, partitions, opts)

      assert result.topic == topic_name
      assert [partition_offset] = result.partition_offsets
      assert partition_offset.error_code == :no_error
    end
  end
end
