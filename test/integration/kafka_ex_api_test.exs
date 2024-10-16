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
end
