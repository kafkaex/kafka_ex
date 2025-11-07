defmodule KafkaEx.New.Structs.MetadataTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.Broker
  alias KafkaEx.New.Structs.ClusterMetadata
  alias KafkaEx.New.Structs.Metadata
  alias KafkaEx.New.Structs.Partition
  alias KafkaEx.New.Structs.Topic

  doctest Metadata

  describe "request/0" do
    test "creates request for all topics" do
      request = Metadata.request()

      assert %Metadata.Request{} = request
      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end
  end

  describe "request/1 with topics list" do
    test "creates request for specific topics" do
      request = Metadata.request(["topic1", "topic2"])

      assert %Metadata.Request{} = request
      assert request.topics == ["topic1", "topic2"]
      assert request.allow_auto_topic_creation == false
    end

    test "creates request for all topics when given nil" do
      request = Metadata.request(nil)

      assert %Metadata.Request{} = request
      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end

    test "creates request for all topics when given empty list" do
      request = Metadata.request([])

      assert %Metadata.Request{} = request
      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end

    test "creates request for single topic" do
      request = Metadata.request(["single-topic"])

      assert %Metadata.Request{} = request
      assert request.topics == ["single-topic"]
      assert request.allow_auto_topic_creation == false
    end
  end

  describe "request/1 with options keyword list" do
    test "creates request with topics option" do
      request = Metadata.request(topics: ["topic1", "topic2"])

      assert %Metadata.Request{} = request
      assert request.topics == ["topic1", "topic2"]
      assert request.allow_auto_topic_creation == false
    end

    test "creates request with allow_auto_topic_creation option" do
      request = Metadata.request(topics: ["topic1"], allow_auto_topic_creation: true)

      assert %Metadata.Request{} = request
      assert request.topics == ["topic1"]
      assert request.allow_auto_topic_creation == true
    end

    test "creates request with nil topics option" do
      request = Metadata.request(topics: nil)

      assert %Metadata.Request{} = request
      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end

    test "creates request with empty options" do
      request = Metadata.request([])

      assert %Metadata.Request{} = request
      assert request.topics == nil
      assert request.allow_auto_topic_creation == false
    end
  end

  describe "response/2" do
    test "builds response from cluster metadata" do
      cluster_metadata = build_cluster_metadata()
      response = Metadata.response(cluster_metadata)

      assert %Metadata.Response{} = response
      assert response.cluster_metadata == cluster_metadata
      assert response.throttle_time_ms == nil
    end

    test "builds response with throttle time" do
      cluster_metadata = build_cluster_metadata()
      response = Metadata.response(cluster_metadata, throttle_time_ms: 100)

      assert %Metadata.Response{} = response
      assert response.cluster_metadata == cluster_metadata
      assert response.throttle_time_ms == 100
    end

    test "builds response with zero throttle time" do
      cluster_metadata = build_cluster_metadata()
      response = Metadata.response(cluster_metadata, throttle_time_ms: 0)

      assert %Metadata.Response{} = response
      assert response.throttle_time_ms == 0
    end
  end

  describe "topic_exists?/2" do
    test "returns true when topic exists" do
      cluster_metadata = build_cluster_metadata(topics: ["topic1", "topic2"])
      response = Metadata.response(cluster_metadata)

      assert Metadata.topic_exists?(response, "topic1")
      assert Metadata.topic_exists?(response, "topic2")
    end

    test "returns false when topic does not exist" do
      cluster_metadata = build_cluster_metadata(topics: ["topic1"])
      response = Metadata.response(cluster_metadata)

      refute Metadata.topic_exists?(response, "non-existent")
      refute Metadata.topic_exists?(response, "topic2")
    end

    test "returns false when no topics in cluster" do
      cluster_metadata = build_cluster_metadata(topics: [])
      response = Metadata.response(cluster_metadata)

      refute Metadata.topic_exists?(response, "any-topic")
    end
  end

  describe "controller/1" do
    test "returns controller broker when controller_id is set" do
      cluster_metadata = build_cluster_metadata(controller_id: 1)
      response = Metadata.response(cluster_metadata)

      assert {:ok, broker} = Metadata.controller(response)
      assert %Broker{} = broker
      assert broker.node_id == 1
    end

    test "returns error when controller_id is nil" do
      cluster_metadata = build_cluster_metadata(controller_id: nil)
      response = Metadata.response(cluster_metadata)

      assert {:error, :no_controller} = Metadata.controller(response)
    end

    test "returns error when controller_id points to non-existent broker" do
      cluster_metadata = build_cluster_metadata(controller_id: 999)
      response = Metadata.response(cluster_metadata)

      assert {:error, :no_controller} = Metadata.controller(response)
    end
  end

  describe "brokers/1" do
    test "returns all brokers from cluster metadata" do
      cluster_metadata = build_cluster_metadata()
      response = Metadata.response(cluster_metadata)

      brokers = Metadata.brokers(response)

      assert length(brokers) == 3
      assert Enum.all?(brokers, fn broker -> match?(%Broker{}, broker) end)
      assert Enum.map(brokers, & &1.node_id) |> Enum.sort() == [1, 2, 3]
    end

    test "returns empty list when no brokers" do
      cluster_metadata = %ClusterMetadata{brokers: %{}}
      response = Metadata.response(cluster_metadata)

      assert Metadata.brokers(response) == []
    end

    test "returns single broker" do
      cluster_metadata = build_cluster_metadata(broker_count: 1)
      response = Metadata.response(cluster_metadata)

      brokers = Metadata.brokers(response)

      assert length(brokers) == 1
      assert [%Broker{node_id: 1}] = brokers
    end
  end

  describe "topic/2" do
    test "returns topic when it exists" do
      cluster_metadata = build_cluster_metadata(topics: ["topic1", "topic2"])
      response = Metadata.response(cluster_metadata)

      assert {:ok, topic} = Metadata.topic(response, "topic1")
      assert %Topic{} = topic
      assert topic.name == "topic1"
    end

    test "returns error when topic does not exist" do
      cluster_metadata = build_cluster_metadata(topics: ["topic1"])
      response = Metadata.response(cluster_metadata)

      assert {:error, :no_such_topic} = Metadata.topic(response, "non-existent")
    end

    test "returns error when no topics in cluster" do
      cluster_metadata = build_cluster_metadata(topics: [])
      response = Metadata.response(cluster_metadata)

      assert {:error, :no_such_topic} = Metadata.topic(response, "any-topic")
    end
  end

  describe "topics/2" do
    test "returns multiple topics" do
      cluster_metadata = build_cluster_metadata(topics: ["topic1", "topic2", "topic3"])
      response = Metadata.response(cluster_metadata)

      topics = Metadata.topics(response, ["topic1", "topic3"])

      assert length(topics) == 2
      assert Enum.map(topics, & &1.name) |> Enum.sort() == ["topic1", "topic3"]
    end

    test "returns empty list for non-existent topics" do
      cluster_metadata = build_cluster_metadata(topics: ["topic1"])
      response = Metadata.response(cluster_metadata)

      topics = Metadata.topics(response, ["non-existent1", "non-existent2"])

      assert topics == []
    end

    test "returns only existing topics from mixed list" do
      cluster_metadata = build_cluster_metadata(topics: ["topic1", "topic2"])
      response = Metadata.response(cluster_metadata)

      topics = Metadata.topics(response, ["topic1", "non-existent", "topic2"])

      assert length(topics) == 2
      assert Enum.map(topics, & &1.name) |> Enum.sort() == ["topic1", "topic2"]
    end

    test "returns empty list when given empty list" do
      cluster_metadata = build_cluster_metadata(topics: ["topic1", "topic2"])
      response = Metadata.response(cluster_metadata)

      topics = Metadata.topics(response, [])

      assert topics == []
    end
  end

  describe "all_topics/1" do
    test "returns all topics from cluster" do
      cluster_metadata = build_cluster_metadata(topics: ["topic1", "topic2", "topic3"])
      response = Metadata.response(cluster_metadata)

      topics = Metadata.all_topics(response)

      assert length(topics) == 3
      assert Enum.all?(topics, fn topic -> match?(%Topic{}, topic) end)
      topic_names = Enum.map(topics, & &1.name) |> Enum.sort()
      assert topic_names == ["topic1", "topic2", "topic3"]
    end

    test "returns empty list when no topics" do
      cluster_metadata = build_cluster_metadata(topics: [])
      response = Metadata.response(cluster_metadata)

      assert Metadata.all_topics(response) == []
    end

    test "returns single topic" do
      cluster_metadata = build_cluster_metadata(topics: ["single-topic"])
      response = Metadata.response(cluster_metadata)

      topics = Metadata.all_topics(response)

      assert length(topics) == 1
      assert [%Topic{name: "single-topic"}] = topics
    end
  end

  # Helper functions

  defp build_cluster_metadata(opts \\ []) do
    broker_count = Keyword.get(opts, :broker_count, 3)
    controller_id = Keyword.get(opts, :controller_id, 1)
    topic_names = Keyword.get(opts, :topics, ["topic1", "topic2"])

    brokers =
      1..broker_count
      |> Enum.into(%{}, fn node_id ->
        {node_id,
         %Broker{
           node_id: node_id,
           host: "broker#{node_id}.example.com",
           port: 9092,
           socket: nil,
           rack: "rack#{node_id}"
         }}
      end)

    topics =
      Enum.into(topic_names, %{}, fn topic_name ->
        {topic_name,
         %Topic{
           name: topic_name,
           partition_leaders: %{0 => 1, 1 => 2, 2 => 3},
           is_internal: false,
           partitions: [
             %Partition{partition_id: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3]},
             %Partition{partition_id: 1, leader: 2, replicas: [2, 3, 1], isr: [2, 3, 1]},
             %Partition{partition_id: 2, leader: 3, replicas: [3, 1, 2], isr: [3, 1, 2]}
           ]
         }}
      end)

    %ClusterMetadata{
      brokers: brokers,
      controller_id: controller_id,
      topics: topics,
      consumer_group_coordinators: %{}
    }
  end
end
