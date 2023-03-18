defmodule KafkaEx.New.NodeSelectorTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.NodeSelector

  describe "node_id/1" do
    test "build selector based on node id strategy" do
      selector = NodeSelector.node_id(123)

      assert selector.strategy == :node_id
      assert selector.node_id == 123
      refute selector.topic
      refute selector.partition
      refute selector.consumer_group_name
    end

    test "raises error when node id is  invalid" do
      assert_raise FunctionClauseError, fn ->
        NodeSelector.node_id("invalid data")
      end
    end
  end

  describe "random/0" do
    test "returns selector with random strategy" do
      selector = NodeSelector.random()

      assert selector.strategy == :random
      refute selector.node_id
      refute selector.topic
      refute selector.partition
      refute selector.consumer_group_name
    end
  end

  describe "first_available/0" do
    test "returns selector with first_available strategy" do
      selector = NodeSelector.first_available()

      assert selector.strategy == :first_available
      refute selector.node_id
      refute selector.topic
      refute selector.partition
      refute selector.consumer_group_name
    end
  end

  describe "controller/0" do
    test "returns selector with controller strategy" do
      selector = NodeSelector.controller()

      assert selector.strategy == :controller
      refute selector.node_id
      refute selector.topic
      refute selector.partition
      refute selector.consumer_group_name
    end
  end

  describe "topic_partition/2" do
    test "returns selector with topic_partition strategy" do
      selector = NodeSelector.topic_partition("topic-name", 123)

      assert selector.strategy == :topic_partition
      refute selector.node_id
      assert selector.topic == "topic-name"
      assert selector.partition == 123
      refute selector.consumer_group_name
    end
  end

  describe "consumer_group/1" do
    test "returns selector with topic_partition strategy" do
      selector = NodeSelector.consumer_group("consumer-group-name")

      assert selector.strategy == :consumer_group
      refute selector.node_id
      refute selector.topic
      refute selector.partition
      refute selector.consumer_group_name == "consumer_group-name"
    end
  end
end
