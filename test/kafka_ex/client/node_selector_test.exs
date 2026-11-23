defmodule KafkaEx.Client.NodeSelectorTest do
  use ExUnit.Case, async: true
  import KafkaEx.TestHelpers, only: [opaque: 1]

  alias KafkaEx.Client.NodeSelector

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
        NodeSelector.node_id(opaque("invalid data"))
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

  describe "connection_role/2" do
    @selectors [
      NodeSelector.node_id(1),
      NodeSelector.random(),
      NodeSelector.first_available(),
      NodeSelector.controller(),
      NodeSelector.topic_partition("t", 0),
      NodeSelector.topic_partition_replica("t", 0),
      NodeSelector.consumer_group("g")
    ]

    test "metadata, find_coordinator and api_versions go to :metadata whatever the selector" do
      for selector <- @selectors, op <- [:metadata, :find_coordinator, :api_versions] do
        assert NodeSelector.connection_role(selector, op) == :metadata
      end
    end

    test "a consumer group request goes to :coordinator" do
      for op <- [:join_group, :sync_group, :heartbeat, :offset_commit, :offset_fetch, :fetch] do
        assert NodeSelector.connection_role(NodeSelector.consumer_group("g"), op) == :coordinator
      end
    end

    test "a fetch by topic and partition goes to :fetch, one role per broker whatever the partition" do
      assert NodeSelector.connection_role(NodeSelector.topic_partition_replica("orders", 3), :fetch) == :fetch
      assert NodeSelector.connection_role(NodeSelector.topic_partition("orders", 0), :fetch) == :fetch
    end

    test "a fetch with no partition in its selector goes to :data" do
      for selector <- [NodeSelector.node_id(1), NodeSelector.random(), NodeSelector.controller()] do
        assert NodeSelector.connection_role(selector, :fetch) == :data
      end
    end

    test "everything else goes to :data" do
      assert NodeSelector.connection_role(NodeSelector.topic_partition("t", 0), :produce) == :data
      assert NodeSelector.connection_role(NodeSelector.topic_partition("t", 0), :list_offsets) == :data
      assert NodeSelector.connection_role(NodeSelector.controller(), :create_topics) == :data
      assert NodeSelector.connection_role(NodeSelector.node_id(2), :list_groups) == :data
    end
  end
end
