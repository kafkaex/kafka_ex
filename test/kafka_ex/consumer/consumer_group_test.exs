defmodule KafkaEx.Consumer.ConsumerGroupTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Consumer.ConsumerGroup

  describe "init/1" do
    defmodule TestConsumer do
      @moduledoc false
      # Minimal test consumer module
    end

    test "returns supervisor init with Manager child" do
      init_arg = {{KafkaEx.Consumer.GenConsumer, TestConsumer}, "test-group", ["test-topic"], []}
      {:ok, {sup_flags, children}} = ConsumerGroup.init(init_arg)

      assert sup_flags.strategy == :one_for_all
      assert sup_flags.intensity == 0
      assert sup_flags.period == 1

      assert length(children) == 1
      [manager_child] = children
      assert manager_child.id == KafkaEx.Consumer.ConsumerGroup.Manager
    end

    test "passes supervisor_pid to Manager options" do
      init_arg =
        {{KafkaEx.Consumer.GenConsumer, TestConsumer}, "test-group", ["test-topic"], [heartbeat_interval: 1000]}

      {:ok, {_sup_flags, [manager_child]}} = ConsumerGroup.init(init_arg)

      {_module, :start_link, [init_args]} = manager_child.start
      {{_gen_mod, _consumer_mod}, _group, _topics, opts} = init_args

      assert Keyword.has_key?(opts, :supervisor_pid)
      assert Keyword.get(opts, :heartbeat_interval) == 1000
    end

    test "preserves consumer module tuple format" do
      init_arg = {{KafkaEx.Consumer.GenConsumer, TestConsumer}, "my-group", ["topic1", "topic2"], []}
      {:ok, {_sup_flags, [manager_child]}} = ConsumerGroup.init(init_arg)

      {_module, :start_link, [init_args]} = manager_child.start
      {{gen_mod, consumer_mod}, group, topics, _opts} = init_args

      assert gen_mod == KafkaEx.Consumer.GenConsumer
      assert consumer_mod == TestConsumer
      assert group == "my-group"
      assert topics == ["topic1", "topic2"]
    end
  end
end
