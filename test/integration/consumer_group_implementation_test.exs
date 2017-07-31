defmodule KafkaEx.ConsumerGroupImplementationTest do
  use ExUnit.Case

  alias KafkaEx.ConsumerGroup
  import TestHelper

  require Logger

  @moduletag :consumer_group

  # note this topic is created by docker_up.sh
  @topic_name "consumer_group_implementation_test"
  @partition_count 4
  @consumer_group_name "consumer_group_implementation"

  defmodule TestObserver do
    def start_link do
      Agent.start_link(fn -> %{} end, name: __MODULE__)
    end

    def on_handled_message_set(message_set, topic, partition) do
      Agent.update(
        __MODULE__,
        fn(state) ->
          key = {topic, partition}
          Logger.debug("FUCK #{inspect message_set}")
          Map.update(state, key, [message_set], &(&1 ++ [message_set]))
        end
      )
    end

    def last_handled_message_set(topic, partition) do
      Agent.get(
        __MODULE__,
        fn(state) ->
          key = {topic, partition}
          message_sets_handled = Map.get(state, key, [])
          Logger.debug("XXX #{inspect message_sets_handled}")
          List.last(message_sets_handled)
        end
      )
    end

    def on_assign_partitions(topic, members, partitions) do
      Agent.update(
        __MODULE__,
        fn(state) ->
          key = {:assigns, topic}
          value = {members, partitions}
          Map.update(state, key, [value], &(&1 ++ [value]))
        end
      )
    end

    def get do
      Agent.get(__MODULE__, &(&1))
    end
  end

  defmodule TestConsumer do
    use KafkaEx.GenConsumer

    alias KafkaEx.ConsumerGroupImplementationTest.TestObserver

    def init(topic, partition) do
      {:ok, %{topic: topic, partition: partition}}
    end

    def handle_message_set(message_set, state) do
      TestObserver.on_handled_message_set(message_set, state.topic, state.partition)
      {:async_commit, state}
    end

    def assign_partitions(members, partitions) do
      # TODO this function should get the state as part of its call and be
      # allowed to mutate the state
      topic_name = KafkaEx.ConsumerGroupImplementationTest.topic_name
      TestObserver.on_assign_partitions(topic_name, members, partitions)
      super(members, partitions)
    end
  end

  def produce(message, partition) do
    KafkaEx.produce(@topic_name, partition, message)
  end

  def topic_name do
    @topic_name
  end

  setup do
    {:ok, _} = TestObserver.start_link
    {:ok, _} = ConsumerGroup.start_link(TestConsumer, @consumer_group_name, [@topic_name])

    :ok
  end

  test "basic startup and consume test" do
    starting_offset = latest_offset_number(@topic_name, 0)

    produce("OHAI", 0)

    wait_for(fn ->
      state = TestObserver.get()
      length(Map.get(state, {:assigns, @topic_name}, [])) > 0
    end)

    # the assign_partitions callback should have been called with all 4
    # partitions
    assigns = Map.get(TestObserver.get(), {:assigns, @topic_name}, [])
    [{[_consumer_id], partitions}] = assigns
    assert @partition_count == length(partitions)
    for ix <- 0..(@partition_count - 1) do
      assert {@topic_name, ix} in partitions
    end

    wait_for(fn ->
      message_set = TestObserver.last_handled_message_set(@topic_name, 0)
      message_set && Map.get(List.last(message_set), :offset) >= starting_offset
    end)
  end
end
