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
    defmodule Event do
      defstruct type: nil, from: nil, key: nil, payload: nil

      def type?(%Event{type: type}, type), do: true
      def type?(%Event{}, _type), do: false

      def key?(%Event{key: key}, key), do: true
      def key?(%Event{}, _key), do: false
    end

    def start_link do
      Agent.start_link(fn -> [] end, name: __MODULE__)
    end

    def event(event = %Event{}) do
      event = %{event | from: self()}
      Agent.update(__MODULE__, fn(events) -> events ++ [event] end)
    end

    def all_events() do
      Agent.get(__MODULE__, &(&1))
    end

    def by_type(events, type) do
      Enum.filter(events, &Event.type?(&1, type))
    end

    def by_key(events, key) do
      Enum.filter(events, &Event.key?(&1, key))
    end

    def payloads(events) do
      Enum.map(events, &(&1.payload))
    end

    def on_assign_partitions(topic, members, partitions) do
      event(
        %Event{
          type: :assign_partitions,
          key: topic,
          payload: {members, partitions}
        }
      )
    end

    def on_handled_message_set(message_set, topic, partition) do
      event(
        %Event{
          type: :handled_message_set,
          key: {topic, partition},
          payload: message_set
        }
      )
    end

    def last_handled_message_set(topic, partition) do
      all_events()
      |> by_type(:handled_message_set)
      |> by_key({topic, partition})
      |> payloads()
      |> List.last
    end

    def get_assigns(topic) do
      all_events()
      |> by_type(:assign_partitions)
      |> by_key(topic)
      |> payloads()
    end
  end

  defmodule TestConsumer do
    use KafkaEx.GenConsumer

    alias KafkaEx.ConsumerGroupImplementationTest.TestObserver

    def init(topic, partition) do
      Logger.debug(fn ->
        "Initialized consumer #{inspect self()} for #{topic}:#{partition}"
      end)
      {:ok, %{topic: topic, partition: partition}}
    end

    def handle_message_set(message_set, state) do
      Logger.debug(fn ->
        "Consumer #{inspect self()} handled message set #{inspect message_set}"
      end)
      TestObserver.on_handled_message_set(message_set, state.topic, state.partition)
      {:async_commit, state}
    end

    def assign_partitions(members, partitions) do
      Logger.debug(fn ->
        "Consumer #{inspect self()} got " <>
          "partition assignment: #{inspect members} #{inspect partitions}"
      end)
      # TODO this function should get the state as part of its call and be
      # allowed to mutate the state
      topic_name = KafkaEx.ConsumerGroupImplementationTest.topic_name
      TestObserver.on_assign_partitions(topic_name, members, partitions)
      super(members, partitions)
    end
  end

  def produce(message, partition) do
    KafkaEx.produce(@topic_name, partition, message)
    message
  end

  def right_last_message?(nil, _, _), do: false
  def right_last_message?([], _, _), do: false
  def right_last_message?(message_set, expected_message, expected_offset) do
    Logger.debug(fn ->
      "Got message set: #{inspect message_set} " <>
        "expecting '#{expected_message}' @ offset #{expected_offset}"
    end)
    message = List.last(message_set)
    message.value == expected_message && message.offset == expected_offset
  end

  def topic_name do
    @topic_name
  end

  def sync_stop(pid) when is_pid(pid) do
    wait_for(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end
      !Process.alive?(pid)
    end)
  end

  setup do
    {:ok, _} = TestObserver.start_link
    {:ok, consumer_group_pid1} = ConsumerGroup.start_link(
      TestConsumer,
      @consumer_group_name,
      [@topic_name],
      heartbeat_interval: 100
    )
    {:ok, consumer_group_pid2} = ConsumerGroup.start_link(
      TestConsumer,
      @consumer_group_name,
      [@topic_name],
      heartbeat_interval: 100
    )

    on_exit fn ->
      sync_stop(consumer_group_pid1)
      sync_stop(consumer_group_pid2)
    end

    {
      :ok,
      consumer_group_pid1: consumer_group_pid1,
      consumer_group_pid2: consumer_group_pid2
    }
  end

  test "basic startup, consume, and shutdown test", context do
    partition_range = 0..(@partition_count - 1)

    # wait for both consumer groups to join
    wait_for(fn ->
      assigns = TestObserver.get_assigns(@topic_name) || []
      length(assigns) > 0 && length(elem(List.last(assigns), 0)) == 2
    end)

    # the assign_partitions callback should have been called with all 4
    # partitions
    assigns = TestObserver.get_assigns(@topic_name)
    assert length(assigns) == 2
    last_assigns = List.last(assigns)
    # we should have two consumers in the most recent batch
    {[_consumer1_id, _consumer2_id], partitions} = last_assigns
    assert @partition_count == length(partitions)
    for ix <- 0..(@partition_count - 1) do
      assert {@topic_name, ix} in partitions
    end

    starting_offsets = partition_range
    |> Enum.map(fn(px) -> {px, latest_offset_number(@topic_name, px)} end)
    |> Enum.into(%{})

    messages = partition_range
    |> Enum.map(fn(px) ->
      offset = Map.get(starting_offsets, px)
      {px, produce("M #{px} #{offset}", px)}
    end)
    |> Enum.into(%{})

    # we actually consume the messages
    for px <- partition_range do
      wait_for(fn ->
        message_set = TestObserver.last_handled_message_set(@topic_name, px)
        right_last_message?(message_set, messages[px], starting_offsets[px])
      end)
    end

    # stop the supervisors
    Process.unlink(context[:consumer_group_pid1])
    sync_stop(context[:consumer_group_pid1])
    Process.unlink(context[:consumer_group_pid2])
    sync_stop(context[:consumer_group_pid2])

    # offsets should be committed on exit
    for px <- partition_range do
      wait_for(fn ->
        ending_offset =
          latest_consumer_offset_number(@topic_name, px, @consumer_group_name)
          message_set = TestObserver.last_handled_message_set(@topic_name, px)
          last_message = List.last(message_set)
          ending_offset == last_message.offset + 1
      end)
    end
  end
end
