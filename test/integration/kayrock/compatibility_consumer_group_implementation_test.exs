defmodule KafkaEx.KayrockCompatibilityConsumerGroupImplementationTest do
  @moduledoc """
  These are tests using the original KafkaEx API with the kayrock server

  These come from consumer_group_implementation test.exs. Note that even
  though Kayrock does not support this version of Kafka, the original
  implementations often delegate to previous versions. So unless the test is
  testing functionality that doesn't make sense in newer versions of kafka,
  we will test it here.

  """

  use ExUnit.Case

  @moduletag :new_client

  alias KafkaEx.ConsumerGroup
  alias KafkaEx.GenConsumer

  # note this topic is created by docker_up.sh
  @topic_name "consumer_group_implementation_test"
  @partition_count 4
  @consumer_group_name "consumer_group_implementation"

  require Logger

  defmodule TestPartitioner do
    # wraps an Agent that we use to capture the fact that the partitioner was
    # called - normally one would not really need to do this

    alias KafkaEx.ConsumerGroup.PartitionAssignment

    def start_link do
      Agent.start_link(fn -> 0 end, name: __MODULE__)
    end

    def calls do
      Agent.get(__MODULE__, & &1)
    end

    def assign_partitions(members, partitions) do
      Logger.debug(fn ->
        "Consumer #{inspect(self())} got " <>
          "partition assignment: #{inspect(members)} #{inspect(partitions)}"
      end)

      Agent.update(__MODULE__, &(&1 + 1))

      PartitionAssignment.round_robin(members, partitions)
    end
  end

  defmodule TestConsumer do
    # test consumer - keeps track of messages handled

    use KafkaEx.GenConsumer

    alias KafkaEx.GenConsumer

    def last_message_set(pid) do
      List.last(GenConsumer.call(pid, :message_sets)) || []
    end

    def get(pid, key) do
      GenConsumer.call(pid, {:get, key})
    end

    def set(pid, key, value) do
      GenConsumer.cast(pid, {:set, key, value})
    end

    def init(topic, partition) do
      Logger.debug(fn ->
        "Initialized consumer #{inspect(self())} for #{topic}:#{partition}"
      end)

      {:ok, %{message_sets: []}}
    end

    def handle_call(:message_sets, _from, state) do
      {:reply, state.message_sets, state}
    end

    def handle_call({:get, key}, _from, state) do
      {:reply, Map.get(state, key), state}
    end

    def handle_cast({:set, key, value}, state) do
      {:noreply, Map.put_new(state, key, value)}
    end

    def handle_info({:set, key, value}, state) do
      {:noreply, Map.put_new(state, key, value)}
    end

    def handle_message_set(message_set, state) do
      Logger.debug(fn ->
        "Consumer #{inspect(self())} handled message set #{inspect(message_set)}"
      end)

      {
        :async_commit,
        %{state | message_sets: state.message_sets ++ [message_set]}
      }
    end
  end

  def produce(message, partition) do
    KafkaEx.produce(@topic_name, partition, message)
    message
  end

  def correct_last_message?(nil, _, _), do: false
  def correct_last_message?([], _, _), do: false

  def correct_last_message?(message_set, expected_message, expected_offset) do
    Logger.debug(fn ->
      "Got message set: #{inspect(message_set)} " <>
        "expecting '#{expected_message}' @ offset #{expected_offset}"
    end)

    message = List.last(message_set)
    message.value == expected_message && message.offset == expected_offset
  end

  def sync_stop(pid) when is_pid(pid) do
    TestHelper.wait_for(fn ->
      if Process.alive?(pid) do
        Process.exit(pid, :normal)
      end

      !Process.alive?(pid)
    end)
  end

  def num_open_ports() do
    :erlang.ports()
    |> Enum.map(&:erlang.port_info(&1, :name))
    |> Enum.filter(&(&1 == {:name, 'tcp_inet'}))
    |> length
  end

  setup_all do
    kafka_version = Application.get_env(:kafka_ex, :kafka_version)

    Application.put_env(:kafka_ex, :kafka_version, "kayrock")

    :ok = Application.stop(:kafka_ex)
    {:ok, _} = Application.ensure_all_started(:kafka_ex)

    on_exit(fn ->
      Application.put_env(:kafka_ex, :kafka_version, kafka_version)

      :ok = Application.stop(:kafka_ex)
      {:ok, _} = Application.ensure_all_started(:kafka_ex)
    end)
  end

  setup do
    ports_before = num_open_ports()
    {:ok, _} = TestPartitioner.start_link()

    {:ok, consumer_group_pid1} =
      ConsumerGroup.start_link(
        TestConsumer,
        @consumer_group_name,
        [@topic_name],
        heartbeat_interval: 100,
        partition_assignment_callback: &TestPartitioner.assign_partitions/2,
        session_timeout_padding: 30000
      )

    {:ok, consumer_group_pid2} =
      ConsumerGroup.start_link(
        TestConsumer,
        @consumer_group_name,
        [@topic_name],
        heartbeat_interval: 100,
        partition_assignment_callback: &TestPartitioner.assign_partitions/2,
        session_timeout_padding: 30000
      )

    # wait for both consumer groups to join
    TestHelper.wait_for(fn ->
      ConsumerGroup.active?(consumer_group_pid1, 30000) &&
        ConsumerGroup.active?(consumer_group_pid2, 30000)
    end)

    on_exit(fn ->
      sync_stop(consumer_group_pid1)
      sync_stop(consumer_group_pid2)
    end)

    {
      :ok,
      consumer_group_pid1: consumer_group_pid1,
      consumer_group_pid2: consumer_group_pid2,
      ports_before: ports_before
    }
  end

  test "basic startup, consume, and shutdown test", context do
    assert num_open_ports() > context[:ports_before]

    assert TestPartitioner.calls() > 0

    generation_id1 = ConsumerGroup.generation_id(context[:consumer_group_pid1])
    generation_id2 = ConsumerGroup.generation_id(context[:consumer_group_pid2])
    assert generation_id1 == generation_id2

    assert @consumer_group_name ==
             ConsumerGroup.group_name(context[:consumer_group_pid1])

    member1 = ConsumerGroup.member_id(context[:consumer_group_pid1])
    member2 = ConsumerGroup.member_id(context[:consumer_group_pid2])
    assert member1 != member2

    leader1 = ConsumerGroup.leader_id(context[:consumer_group_pid1])
    leader2 = ConsumerGroup.leader_id(context[:consumer_group_pid2])
    assert leader1 == leader2

    cond do
      leader1 == member1 ->
        assert ConsumerGroup.leader?(context[:consumer_group_pid1])
        refute ConsumerGroup.leader?(context[:consumer_group_pid2])

      leader1 == member2 ->
        refute ConsumerGroup.leader?(context[:consumer_group_pid1])
        assert ConsumerGroup.leader?(context[:consumer_group_pid2])

      true ->
        raise "Neither member is the leader"
    end

    assignments1 = ConsumerGroup.assignments(context[:consumer_group_pid1])
    assignments2 = ConsumerGroup.assignments(context[:consumer_group_pid2])
    assert 2 == length(assignments1)
    assert 2 == length(assignments2)

    assert MapSet.disjoint?(
             Enum.into(assignments1, MapSet.new()),
             Enum.into(assignments2, MapSet.new())
           )

    consumer1_pid =
      ConsumerGroup.consumer_supervisor_pid(context[:consumer_group_pid1])

    consumer1_assignments =
      consumer1_pid
      |> GenConsumer.Supervisor.child_pids()
      |> Enum.map(&GenConsumer.partition/1)
      |> Enum.sort()

    assert consumer1_assignments == Enum.sort(assignments1)

    consumer2_pid =
      ConsumerGroup.consumer_supervisor_pid(context[:consumer_group_pid2])

    consumer2_assignments =
      consumer2_pid
      |> GenConsumer.Supervisor.child_pids()
      |> Enum.map(&GenConsumer.partition/1)
      |> Enum.sort()

    assert consumer2_assignments == Enum.sort(assignments2)

    # all of the partitions should be accounted for
    assert @partition_count == length(Enum.uniq(assignments1 ++ assignments2))

    partition_range = 0..(@partition_count - 1)

    starting_offsets =
      partition_range
      |> Enum.map(fn px ->
        {px, TestHelper.latest_offset_number(@topic_name, px)}
      end)
      |> Enum.into(%{})

    messages =
      partition_range
      |> Enum.map(fn px ->
        offset = Map.get(starting_offsets, px)
        {px, produce("M #{px} #{offset}", px)}
      end)
      |> Enum.into(%{})

    consumers =
      Map.merge(
        ConsumerGroup.partition_consumer_map(context[:consumer_group_pid1]),
        ConsumerGroup.partition_consumer_map(context[:consumer_group_pid2])
      )

    # we actually consume the messages
    last_offsets =
      partition_range
      |> Enum.map(fn px ->
        consumer_pid = Map.get(consumers, {@topic_name, px})

        TestHelper.wait_for(fn ->
          message_set = TestConsumer.last_message_set(consumer_pid)
          correct_last_message?(message_set, messages[px], starting_offsets[px])
        end)

        last_message = List.last(TestConsumer.last_message_set(consumer_pid))
        {px, last_message.offset}
      end)
      |> Enum.into(%{})

    # stop the supervisors
    Process.unlink(context[:consumer_group_pid1])
    sync_stop(context[:consumer_group_pid1])
    Process.unlink(context[:consumer_group_pid2])
    sync_stop(context[:consumer_group_pid2])

    # offsets should be committed on exit
    for px <- partition_range do
      TestHelper.wait_for(fn ->
        ending_offset =
          TestHelper.latest_consumer_offset_number(
            @topic_name,
            px,
            @consumer_group_name
          )

        last_offset = Map.get(last_offsets, px)
        ending_offset == last_offset + 1
      end)
    end

    # ports should be released
    assert context[:ports_before] == num_open_ports()
  end
end
