defmodule KafkaEx.TestSupport.TestGenConsumer do
  @moduledoc """
  A test GenConsumer implementation that collects messages and notifies test processes.

  Used for integration testing of consumer groups.

  ## Usage

      # Start consumer group with test process notification
      ConsumerGroup.start_link(
        TestGenConsumer,
        "my-group",
        ["my-topic"],
        extra_consumer_args: %{test_pid: self()}
      )

      # Receive message notifications
      receive do
        {:messages_received, count} -> IO.puts("Got \#{count} messages")
      end

  ## State queries

  You can query the consumer state via GenServer.call:

      GenServer.call(consumer_pid, :get_messages)  # Returns list of message values
      GenServer.call(consumer_pid, :get_count)     # Returns total message count
  """

  use KafkaEx.Consumer.GenConsumer

  alias KafkaEx.Messages.Fetch.Record

  defmodule State do
    @moduledoc false
    defstruct messages: [], message_count: 0, test_pid: nil
  end

  @impl true
  def init(_topic, _partition) do
    {:ok, %State{}}
  end

  @impl true
  def init(_topic, _partition, extra_args) do
    test_pid = Map.get(extra_args || %{}, :test_pid)
    {:ok, %State{test_pid: test_pid}}
  end

  @impl true
  def handle_message_set(message_set, state) do
    new_messages = Enum.map(message_set, fn %Record{value: value} -> value end)

    new_state = %State{
      state
      | messages: state.messages ++ new_messages,
        message_count: state.message_count + length(message_set)
    }

    # Notify test process if provided
    if state.test_pid do
      send(state.test_pid, {:messages_received, length(message_set)})
    end

    {:async_commit, new_state}
  end

  @impl true
  def handle_call(:get_messages, _from, state) do
    {:reply, state.messages, state}
  end

  def handle_call(:get_count, _from, state) do
    {:reply, state.message_count, state}
  end
end
