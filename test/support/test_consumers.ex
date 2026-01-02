defmodule KafkaEx.TestConsumers do
  @moduledoc """
  Shared test consumers for integration tests.

  These consumers send messages to a test process and support crash testing.
  """

  defmodule AsyncTestConsumer do
    @moduledoc """
    Test consumer with async commit.
    Crashes when receiving "CRASH" message.
    Sends `{:messages_received, messages}` to test_pid.
    Sends `{:partition_messages, partition, messages}` when include_partition: true.
    Sends `{:consumer_partition, consumer_id, partition, messages}` when consumer_id is set.
    """
    use KafkaEx.Consumer.GenConsumer

    def init(_topic, _partition), do: {:ok, %{messages: [], test_pid: nil, partition: nil}}

    def init(_topic, partition, opts) do
      test_pid = Keyword.get(opts, :test_pid)
      include_partition = Keyword.get(opts, :include_partition, false)
      consumer_id = Keyword.get(opts, :consumer_id)

      {:ok,
       %{
         messages: [],
         test_pid: test_pid,
         partition: partition,
         include_partition: include_partition,
         consumer_id: consumer_id
       }}
    end

    def handle_message_set(message_set, state) do
      %{
        test_pid: test_pid,
        partition: partition,
        include_partition: include_partition,
        consumer_id: consumer_id
      } = state

      messages =
        Enum.map(message_set, fn message ->
          if message.value == "CRASH", do: raise("Intentional crash")
          message.value
        end)

      if test_pid do
        cond do
          consumer_id != nil ->
            send(test_pid, {:consumer_partition, consumer_id, partition, messages})

          include_partition ->
            send(test_pid, {:partition_messages, partition, messages})

          true ->
            send(test_pid, {:messages_received, messages})
        end
      end

      {:async_commit, %{state | messages: state.messages ++ messages}}
    end
  end

  defmodule SyncTestConsumer do
    @moduledoc """
    Test consumer with sync commit.
    Crashes when receiving "CRASH" message.
    Sends `{:messages_received, messages}` to test_pid.
    Sends `{:partition_messages, partition, messages}` when include_partition: true.
    """
    use KafkaEx.Consumer.GenConsumer

    def init(_topic, _partition), do: {:ok, %{messages: [], test_pid: nil, partition: nil}}

    def init(_topic, partition, opts) do
      test_pid = Keyword.get(opts, :test_pid)
      include_partition = Keyword.get(opts, :include_partition, false)
      {:ok, %{messages: [], test_pid: test_pid, partition: partition, include_partition: include_partition}}
    end

    def handle_message_set(
          message_set,
          %{test_pid: test_pid, partition: partition, include_partition: include_partition} = state
        ) do
      messages =
        Enum.map(message_set, fn message ->
          if message.value == "CRASH", do: raise("Intentional crash")
          message.value
        end)

      if test_pid do
        if include_partition do
          send(test_pid, {:partition_messages, partition, messages})
        else
          send(test_pid, {:messages_received, messages})
        end
      end

      {:sync_commit, %{state | messages: state.messages ++ messages}}
    end
  end
end
