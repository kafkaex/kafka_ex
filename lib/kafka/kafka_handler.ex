defmodule KafkaHandler do
  use GenEvent

  def handle_event(message_set, message_sets) do
    {:ok, [message_set|message_sets]}
  end

  def handle_call(:messages, message_sets) do
    {:ok, Enum.reverse(message_sets), []}
  end
end
