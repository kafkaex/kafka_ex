defmodule KafkaEx.Handler do
  @moduledoc """
  Default GenEvent handler for KafkaEx.stream

  Recieved message sets are accumulated in the GenEvent state and can
  be retrieved and flushed via the `:messages` call.
  """

  use GenEvent

  def handle_event(message_set, message_sets) do
    {:ok, [message_set|message_sets]}
  end

  def handle_call(:messages, message_sets) do
    {:ok, Enum.reverse(message_sets), []}
  end
end
