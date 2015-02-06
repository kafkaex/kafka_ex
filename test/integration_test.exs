defmodule Kafka.Integration.Test do
  use ExUnit.Case
  @moduletag :integration

  defmodule TestHandler do
    use GenEvent
    def handle_event(event, parent) do
      IO.inspect event
      {:ok, parent}
    end
  end

  test "consumer fetches messages from Kafka" do
    # {:ok, producer} = Kafka.Producer.new([["localhost", 9092]], "test_producer", "test", 0)
    # {:ok, producer} = Kafka.Producer.produce(producer, "message", "key")

    {:ok, consumer} = Kafka.Consumer.new([["localhost", 9092]], "foo")
    Kafka.Consumer.subscribe(consumer, TestHandler, "test", 0, 0)
    :timer.sleep(2000)
  end
end
