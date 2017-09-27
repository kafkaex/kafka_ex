defmodule KafkaEx.GenConsumerTest do
  use ExUnit.Case

  # non-integration GenConsumer tests

  defmodule TestConsumer do
    use KafkaEx.GenConsumer

    def handle_message_set(_, state), do: state
  end

  test "calling handle_call raises an error if there is no implementation" do
    assert_raise RuntimeError, fn -> TestConsumer.handle_call(nil, nil, nil) end
  end
end
