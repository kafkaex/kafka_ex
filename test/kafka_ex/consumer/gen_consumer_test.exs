defmodule KafkaEx.Consumer.GenConsumerTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  # non-integration GenConsumer tests

  defmodule TestConsumer do
    use KafkaEx.Consumer.GenConsumer

    def handle_message_set(_, state), do: state
  end

  test "calling handle_call raises an error if there is no implementation" do
    assert_raise RuntimeError, fn -> TestConsumer.handle_call(nil, nil, nil) end
  end

  test "calling handle_cast raises an error if there is no implementation" do
    assert_raise RuntimeError, fn -> TestConsumer.handle_cast(nil, nil) end
  end

  test "calling handle_info raises an error if there is no implementation" do
    assert capture_log(fn ->
             TestConsumer.handle_info(nil, nil)
           end) =~ "unexpected message in handle_info"
  end
end
