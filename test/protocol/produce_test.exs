defmodule KafkaEx.Protocol.Produce.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid payload" do
    expected_request = <<0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 102, 111, 111, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 106, 86, 37, 142, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121>>

    request = KafkaEx.Protocol.Produce.create_request(1, "foo", %KafkaEx.Protocol.Produce.Request{
      topic: "food", partition: 0, required_acks: 1, timeout: 10, compression: :none, messages: [
        %KafkaEx.Protocol.Produce.Message{key: "", value: "hey"},
      ]
    })

    assert expected_request == request
  end

  test "create_request correctly batches multiple request messages" do
    expected_request = <<0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 102, 111, 111, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 88, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 106, 86, 37, 142, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 225, 27, 42, 82, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 104, 105, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 119, 44, 195, 207, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 104, 101, 108, 108, 111>>

    request = KafkaEx.Protocol.Produce.create_request(1, "foo", %KafkaEx.Protocol.Produce.Request{
      topic: "food", partition: 0, required_acks: 1, timeout: 10, compression: :none, messages: [
        %KafkaEx.Protocol.Produce.Message{key: "", value: "hey"},
        %KafkaEx.Protocol.Produce.Message{key: "", value: "hi"},
        %KafkaEx.Protocol.Produce.Message{key: "", value: "hello"},
      ]
    })

    assert expected_request == request
  end

  test "parse_response correctly parses a valid response with single topic and partition" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64 >>
    expected_response = [%KafkaEx.Protocol.Produce.Response{partitions: [%{error_code: 0, offset: 10, partition: 0}], topic: "bar"}]
    assert expected_response == KafkaEx.Protocol.Produce.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple topics and partitions" do
    response = << 0 :: 32, 2 :: 32, 3 :: 16, "bar" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 1 :: 32, 0 :: 16, 20 :: 64, 3 :: 16, "baz" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 30 :: 64, 1 :: 32, 0 :: 16, 40 :: 64 >>
    expected_response = [%KafkaEx.Protocol.Produce.Response{partitions: [
        %{error_code: 0, offset: 20, partition: 1},
        %{error_code: 0, offset: 10, partition: 0}
      ], topic: "bar"},
      %KafkaEx.Protocol.Produce.Response{partitions: [
        %{error_code: 0, offset: 40, partition: 1},
        %{error_code: 0, offset: 30, partition: 0}
      ], topic: "baz"}]

    assert expected_response == KafkaEx.Protocol.Produce.parse_response(response)
  end
end
