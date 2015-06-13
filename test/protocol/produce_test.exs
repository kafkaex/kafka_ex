defmodule KafkaEx.Protocol.Produce.Test do
  use ExUnit.Case, async: true

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
