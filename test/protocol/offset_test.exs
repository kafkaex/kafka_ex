defmodule KafkaEx.Protocol.Offset.Test do
  use ExUnit.Case, async: true

  test "parse_response correctly parses a valid response with an offset" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 1 :: 32, 10 :: 64 >>
    expected_response = [%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: '\n', partition: 0}], topic: "bar"}]

    assert expected_response == KafkaEx.Protocol.Offset.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple offsets" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 2 :: 32, 10 :: 64, 20 :: 64 >>
    expected_response = [%KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: [10, 20], partition: 0}], topic: "bar"}]

    assert expected_response == KafkaEx.Protocol.Offset.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple partitions" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 1 :: 32, 10 :: 64, 1 :: 32, 0 :: 16, 1 :: 32, 20 :: 64 >>
    expected_response = [%KafkaEx.Protocol.Offset.Response{partition_offsets: [
          %{error_code: 0, offset: [20], partition: 1},
          %{error_code: 0, offset: '\n', partition: 0}
    ], topic: "bar"}]

    assert expected_response == KafkaEx.Protocol.Offset.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple topics" do
    response = << 0 :: 32, 2 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 1 :: 32, 10 :: 64, 3 :: 16, "baz" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 1 :: 32, 20 :: 64 >>
    expected_response = [
      %KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: [10], partition: 0}], topic: "bar"},
      %KafkaEx.Protocol.Offset.Response{partition_offsets: [%{error_code: 0, offset: [20], partition: 0}], topic: "baz"}
    ]

    assert expected_response == KafkaEx.Protocol.Offset.parse_response(response)
  end
end
