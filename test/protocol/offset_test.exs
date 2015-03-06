defmodule KafkaEx.Protocol.Offset.Test do
  use ExUnit.Case, async: true

  test "parse_response correctly parses a valid response with an offset" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 1 :: 32, 10 :: 64 >>
    assert {:ok,
      %{"bar" => %{0 => %{:error_code => 0, :offsets => [10]}}}}
    = KafkaEx.Protocol.Offset.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple offsets" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 2 :: 32, 10 :: 64, 20 :: 64 >>
    assert {:ok,
      %{"bar" => %{0 => %{:error_code => 0, :offsets => [10, 20]}}}}
    = KafkaEx.Protocol.Offset.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple partitions" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 1 :: 32, 10 :: 64, 1 :: 32, 0 :: 16, 1 :: 32, 20 :: 64 >>
    assert {:ok,
      %{"bar" => %{0 => %{:error_code => 0, :offsets => [10]}, 1 => %{:error_code => 0, :offsets => [20]}}}}
    = KafkaEx.Protocol.Offset.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple topics" do
    response = << 0 :: 32, 2 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 1 :: 32, 10 :: 64, 3 :: 16, "baz" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 1 :: 32, 20 :: 64 >>
    assert {:ok,
      %{"bar" => %{0 => %{:error_code => 0, :offsets => [10]}},
        "baz" => %{0 => %{:error_code => 0, :offsets => [20]}}}}
    = KafkaEx.Protocol.Offset.parse_response(response)
  end
end
