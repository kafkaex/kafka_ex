defmodule KafkaEx.Protocol.Metadata.Test do
  use ExUnit.Case, async: true

  test "create_request with no topics creates a valid metadata request" do
    good_request = << 3 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, 0 :: 32 >>
    request = KafkaEx.Protocol.Metadata.create_request(1, "foo", [])
    assert request == good_request
  end

  test "create_request with a single topic creates a valid metadata request" do
    good_request = << 3 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, 1 :: 32, 3 :: 16, "bar" :: binary >>
    request = KafkaEx.Protocol.Metadata.create_request(1, "foo", ["bar"])
    assert request == good_request
  end

  test "create_request with a multiple topics creates a valid metadata request" do
    good_request = << 3 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, 3 :: 32, 3 :: 16, "bar" :: binary, 3 :: 16, "baz" :: binary, 4 :: 16, "food" :: binary >>
    request = KafkaEx.Protocol.Metadata.create_request(1, "foo", ["bar", "baz", "food"])
    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = << 0 :: 32, 1 :: 32, 0 :: 32, 3 :: 16, "foo" :: binary, 9092 :: 32, 1 :: 32, 0 :: 16, 3 :: 16, "bar" :: binary,
      1 :: 32, 0 :: 16, 0 :: 32, 0 :: 32, 0 :: 32, 1 :: 32, 0 :: 32 >>
    assert %{:brokers => %{0 => {"foo", 9092}},
        :topics => %{"bar" => %{:error_code => :no_error,
            :partitions => %{0 => %{:error_code => :no_error, :isrs => [0], :leader => 0,
                :replicas => []}}}}} = KafkaEx.Protocol.Metadata.parse_response(response)
  end
end
