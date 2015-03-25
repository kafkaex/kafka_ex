defmodule KafkaEx.Protocol.Metadata.Test do
  use ExUnit.Case, async: true

  test "create_request_fn with no topics creates a valid metadata request" do
    good_request = << 3 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, 0 :: 32 >>
    request_fn = KafkaEx.Protocol.Metadata.create_request_fn([])
    request = request_fn.(1, "foo")
    assert request == good_request
  end

  test "create_request_fn with a single topic creates a valid metadata request" do
    good_request = << 3 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, 1 :: 32, 3 :: 16, "bar" :: binary >>
    request_fn = KafkaEx.Protocol.Metadata.create_request_fn(["bar"])
    request = request_fn.(1, "foo")
    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = << 0 :: 32, 1 :: 32, 0 :: 32, 3 :: 16, "foo" :: binary, 9092 :: 32, 1 :: 32, 0 :: 16, 3 :: 16, "bar" :: binary,
      1 :: 32, 0 :: 16, 0 :: 32, 0 :: 32, 0 :: 32, 1 :: 32, 0 :: 32 >>
    assert %{:brokers => %{0 => {"foo", 9092}},
        :topics => %{"bar" => %{:error_code => 0,
            :partitions => %{0 => %{:error_code => 0, :isrs => [0], :leader => 0,
                :replicas => []}}}}} = KafkaEx.Protocol.Metadata.parse_response(response)
  end
end
