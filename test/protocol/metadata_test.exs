defmodule Kafka.Protocol.Metadata.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid metadata request" do
    good_request = << 3 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, 0 :: 32 >>
    request = Kafka.Protocol.Metadata.create_request(1, "foo")
    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = << 0 :: 32, 1 :: 32, 0 :: 32, 3 :: 16, "foo" :: binary, 9092 :: 32, 1 :: 32, 0 :: 16, 3 :: 16, "bar" :: binary,
      1 :: 32, 0 :: 16, 0 :: 32, 0 :: 32, 0 :: 32, 1 :: 32, 0 :: 32 >>
    assert {:ok,
      %{:brokers => %{0 => %{:host => "foo", :port => 9092}},
        :topics => %{"bar" => [error_code: 0,
            partitions: %{0 => %{:error_code => 0, :isrs => [0], :leader => 0,
                :replicas => []}}]}}} = Kafka.Protocol.Metadata.parse_response(response)
  end

  test "parse_response returns an error parsing an invalid response" do
    response = << 0 :: 32, 1 :: 32, 0 :: 32, 3 :: 16, "foo" :: binary, 9092 :: 32, 1 :: 32, 0 :: 16, 3 :: 16, "bar" :: binary,
      1 :: 16, 0 :: 16, 0 :: 32, 0 :: 32, 0 :: 32, 1 :: 32, 0 :: 32 >>
    assert {:error, _, _} = Kafka.Protocol.Metadata.parse_response(response)
  end
end
