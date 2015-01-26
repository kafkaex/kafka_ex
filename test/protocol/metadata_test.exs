defmodule Kafka.Protocol.Metadata.Test do
  use ExUnit.Case, async: true
  import Mock

  test "create_request creates a valid metadata request" do
    good_request = << 3 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, 0 :: 32 >>
    request = Kafka.Protocol.Metadata.create_request(%{:correlation_id => 1, :client_id => "foo"})
    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = << 0 :: 32, 1 :: 32, 0 :: 32, 3 :: 16, "foo" :: binary, 9092 :: 32, 1 :: 32, 0 :: 16, 3 :: 16, "bar" :: binary,
      1 :: 32, 0 :: 16, 0 :: 32, 0 :: 32, 0 :: 32, 1 :: 32, 0 :: 32 >>
    assert {:ok,
      %{:brokers => %{0 => %{:host => "foo", :port => 9092, :socket => nil}},
        :connection => %{:client_id => "foo", :correlation_id => 1}, :timestamp => _,
        :topics => %{"bar" => [error_code: 0,
            partitions: %{0 => %{:error_code => 0, :isrs => [0], :leader => 0,
                :replicas => []}}]}}} = Kafka.Protocol.Metadata.parse_response(%{:correlation_id => 1, :client_id => "foo"}, response)
  end

  test "parse_response returns an error parsing an invalid response" do
    response = << 0 :: 32, 1 :: 32, 0 :: 32, 3 :: 16, "foo" :: binary, 9092 :: 32, 1 :: 32, 0 :: 16, 3 :: 16, "bar" :: binary,
      1 :: 16, 0 :: 16, 0 :: 32, 0 :: 32, 0 :: 32, 1 :: 32, 0 :: 32 >>
    assert {:error, _, _, %{:correlation_id => 1, :client_id => "foo"}} = Kafka.Protocol.Metadata.parse_response(%{:correlation_id => 1, :client_id => "foo"}, response)
  end
end
