defmodule Kafka.Protocol.Produce.Test do
  use ExUnit.Case, async: true
  import Mock

  test "create_request creates a valid produce request" do
    message_set = Kafka.Util.create_message_set("value", "key")
    good_request = << 0 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, 0 :: 16, 100 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32,
      0 :: 32, byte_size(message_set) :: 32 >> <> message_set
    request = Kafka.Protocol.Produce.create_request(%{correlation_id: 1, client_id: "foo"}, "bar", 0, "value", "key", 0, 100)
    assert request == good_request
  end

  test "parse_response correctly parses a valid response with single topic and partition" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64 >>
    producer = %{:connection => %{:correlation_id => 1, :client_id => "foo"}, :broker => %{:host => "localhost", :port => 9092},
      :metadata => %{}, :topic => "test", :partition => 0}
    assert {:ok, %{"bar" => %{0 => %{:error_code => 0, :offset => 10}}}, producer} == Kafka.Protocol.Produce.parse_response(producer, response)
  end

  test "parse_response correctly parses a valid response with multiple topics and partitions" do
    response = << 0 :: 32, 2 :: 32, 3 :: 16, "bar" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 1 :: 32, 0 :: 16, 20 :: 64,
                                    3 :: 16, "baz" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 30 :: 64, 1 :: 32, 0 :: 16, 40 :: 64 >>
    producer = %{:connection => %{:correlation_id => 1, :client_id => "foo"}, :broker => %{:host => "localhost", :port => 9092},
      :metadata => %{}, :topic => "test", :partition => 0}
    assert {:ok, %{"bar" => %{0 => %{:error_code => 0, :offset => 10}, 1 => %{:error_code => 0, :offset => 20}},
                   "baz" => %{0 => %{:error_code => 0, :offset => 30}, 1 => %{:error_code => 0, :offset => 40}}}, producer} == Kafka.Protocol.Produce.parse_response(producer, response)
  end

  test "parse_response correctly parses an invalid response returning an error" do
    response = << 0 :: 32, 2 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64 >>
    producer = %{:connection => %{:correlation_id => 1, :client_id => "foo"}, :broker => %{:host => "localhost", :port => 9092},
      :metadata => %{}, :topic => "test", :partition => 0}
    assert {:error, "Error parsing topic or number of partitions in produce response", "", producer} == Kafka.Protocol.Produce.parse_response(producer, response)
  end
end
