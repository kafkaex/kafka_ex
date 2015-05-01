defmodule KafkaEx.Protocol.OffsetCommit.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid offset commit message" do
    corr_id = 3
    client_id = "kafka_ex"
    offset = 10
    topic = "foo"
    consumer_group = "bar"
    metadata = "baz"
    good_request = << 8 :: 16, 0 :: 16, corr_id :: 32, byte_size(client_id) :: 16, client_id :: binary, 3 :: 16, consumer_group :: binary, 1 :: 32, 3 :: 16, topic :: binary, 1 :: 32, 0 :: 32, offset :: 64, 3 :: 16, metadata :: binary >>
    request = KafkaEx.Protocol.OffsetCommit.create_request(corr_id, client_id, consumer_group, topic, 0, offset, metadata)
    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = <<0, 0, 156, 64, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0>>
    assert KafkaEx.Protocol.OffsetCommit.parse_response(response) == [%{partitions: [0], topic: "food"}]
  end
end
