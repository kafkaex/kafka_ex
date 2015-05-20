defmodule KafkaEx.Protocol.OffsetFetch.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid offset commit message" do
    corr_id = 3
    client_id = "kafka_ex"
    offset_commit_request = %KafkaEx.Protocol.OffsetCommit.Request{topic: "foo", consumer_group: "bar"}
    good_request = << 9 :: 16, 0 :: 16, 3 :: 32, 8 :: 16, "kafka_ex" :: binary, 3 :: 16, "bar" :: binary, 1 :: 32, 3 :: 16, "foo" :: binary, 1 :: 32, 0 :: 32 >>
    request = KafkaEx.Protocol.OffsetFetch.create_request(corr_id, client_id, offset_commit_request)
    assert request == good_request
  end

  test "parse_response correctly parses a valid response" do
    response = <<0, 0, 156, 66, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0>>
    assert KafkaEx.Protocol.OffsetFetch.parse_response(response) == [%KafkaEx.Protocol.OffsetFetch.Response{partitions: [%{metadata: "", offset: 9, partition: 0}], topic: "food"}]
  end
end
