defmodule KafkaEx.Protocol.OffsetCommit.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid offset commit message" do
    corr_id = 3
    client_id = "kafka_ex"

    offset_commit_request = %KafkaEx.Protocol.OffsetCommit.Request{
      offset: 10,
      partition: 0,
      topic: "foo",
      consumer_group: "bar",
      metadata: "baz"
    }

    good_request =
      <<8::16, 0::16, corr_id::32, byte_size(client_id)::16, client_id::binary, 3::16, "bar", 1::32, 3::16, "foo",
        1::32, 0::32, 10::64, 3::16, "baz">>

    request =
      KafkaEx.Protocol.OffsetCommit.create_request(
        corr_id,
        client_id,
        offset_commit_request
      )

    assert :erlang.iolist_to_binary(request) == good_request
  end

  test "parse_response correctly parses a valid response" do
    response =
      <<0, 0, 156, 64, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0>>

    assert KafkaEx.Protocol.OffsetCommit.parse_response(response) == [
             %KafkaEx.Protocol.OffsetCommit.Response{
               partitions: [0],
               topic: "food"
             }
           ]
  end
end
