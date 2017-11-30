defmodule KafkaEx.Protocol.Produce.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid payload" do
    expected_request = <<0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 102, 111, 111, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 106, 86, 37, 142, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121>>

    request = KafkaEx.Protocol.Produce.create_request(1, "foo", %KafkaEx.Protocol.Produce.Request{
      topic: "food", partition: 0, required_acks: 1, timeout: 10, compression: :none, messages: [
        %KafkaEx.Protocol.Produce.Message{key: "", value: "hey"},
      ]
    })

    assert expected_request == request
  end

  test "create_request correctly batches multiple request messages" do
    expected_request = <<0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 102, 111, 111, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 88, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 106, 86, 37, 142, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 225, 27, 42, 82, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 104, 105, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 19, 119, 44, 195, 207, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 104, 101, 108, 108, 111>>

    request = KafkaEx.Protocol.Produce.create_request(1, "foo", %KafkaEx.Protocol.Produce.Request{
      topic: "food", partition: 0, required_acks: 1, timeout: 10, compression: :none, messages: [
        %KafkaEx.Protocol.Produce.Message{key: "", value: "hey"},
        %KafkaEx.Protocol.Produce.Message{key: "", value: "hi"},
        %KafkaEx.Protocol.Produce.Message{key: "", value: "hello"},
      ]
    })

    assert expected_request == request
  end

  test "create_request correctly encodes messages with gzip" do
    expected_request = <<0, 0, 0, 0, 0, 0, 0, 1, 0, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 95, 99, 108, 105, 101, 110, 116, 95, 116, 101, 115, 116, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0, 16, 99, 111, 109, 112, 114, 101, 115, 115, 101, 100, 95, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 86, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 74, 79, 44, 46, 209, 0, 1, 255, 255, 255, 255, 0, 0, 0, 60, 31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 99, 96, 128, 3, 153, 135, 115, 4, 255, 131, 89, 172, 217, 169, 149, 10, 137, 64, 6, 103, 110, 106, 113, 113, 98, 122, 42, 152, 3, 87, 199, 242, 37, 117, 30, 66, 93, 18, 178, 186, 36, 0, 127, 205, 212, 97, 80, 0, 0, 0>>

    client_id = "compression_client_test"
    topic = "compressed_topic"
    messages = [
      %KafkaEx.Protocol.Produce.Message{key: "key a", value: "message a"},
      %KafkaEx.Protocol.Produce.Message{key: "key b", value: "message b"}
    ]

    produce = %KafkaEx.Protocol.Produce.Request{
      topic: topic,
      partition: 0,
      required_acks: 1,
      timeout: 10,
      compression: :gzip,
      messages: messages
    }

    request = KafkaEx.Protocol.Produce.create_request(1, client_id, produce)

    # The exact binary contents of the message can change as zlib changes,
    # but they should remain compatible.  We test this by splitting the binary
    # up into the parts that should be the same and the parts that may differ -
    # which are the crc checkshum and the compressed part of the message.
    #
    # the byte sizes here are determined by looking at the construction of the
    # messages and headers in produce.ex
    pre_crc_header_size = 46 + byte_size(topic) + byte_size(client_id)
    crc_size = 4
    # note this includes the size of the compressed part of the binary, which
    # should be the same.
    post_crc_header_size = 10

    << pre_crc_header :: binary-size(pre_crc_header_size),
      _crc :: binary-size(crc_size),
      post_crc_header :: binary-size(post_crc_header_size),
      compressed_message_set :: binary >> = request

    << expect_pre_crc_header :: binary-size(pre_crc_header_size),
      _expect_crc :: binary-size(crc_size),
      expect_post_crc_header :: binary-size(post_crc_header_size),
      expect_compressed_message_set :: binary >> = expected_request

    assert pre_crc_header == expect_pre_crc_header
    assert post_crc_header == expect_post_crc_header

    decompressed_message_set = :zlib.gunzip(compressed_message_set)
    expect_decompressed_message_set =
      :zlib.gunzip(expect_compressed_message_set)

    assert decompressed_message_set == expect_decompressed_message_set
  end

  test "create_request correctly encodes messages with snappy" do
    expected_request = <<0, 0, 0, 0, 0, 0, 0, 1, 0, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 95, 99, 108, 105, 101, 110, 116, 95, 116, 101, 115, 116, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0, 16, 99, 111, 109, 112, 114, 101, 115, 115, 101, 100, 95, 116, 111, 112, 105, 99, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 86, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 74, 67, 64, 33, 66, 0, 2, 255, 255, 255, 255, 0, 0, 0, 60, 80, 0, 0, 25, 1, 16, 28, 225, 156, 17, 255, 5, 15, 64, 5, 107, 101, 121, 32, 97, 0, 0, 0, 9, 109, 101, 115, 115, 97, 103, 101, 5, 13, 17, 1, 16, 28, 4, 244, 101, 158, 5, 13, 5, 40, 52, 98, 0, 0, 0, 9, 109, 101, 115, 115, 97, 103, 101, 32, 98>> 

    produce = %KafkaEx.Protocol.Produce.Request{
      topic: "compressed_topic",
      partition: 0,
      required_acks: 1,
      timeout: 10,
      compression: :snappy,
      messages: [
        %KafkaEx.Protocol.Produce.Message{key: "key a", value: "message a"},
        %KafkaEx.Protocol.Produce.Message{key: "key b", value: "message b"}
      ]
    }

    request = KafkaEx.Protocol.Produce.create_request(1,
                                                      "compression_client_test",
                                                      produce)

    assert expected_request == request
  end

  test "parse_response correctly parses a valid response with single topic and partition" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64 >>
    expected_response = [%KafkaEx.Protocol.Produce.Response{partitions: [%{error_code: :no_error, offset: 10, partition: 0}], topic: "bar"}]
    assert expected_response == KafkaEx.Protocol.Produce.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple topics and partitions" do
    response = << 0 :: 32, 2 :: 32, 3 :: 16, "bar" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 1 :: 32, 0 :: 16, 20 :: 64, 3 :: 16, "baz" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 30 :: 64, 1 :: 32, 0 :: 16, 40 :: 64 >>
    expected_response = [%KafkaEx.Protocol.Produce.Response{partitions: [
        %{error_code: :no_error, offset: 20, partition: 1},
        %{error_code: :no_error, offset: 10, partition: 0}
      ], topic: "bar"},
      %KafkaEx.Protocol.Produce.Response{partitions: [
        %{error_code: :no_error, offset: 40, partition: 1},
        %{error_code: :no_error, offset: 30, partition: 0}
      ], topic: "baz"}]

    assert expected_response == KafkaEx.Protocol.Produce.parse_response(response)
  end
end
