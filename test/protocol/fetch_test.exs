defmodule KafkaEx.Protocol.Fetch.Test do
  use ExUnit.Case, async: true

  test "create_request creates a valid fetch request" do
    good_request = << 1 :: 16, 0 :: 16, 1 :: 32, 3 :: 16, "foo" :: binary, -1 :: 32, 10 :: 32, 1 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 1 :: 64, 10000 :: 32 >>
    request = KafkaEx.Protocol.Fetch.create_request(1, "foo", "bar", 0, 1, 10, 1, 10000)
    assert request == good_request
  end

  test "parse_response correctly parses a valid response with a key and a value" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 32 :: 32, 1 :: 64, 20 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, 3 :: 32, "foo" :: binary, 3 :: 32, "bar" :: binary >>
    expected_response = [%KafkaEx.Protocol.Fetch.Response{partitions: [%{error_code: 0, hw_mark_offset: 10, last_offset: 1, message_set: [%{attributes: 0, crc: 0, key: "foo", offset: 1, value: "bar"}], partition: 0}], topic: "bar"}]
    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a response with excess bytes" do
    response = <<0, 0, 0, 1, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 56, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 17, 254>>
    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{partitions: [
          %{error_code: 0, hw_mark_offset: 56, last_offset: 2, message_set: [
              %{attributes: 0, crc: 4264455069, key: nil, offset: 0, value: "hey"}, %{attributes: 0, crc: 4264455069, key: nil, offset: 1, value: "hey"},
              %{attributes: 0, crc: 4264455069, key: nil, offset: 2, value: "hey"}
            ],
          partition: 0}], topic: "food"}
    ]
    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with a nil key and a value" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      -1 :: 32, 3 :: 32, "bar" :: binary >>
    expected_response = [%KafkaEx.Protocol.Fetch.Response{partitions: [%{error_code: 0, hw_mark_offset: 10, last_offset: 1, message_set: [%{attributes: 0, crc: 0, key: nil, offset: 1, value: "bar"}], partition: 0}], topic: "bar"}]
    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with a key and a nil value" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      3 :: 32, "foo" :: binary, -1 :: 32 >>
    expected_response = [%KafkaEx.Protocol.Fetch.Response{partitions: [%{error_code: 0, hw_mark_offset: 10, last_offset: 1, message_set: [%{attributes: 0, crc: 0, key: "foo", offset: 1, value: nil}], partition: 0}], topic: "bar"}]
    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple messages" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 58 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      -1 :: 32, 3 :: 32, "bar" :: binary, 2 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, -1 :: 32, 3 :: 32, "baz" :: binary >>
   expected_response = [
     %KafkaEx.Protocol.Fetch.Response{partitions: [
         %{error_code: 0, hw_mark_offset: 10, last_offset: 2, message_set: [
             %{attributes: 0, crc: 0, key: nil, offset: 1, value: "bar"},
             %{attributes: 0, crc: 0, key: nil, offset: 2, value: "baz"}
           ], partition: 0}
     ], topic: "bar"}
    ]
    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple partitions" do
    response = << 0 :: 32, 1 :: 32, 3 :: 16, "bar" :: binary, 2 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8,
      -1 :: 32, 3 :: 32, "bar" :: binary, 1 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, -1 :: 32, 3 :: 32, "baz" :: binary >>
    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{partitions: [
          %{error_code: 0, hw_mark_offset: 10, last_offset: 1, message_set: [%{attributes: 0, crc: 0, key: nil, offset: 1, value: "baz"}], partition: 1},
          %{error_code: 0, hw_mark_offset: 10, last_offset: 1, message_set: [%{attributes: 0, crc: 0, key: nil, offset: 1, value: "bar"}], partition: 0}
        ], topic: "bar"}
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple topics" do
    response = << 0 :: 32, 2 :: 32,
      3 :: 16, "bar" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, -1 :: 32, 3 :: 32, "foo" :: binary,
      3 :: 16, "baz" :: binary, 1 :: 32, 0 :: 32, 0 :: 16, 10 :: 64, 29 :: 32, 1 :: 64, 17 :: 32, 0 :: 32, 0 :: 8, 0 :: 8, -1 :: 32, 3 :: 32, "bar" :: binary >>
    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{partitions:
        [
          %{error_code: 0, hw_mark_offset: 10, last_offset: 1, message_set: [%{attributes: 0, crc: 0, key: nil, offset: 1, value: "foo"}], partition: 0}
        ],
      topic: "bar"},
      %KafkaEx.Protocol.Fetch.Response{partitions:
        [
          %{error_code: 0, hw_mark_offset: 10, last_offset: 1, message_set: [%{attributes: 0, crc: 0, key: nil, offset: 1, value: "bar"}], partition: 0}
        ],
      topic: "baz"}
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with a snappy-encoded message" do
    response = <<0, 0, 0, 8, 0, 0, 0, 1, 0, 11, 115, 110, 97, 112, 112, 121, 95, 116, 101, 115, 116, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 83, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 71, 183, 227, 95, 48, 0, 2, 255, 255, 255, 255, 0, 0, 0, 57, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 37, 38, 0, 0, 9, 1, 120, 1, 0, 0, 0, 26, 166, 224, 205, 141, 0, 0, 255, 255, 255, 255, 0, 0, 0, 12, 116, 101, 115, 116, 32, 109, 101, 115, 115, 97, 103, 101>>
    value = "test message"
    message = %{attributes: 0, crc: 2799750541, key: nil, offset: 1,
               value: value}
    partition1 = %{error_code: 0,
                   hw_mark_offset: 2,
                   last_offset: 1,
                   partition: 1,
                   message_set: [message]}
    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{partitions: [partition1],
                                       topic: "snappy_test"}
    ]
    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

   test "parse_response correctly parses a valid response with batched snappy-encoded messages" do
    response = <<0, 0, 0, 14, 0, 0, 0, 1, 0, 17, 115, 110, 97, 112, 112, 121, 95, 98, 97, 116, 99, 104, 95, 116, 101, 115, 116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 93, 70, 199, 142, 116, 0, 2, 255, 255, 255, 255, 0, 0, 0, 79, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 59, 84, 0, 0, 25, 1, 16, 30, 204, 101, 110, 2, 5, 15, 76, 4, 107, 101, 121, 49, 0, 0, 0, 12, 98, 97, 116, 99, 104, 32, 116, 101, 115, 116, 32, 1, 16, 1, 1, 32, 1, 0, 0, 0, 30, 6, 246, 100, 60, 1, 13, 5, 42, 0, 50, 58, 42, 0, 0, 50>>
    message1 = %{attributes: 0, crc: 3429199362, key: "key1", offset: 0,
                 value: "batch test 1"}
    message2 = %{attributes: 0, crc: 116810812, key: "key2", offset: 1,
                 value: "batch test 2"}
    partition1 = %{error_code: 0,
                   hw_mark_offset: 2,
                   last_offset: 1,
                   partition: 0,
                   message_set: [message1, message2]}
    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{partitions: [partition1],
                                       topic: "snappy_batch_test"}
    ]
    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end
end
