defmodule KafkaEx.Protocol.Fetch.Test do
  use ExUnit.Case, async: true
  alias KafkaEx.Protocol.Fetch.Message

  test "create_request creates a valid fetch request" do
    good_request =
      <<1::16, 0::16, 1::32, 3::16, "foo"::binary, -1::32, 10::32, 1::32, 1::32,
        3::16, "bar"::binary, 1::32, 0::32, 1::64, 10000::32>>

    fetch_request = %KafkaEx.Protocol.Fetch.Request{
      correlation_id: 1,
      client_id: "foo",
      topic: "bar",
      partition: 0,
      offset: 1,
      wait_time: 10,
      min_bytes: 1,
      max_bytes: 10_000
    }

    request = KafkaEx.Protocol.Fetch.create_request(fetch_request)
    assert request == good_request
  end

  test "parse_response correctly parses a valid response with a key and a value" do
    topic = "baz"
    key = "foo"
    value = "bar"
    partition = 0

    response =
      <<0::32, 1::32, 3::16, topic::binary, 1::32, 0::32, 0::16, 10::64, 32::32,
        1::64, 20::32, 0::32, 0::8, 0::8, 3::32, key::binary, 3::32,
        value::binary>>

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [
          %{
            error_code: :no_error,
            hw_mark_offset: 10,
            last_offset: 1,
            message_set: [
              %Message{
                attributes: 0,
                crc: 0,
                key: key,
                offset: 1,
                value: value,
                topic: topic,
                partition: partition
              }
            ],
            partition: partition
          }
        ],
        topic: topic
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a response with excess bytes" do
    response =
      <<0, 0, 0, 1, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 56, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3,
        104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 17, 254, 46, 107, 157,
        0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0,
        0, 2, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0,
        3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 17, 254>>

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [
          %{
            error_code: :no_error,
            hw_mark_offset: 56,
            last_offset: 2,
            message_set: [
              %Message{
                attributes: 0,
                crc: 4_264_455_069,
                key: nil,
                offset: 0,
                value: "hey",
                topic: "food",
                partition: 0
              },
              %Message{
                attributes: 0,
                crc: 4_264_455_069,
                key: nil,
                offset: 1,
                value: "hey",
                topic: "food",
                partition: 0
              },
              %Message{
                attributes: 0,
                crc: 4_264_455_069,
                key: nil,
                offset: 2,
                value: "hey",
                topic: "food",
                partition: 0
              }
            ],
            partition: 0
          }
        ],
        topic: "food"
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with a nil key and a value" do
    topic = "foo"
    value = "bar"

    response =
      <<0::32, 1::32, 3::16, topic::binary, 1::32, 0::32, 0::16, 10::64, 29::32,
        1::64, 17::32, 0::32, 0::8, 0::8, -1::32, 3::32, value::binary>>

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [
          %{
            error_code: :no_error,
            hw_mark_offset: 10,
            last_offset: 1,
            message_set: [
              %Message{
                attributes: 0,
                crc: 0,
                key: nil,
                offset: 1,
                value: value,
                topic: topic,
                partition: 0
              }
            ],
            partition: 0
          }
        ],
        topic: topic
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with a key and a nil value" do
    topic = "bar"
    key = "foo"

    response =
      <<0::32, 1::32, 3::16, topic::binary, 1::32, 0::32, 0::16, 10::64, 29::32,
        1::64, 17::32, 0::32, 0::8, 0::8, 3::32, key::binary, -1::32>>

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [
          %{
            error_code: :no_error,
            hw_mark_offset: 10,
            last_offset: 1,
            message_set: [
              %Message{
                attributes: 0,
                crc: 0,
                key: key,
                offset: 1,
                value: nil,
                topic: topic,
                partition: 0
              }
            ],
            partition: 0
          }
        ],
        topic: topic
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple messages" do
    response =
      <<0::32, 1::32, 3::16, "foo"::binary, 1::32, 0::32, 0::16, 10::64, 58::32,
        1::64, 17::32, 0::32, 0::8, 0::8, -1::32, 3::32, "bar"::binary, 2::64,
        17::32, 0::32, 0::8, 0::8, -1::32, 3::32, "baz"::binary>>

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [
          %{
            error_code: :no_error,
            hw_mark_offset: 10,
            last_offset: 2,
            message_set: [
              %Message{
                attributes: 0,
                crc: 0,
                key: nil,
                offset: 1,
                value: "bar",
                topic: "foo",
                partition: 0
              },
              %Message{
                attributes: 0,
                crc: 0,
                key: nil,
                offset: 2,
                value: "baz",
                topic: "foo",
                partition: 0
              }
            ],
            partition: 0
          }
        ],
        topic: "foo"
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple partitions" do
    topic = "foo"

    response =
      <<0::32, 1::32, 3::16, topic::binary, 2::32, 0::32, 0::16, 10::64, 29::32,
        1::64, 17::32, 0::32, 0::8, 0::8, -1::32, 3::32, "bar"::binary, 1::32,
        0::16, 10::64, 29::32, 1::64, 17::32, 0::32, 0::8, 0::8, -1::32, 3::32,
        "baz"::binary>>

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [
          %{
            error_code: :no_error,
            hw_mark_offset: 10,
            last_offset: 1,
            message_set: [
              %Message{
                attributes: 0,
                crc: 0,
                key: nil,
                offset: 1,
                value: "baz",
                topic: topic,
                partition: 1
              }
            ],
            partition: 1
          },
          %{
            error_code: :no_error,
            hw_mark_offset: 10,
            last_offset: 1,
            message_set: [
              %Message{
                attributes: 0,
                crc: 0,
                key: nil,
                offset: 1,
                value: "bar",
                topic: topic,
                partition: 0
              }
            ],
            partition: 0
          }
        ],
        topic: topic
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with multiple topics" do
    response =
      <<0::32, 2::32, 3::16, "bar"::binary, 1::32, 0::32, 0::16, 10::64, 29::32,
        1::64, 17::32, 0::32, 0::8, 0::8, -1::32, 3::32, "foo"::binary, 3::16,
        "baz"::binary, 1::32, 0::32, 0::16, 10::64, 29::32, 1::64, 17::32,
        0::32, 0::8, 0::8, -1::32, 3::32, "bar"::binary>>

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [
          %{
            error_code: :no_error,
            hw_mark_offset: 10,
            last_offset: 1,
            message_set: [
              %Message{
                attributes: 0,
                crc: 0,
                key: nil,
                offset: 1,
                value: "foo",
                topic: "bar",
                partition: 0
              }
            ],
            partition: 0
          }
        ],
        topic: "bar"
      },
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [
          %{
            error_code: :no_error,
            hw_mark_offset: 10,
            last_offset: 1,
            message_set: [
              %Message{
                attributes: 0,
                crc: 0,
                key: nil,
                offset: 1,
                value: "bar",
                topic: "baz",
                partition: 0
              }
            ],
            partition: 0
          }
        ],
        topic: "baz"
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with a gzip-encoded message" do
    response =
      <<0, 0, 0, 4, 0, 0, 0, 1, 0, 9, 103, 122, 105, 112, 95, 116, 101, 115,
        116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 74,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62, 38, 244, 178, 37, 0, 1, 255, 255,
        255, 255, 0, 0, 0, 48, 31, 139, 8, 0, 0, 0, 0, 0, 0, 0, 99, 96, 128, 3,
        169, 101, 15, 206, 246, 50, 48, 252, 7, 2, 32, 143, 167, 36, 181, 184,
        68, 33, 55, 181, 184, 56, 49, 61, 21, 0, 10, 31, 112, 82, 38, 0, 0, 0>>

    topic = "gzip_test"

    message = %Message{
      attributes: 0,
      crc: 2_799_750_541,
      key: nil,
      offset: 0,
      value: "test message",
      topic: topic,
      partition: 0
    }

    partition1 = %{
      error_code: :no_error,
      hw_mark_offset: 1,
      last_offset: 0,
      partition: 0,
      message_set: [message]
    }

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [partition1],
        topic: topic
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with batched gzip-encoded messages" do
    response =
      <<0, 0, 0, 3, 0, 0, 0, 1, 0, 15, 103, 122, 105, 112, 95, 98, 97, 116, 99,
        104, 95, 116, 101, 115, 116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 4, 0, 0, 0, 180, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 74, 112, 213,
        163, 157, 0, 1, 255, 255, 255, 255, 0, 0, 0, 60, 31, 139, 8, 0, 0, 0, 0,
        0, 0, 0, 99, 96, 128, 3, 169, 119, 54, 19, 103, 51, 48, 252, 7, 2, 32,
        143, 39, 41, 177, 36, 57, 67, 161, 36, 181, 184, 68, 193, 16, 170, 130,
        17, 164, 170, 220, 244, 128, 34, 86, 85, 70, 0, 83, 29, 3, 53, 76, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 82, 59, 149, 134, 225, 0, 1, 255,
        255, 255, 255, 0, 0, 0, 68, 31, 139, 8, 0, 0, 0, 0, 0, 0, 0, 99, 96, 0,
        3, 38, 32, 150, 59, 147, 154, 199, 4, 230, 177, 100, 167, 86, 26, 2,
        105, 158, 164, 196, 146, 228, 12, 133, 146, 212, 226, 18, 5, 67, 136,
        66, 6, 102, 144, 74, 182, 111, 41, 54, 112, 149, 70, 104, 42, 141, 0,
        135, 95, 114, 164, 84, 0, 0, 0>>

    topic = "gzip_batch_test"
    partition_id = 0

    message1 = %Message{
      attributes: 0,
      crc: 3_996_946_843,
      key: nil,
      offset: 0,
      value: "batch test 1",
      topic: topic,
      partition: partition_id
    }

    message2 = %Message{
      attributes: 0,
      crc: 2_000_011_297,
      key: nil,
      offset: 1,
      value: "batch test 2",
      topic: topic,
      partition: partition_id
    }

    message3 = %Message{
      attributes: 0,
      crc: 3_429_199_362,
      key: "key1",
      offset: 2,
      value: "batch test 1",
      topic: topic,
      partition: partition_id
    }

    message4 = %Message{
      attributes: 0,
      crc: 116_810_812,
      key: "key2",
      offset: 3,
      value: "batch test 2",
      topic: topic,
      partition: partition_id
    }

    partition1 = %{
      error_code: :no_error,
      hw_mark_offset: 4,
      last_offset: 3,
      partition: partition_id,
      message_set: [message1, message2, message3, message4]
    }

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [partition1],
        topic: topic
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with a snappy-encoded message" do
    response =
      <<0, 0, 0, 8, 0, 0, 0, 1, 0, 11, 115, 110, 97, 112, 112, 121, 95, 116,
        101, 115, 116, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0,
        0, 0, 83, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 71, 183, 227, 95, 48, 0, 2,
        255, 255, 255, 255, 0, 0, 0, 57, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0,
        0, 1, 0, 0, 0, 1, 0, 0, 0, 37, 38, 0, 0, 9, 1, 120, 1, 0, 0, 0, 26, 166,
        224, 205, 141, 0, 0, 255, 255, 255, 255, 0, 0, 0, 12, 116, 101, 115,
        116, 32, 109, 101, 115, 115, 97, 103, 101>>

    value = "test message"

    topic = "snappy_test"

    message = %Message{
      attributes: 0,
      crc: 2_799_750_541,
      key: nil,
      offset: 1,
      value: value,
      topic: topic,
      partition: 1
    }

    partition1 = %{
      error_code: :no_error,
      hw_mark_offset: 2,
      last_offset: 1,
      partition: 1,
      message_set: [message]
    }

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [partition1],
        topic: topic
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end

  test "parse_response correctly parses a valid response with batched snappy-encoded messages" do
    partition_id = 0

    response =
      <<0, 0, 0, 14, 0, 0, 0, 1, 0, 17, 115, 110, 97, 112, 112, 121, 95, 98, 97,
        116, 99, 104, 95, 116, 101, 115, 116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 105, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 93,
        70, 199, 142, 116, 0, 2, 255, 255, 255, 255, 0, 0, 0, 79, 130, 83, 78,
        65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 59, 84, 0, 0, 25, 1,
        16, 30, 204, 101, 110, 2, 5, 15, 76, 4, 107, 101, 121, 49, 0, 0, 0, 12,
        98, 97, 116, 99, 104, 32, 116, 101, 115, 116, 32, 1, 16, 1, 1, 32, 1, 0,
        0, 0, 30, 6, 246, 100, 60, 1, 13, 5, 42, 0, 50, 58, 42, 0, 0, 50>>

    topic = "snappy_batch_test"

    message1 = %Message{
      attributes: 0,
      crc: 3_429_199_362,
      key: "key1",
      offset: 0,
      value: "batch test 1",
      topic: topic,
      partition: partition_id
    }

    message2 = %Message{
      attributes: 0,
      crc: 116_810_812,
      key: "key2",
      offset: 1,
      value: "batch test 2",
      topic: topic,
      partition: partition_id
    }

    partition1 = %{
      error_code: :no_error,
      hw_mark_offset: 2,
      last_offset: 1,
      partition: partition_id,
      message_set: [message1, message2]
    }

    expected_response = [
      %KafkaEx.Protocol.Fetch.Response{
        partitions: [partition1],
        topic: topic
      }
    ]

    assert expected_response == KafkaEx.Protocol.Fetch.parse_response(response)
  end
end
