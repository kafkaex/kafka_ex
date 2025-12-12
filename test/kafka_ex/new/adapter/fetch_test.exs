defmodule KafkaEx.New.AdapterFetchTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.Fetch.Request, as: FetchRequest
  alias KafkaEx.Protocol.Fetch.Response, as: FetchResponse
  alias KafkaEx.Protocol.Fetch.Message, as: FetchMessage
  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  describe "Adapter.fetch_request/1" do
    test "builds V0 request with basic fields" do
      request = %FetchRequest{
        topic: "test-topic",
        partition: 0,
        offset: 100,
        wait_time: 10_000,
        min_bytes: 1,
        max_bytes: 1_000_000,
        api_version: 0
      }

      {kayrock_request, topic, partition} = Adapter.fetch_request(request)

      assert topic == "test-topic"
      assert partition == 0
      assert kayrock_request.replica_id == -1
      assert kayrock_request.max_wait_time == 10_000
      assert kayrock_request.min_bytes == 1
      assert [%{topic: "test-topic", partitions: [part]}] = kayrock_request.topics
      assert part.partition == 0
      assert part.fetch_offset == 100
      assert part.max_bytes == 1_000_000
    end

    test "builds V1 request (same as V0)" do
      request = %FetchRequest{
        topic: "v1-topic",
        partition: 1,
        offset: 50,
        wait_time: 5_000,
        min_bytes: 100,
        max_bytes: 500_000,
        api_version: 1
      }

      {kayrock_request, topic, partition} = Adapter.fetch_request(request)

      assert topic == "v1-topic"
      assert partition == 1
      assert kayrock_request.max_wait_time == 5_000
      assert kayrock_request.min_bytes == 100
      assert [%{partitions: [part]}] = kayrock_request.topics
      assert part.fetch_offset == 50
      assert part.max_bytes == 500_000
    end

    test "builds V2 request (same as V1)" do
      request = %FetchRequest{
        topic: "v2-topic",
        partition: 2,
        offset: 200,
        wait_time: 15_000,
        min_bytes: 50,
        max_bytes: 2_000_000,
        api_version: 2
      }

      {kayrock_request, _topic, _partition} = Adapter.fetch_request(request)

      assert kayrock_request.__struct__ == Kayrock.Fetch.V2.Request
      assert kayrock_request.max_wait_time == 15_000
    end

    test "builds V3 request with max_bytes at request level" do
      request = %FetchRequest{
        topic: "v3-topic",
        partition: 0,
        offset: 0,
        wait_time: 10_000,
        min_bytes: 1,
        max_bytes: 1_500_000,
        api_version: 3
      }

      {kayrock_request, _topic, _partition} = Adapter.fetch_request(request)

      assert kayrock_request.__struct__ == Kayrock.Fetch.V3.Request
      assert kayrock_request.max_bytes == 1_500_000
      assert [%{partitions: [part]}] = kayrock_request.topics
      assert part.max_bytes == 1_500_000
    end

    test "builds V4 request with isolation_level" do
      request = %FetchRequest{
        topic: "v4-topic",
        partition: 0,
        offset: 0,
        wait_time: 10_000,
        min_bytes: 1,
        max_bytes: 1_000_000,
        api_version: 4
      }

      {kayrock_request, _topic, _partition} = Adapter.fetch_request(request)

      assert kayrock_request.__struct__ == Kayrock.Fetch.V4.Request
      assert kayrock_request.isolation_level == 0
      assert kayrock_request.max_bytes == 1_000_000
    end

    test "builds V5 request with log_start_offset" do
      request = %FetchRequest{
        topic: "v5-topic",
        partition: 0,
        offset: 500,
        wait_time: 10_000,
        min_bytes: 1,
        max_bytes: 1_000_000,
        api_version: 5
      }

      {kayrock_request, _topic, _partition} = Adapter.fetch_request(request)

      assert kayrock_request.__struct__ == Kayrock.Fetch.V5.Request
      assert kayrock_request.isolation_level == 0
      assert [%{partitions: [part]}] = kayrock_request.topics
      assert part.log_start_offset == 0
      assert part.fetch_offset == 500
    end

    test "builds V6 request (same as V5)" do
      request = %FetchRequest{
        topic: "v6-topic",
        partition: 0,
        offset: 0,
        wait_time: 10_000,
        min_bytes: 1,
        max_bytes: 1_000_000,
        api_version: 6
      }

      {kayrock_request, _topic, _partition} = Adapter.fetch_request(request)

      assert kayrock_request.__struct__ == Kayrock.Fetch.V6.Request
      assert [%{partitions: [part]}] = kayrock_request.topics
      assert part.log_start_offset == 0
    end

    test "builds V7 request with session fields" do
      request = %FetchRequest{
        topic: "v7-topic",
        partition: 0,
        offset: 0,
        wait_time: 10_000,
        min_bytes: 1,
        max_bytes: 1_000_000,
        api_version: 7
      }

      {kayrock_request, _topic, _partition} = Adapter.fetch_request(request)

      assert kayrock_request.__struct__ == Kayrock.Fetch.V7.Request
      assert [%{partitions: [part]}] = kayrock_request.topics
      assert part.log_start_offset == 0
    end

    test "handles offset 0 (earliest)" do
      request = %FetchRequest{
        topic: "earliest-topic",
        partition: 0,
        offset: 0,
        wait_time: 10_000,
        min_bytes: 1,
        max_bytes: 1_000_000,
        api_version: 0
      }

      {kayrock_request, _topic, _partition} = Adapter.fetch_request(request)

      assert [%{partitions: [part]}] = kayrock_request.topics
      assert part.fetch_offset == 0
    end

    test "handles large offset values" do
      request = %FetchRequest{
        topic: "large-offset-topic",
        partition: 0,
        offset: 9_999_999_999,
        wait_time: 10_000,
        min_bytes: 1,
        max_bytes: 1_000_000,
        api_version: 0
      }

      {kayrock_request, _topic, _partition} = Adapter.fetch_request(request)

      assert [%{partitions: [part]}] = kayrock_request.topics
      assert part.fetch_offset == 9_999_999_999
    end
  end

  describe "Adapter.fetch_response/1" do
    test "parses response with MessageSet (V0-V3)" do
      response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 0,
                  high_watermark: 100
                },
                record_set: %MessageSet{
                  messages: [
                    %Message{
                      offset: 50,
                      key: "key1",
                      value: "value1",
                      timestamp: 1_234_567_890,
                      attributes: 0,
                      crc: 12345
                    },
                    %Message{
                      offset: 51,
                      key: "key2",
                      value: "value2",
                      timestamp: 1_234_567_891,
                      attributes: 0,
                      crc: 12346
                    }
                  ]
                }
              }
            ]
          }
        ]
      }

      {[fetch_response], last_offset} = Adapter.fetch_response(response)

      assert %FetchResponse{} = fetch_response
      assert fetch_response.topic == "test-topic"
      assert [partition_data] = fetch_response.partitions
      assert partition_data.partition == 0
      assert partition_data.error_code == :no_error
      assert partition_data.hw_mark_offset == 100
      assert length(partition_data.message_set) == 2
      assert last_offset == 51

      [msg1, msg2] = partition_data.message_set
      assert %FetchMessage{} = msg1
      assert msg1.offset == 50
      assert msg1.key == "key1"
      assert msg1.value == "value1"
      assert msg1.topic == "test-topic"
      assert msg1.partition == 0
      assert msg1.crc == 12345

      assert msg2.offset == 51
    end

    test "parses response with RecordBatch (V4+)" do
      response = %{
        responses: [
          %{
            topic: "v4-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 1,
                  error_code: 0,
                  high_watermark: 200
                },
                record_set: [
                  %RecordBatch{
                    records: [
                      %Record{
                        offset: 100,
                        key: "event-key",
                        value: "event-data",
                        timestamp: 1_702_000_000_000,
                        attributes: 0,
                        headers: [
                          %RecordHeader{key: "content-type", value: "application/json"},
                          %RecordHeader{key: "trace-id", value: "abc123"}
                        ]
                      }
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }

      {[fetch_response], last_offset} = Adapter.fetch_response(response)

      assert fetch_response.topic == "v4-topic"
      assert [partition_data] = fetch_response.partitions
      assert partition_data.partition == 1
      assert partition_data.hw_mark_offset == 200
      assert length(partition_data.message_set) == 1
      assert last_offset == 100

      [msg] = partition_data.message_set
      assert msg.offset == 100
      assert msg.key == "event-key"
      assert msg.value == "event-data"
      assert msg.timestamp == 1_702_000_000_000
      assert msg.headers == [{"content-type", "application/json"}, {"trace-id", "abc123"}]
    end

    test "parses response with multiple RecordBatches" do
      response = %{
        responses: [
          %{
            topic: "multi-batch-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 0,
                  high_watermark: 300
                },
                record_set: [
                  %RecordBatch{
                    records: [
                      %Record{offset: 10, key: "k1", value: "v1", timestamp: 1000, attributes: 0, headers: nil},
                      %Record{offset: 11, key: "k2", value: "v2", timestamp: 1001, attributes: 0, headers: nil}
                    ]
                  },
                  %RecordBatch{
                    records: [
                      %Record{offset: 20, key: "k3", value: "v3", timestamp: 2000, attributes: 0, headers: nil}
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }

      {[fetch_response], last_offset} = Adapter.fetch_response(response)

      assert [partition_data] = fetch_response.partitions
      assert length(partition_data.message_set) == 3
      assert last_offset == 20

      offsets = Enum.map(partition_data.message_set, & &1.offset)
      assert offsets == [10, 11, 20]
    end

    test "parses response with nil record_set (no messages)" do
      response = %{
        responses: [
          %{
            topic: "empty-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 0,
                  high_watermark: 50
                },
                record_set: nil
              }
            ]
          }
        ]
      }

      {[fetch_response], last_offset} = Adapter.fetch_response(response)

      assert fetch_response.topic == "empty-topic"
      assert [partition_data] = fetch_response.partitions
      assert partition_data.message_set == []
      assert partition_data.hw_mark_offset == 50
      # last_offset is nil when no messages, but the partition struct uses high_watermark as fallback
      assert last_offset == nil
      assert partition_data.last_offset == 50
    end

    test "parses response with empty MessageSet" do
      response = %{
        responses: [
          %{
            topic: "empty-messages-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 0,
                  high_watermark: 100
                },
                record_set: %MessageSet{messages: []}
              }
            ]
          }
        ]
      }

      {[fetch_response], last_offset} = Adapter.fetch_response(response)

      assert [partition_data] = fetch_response.partitions
      assert partition_data.message_set == []
      # last_offset is nil when no messages, partition struct uses hw as fallback
      assert last_offset == nil
      assert partition_data.last_offset == 100
    end

    test "parses response with error code" do
      response = %{
        responses: [
          %{
            topic: "error-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 1,
                  high_watermark: 0
                },
                record_set: nil
              }
            ]
          }
        ]
      }

      {[fetch_response], last_offset} = Adapter.fetch_response(response)

      assert [partition_data] = fetch_response.partitions
      assert partition_data.error_code == :offset_out_of_range
      # last_offset is nil when no messages
      assert last_offset == nil
      assert partition_data.last_offset == 0
    end

    test "returns empty response for throttled response (empty responses)" do
      response = %{responses: []}

      {responses, last_offset} = Adapter.fetch_response(response)

      assert responses == []
      assert last_offset == nil
    end

    test "parses response with nil headers in record" do
      response = %{
        responses: [
          %{
            topic: "no-headers-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 0,
                  high_watermark: 10
                },
                record_set: [
                  %RecordBatch{
                    records: [
                      %Record{
                        offset: 5,
                        key: "k",
                        value: "v",
                        timestamp: 1000,
                        attributes: 0,
                        headers: nil
                      }
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }

      {[fetch_response], _last_offset} = Adapter.fetch_response(response)

      [partition_data] = fetch_response.partitions
      [msg] = partition_data.message_set
      assert msg.headers == []
    end

    test "parses response with empty headers list" do
      response = %{
        responses: [
          %{
            topic: "empty-headers-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 0,
                  high_watermark: 10
                },
                record_set: [
                  %RecordBatch{
                    records: [
                      %Record{
                        offset: 5,
                        key: "k",
                        value: "v",
                        timestamp: 1000,
                        attributes: 0,
                        headers: []
                      }
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }

      {[fetch_response], _last_offset} = Adapter.fetch_response(response)

      [partition_data] = fetch_response.partitions
      [msg] = partition_data.message_set
      assert msg.headers == []
    end

    test "parses response with nil key and value" do
      response = %{
        responses: [
          %{
            topic: "nil-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 0,
                  high_watermark: 10
                },
                record_set: %MessageSet{
                  messages: [
                    %Message{
                      offset: 0,
                      key: nil,
                      value: nil,
                      timestamp: nil,
                      attributes: 0,
                      crc: 0
                    }
                  ]
                }
              }
            ]
          }
        ]
      }

      {[fetch_response], _last_offset} = Adapter.fetch_response(response)

      [partition_data] = fetch_response.partitions
      [msg] = partition_data.message_set
      assert msg.key == nil
      assert msg.value == nil
    end

    test "correctly computes last_offset from non-sequential offsets" do
      response = %{
        responses: [
          %{
            topic: "non-seq-topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 0,
                  high_watermark: 1000
                },
                record_set: %MessageSet{
                  messages: [
                    %Message{offset: 100, key: nil, value: "v1", timestamp: nil, attributes: 0, crc: 0},
                    %Message{offset: 500, key: nil, value: "v2", timestamp: nil, attributes: 0, crc: 0},
                    %Message{offset: 200, key: nil, value: "v3", timestamp: nil, attributes: 0, crc: 0}
                  ]
                }
              }
            ]
          }
        ]
      }

      {_fetch_response, last_offset} = Adapter.fetch_response(response)

      # last_offset should be max of all offsets
      assert last_offset == 500
    end
  end
end
