defmodule KafkaEx.Protocol.Kayrock.Fetch.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Fetch.ResponseHelpers
  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Messages.Header

  describe "extract_first_partition_response/1" do
    test "extracts first partition response" do
      response = %{
        responses: [
          %{
            topic: "test_topic",
            partition_responses: [
              %{partition_header: %{partition: 0}}
            ]
          }
        ]
      }

      assert {:ok, "test_topic", partition_resp} =
               ResponseHelpers.extract_first_partition_response(response)

      assert partition_resp.partition_header.partition == 0
    end

    test "returns error for empty responses" do
      assert {:error, :empty_response} =
               ResponseHelpers.extract_first_partition_response(%{responses: []})
    end

    test "returns error for missing responses" do
      assert {:error, :empty_response} =
               ResponseHelpers.extract_first_partition_response(%{})
    end
  end

  describe "check_error/2" do
    test "returns ok when no error" do
      partition_resp = %{
        partition_header: %{partition: 0, error_code: 0}
      }

      assert {:ok, ^partition_resp} = ResponseHelpers.check_error("topic", partition_resp)
    end

    test "returns error when error code is non-zero" do
      partition_resp = %{
        partition_header: %{partition: 0, error_code: 3}
      }

      assert {:error, error} = ResponseHelpers.check_error("topic", partition_resp)
      assert error.error == :unknown_topic_or_partition
    end
  end

  describe "convert_records/3" do
    test "returns empty list for nil record_set" do
      assert [] = ResponseHelpers.convert_records(nil, "topic", 0)
    end

    test "converts MessageSet to records" do
      message_set = %Kayrock.MessageSet{
        messages: [
          %Kayrock.MessageSet.Message{
            offset: 0,
            key: "key1",
            value: "value1",
            timestamp: 1000,
            attributes: 0,
            crc: 12345
          },
          %Kayrock.MessageSet.Message{
            offset: 1,
            key: "key2",
            value: "value2",
            timestamp: 1001,
            attributes: 0,
            crc: 12346
          }
        ]
      }

      records = ResponseHelpers.convert_records(message_set, "test_topic", 0)

      assert length(records) == 2
      [rec1, rec2] = records

      assert rec1.offset == 0
      assert rec1.key == "key1"
      assert rec1.value == "value1"
      assert rec1.topic == "test_topic"
      assert rec1.partition == 0
      assert rec1.timestamp_type == :create_time

      assert rec2.offset == 1
    end

    test "converts RecordBatch list to records" do
      record_batches = [
        %Kayrock.RecordBatch{
          attributes: 0,
          records: [
            %Kayrock.RecordBatch.Record{
              offset: 0,
              key: "key1",
              value: "value1",
              timestamp: 1000,
              attributes: 0,
              headers: [
                %Kayrock.RecordBatch.RecordHeader{key: "h1", value: "v1"}
              ]
            }
          ]
        },
        %Kayrock.RecordBatch{
          attributes: 8,
          records: [
            %Kayrock.RecordBatch.Record{
              offset: 1,
              key: "key2",
              value: "value2",
              timestamp: 1001,
              attributes: 0,
              headers: nil
            }
          ]
        }
      ]

      records = ResponseHelpers.convert_records(record_batches, "test_topic", 0)

      assert length(records) == 2
      [rec1, rec2] = records

      assert rec1.offset == 0
      assert rec1.headers == [%Header{key: "h1", value: "v1"}]
      assert rec1.topic == "test_topic"
      assert rec1.timestamp_type == :create_time

      assert rec2.offset == 1
      assert rec2.headers == nil
      assert rec2.timestamp_type == :log_append_time
    end

    test "extracts timestamp_type from MessageSet attributes" do
      # Bit 3 = 0 means CreateTime
      message_set_create = %Kayrock.MessageSet{
        messages: [
          %Kayrock.MessageSet.Message{
            offset: 0,
            key: nil,
            value: "value",
            timestamp: 1000,
            attributes: 0,
            crc: 12345
          }
        ]
      }

      [rec] = ResponseHelpers.convert_records(message_set_create, "topic", 0)
      assert rec.timestamp_type == :create_time

      # Bit 3 = 1 means LogAppendTime (attributes = 8 = 0b1000)
      message_set_append = %Kayrock.MessageSet{
        messages: [
          %Kayrock.MessageSet.Message{
            offset: 0,
            key: nil,
            value: "value",
            timestamp: 1000,
            attributes: 8,
            crc: 12345
          }
        ]
      }

      [rec] = ResponseHelpers.convert_records(message_set_append, "topic", 0)
      assert rec.timestamp_type == :log_append_time
    end
  end

  describe "convert_headers/1" do
    test "returns nil for nil input" do
      assert nil == ResponseHelpers.convert_headers(nil)
    end

    test "returns empty list for empty input" do
      assert [] == ResponseHelpers.convert_headers([])
    end

    test "converts RecordHeader list to Header structs" do
      headers = [
        %Kayrock.RecordBatch.RecordHeader{key: "header1", value: "value1"},
        %Kayrock.RecordBatch.RecordHeader{key: "header2", value: "value2"}
      ]

      result = ResponseHelpers.convert_headers(headers)

      assert [
               %Header{key: "header1", value: "value1"},
               %Header{key: "header2", value: "value2"}
             ] = result
    end
  end

  describe "compute_last_offset/1" do
    test "returns nil for empty records" do
      assert nil == ResponseHelpers.compute_last_offset([])
    end

    test "returns max offset from records" do
      records = [
        %Record{offset: 5},
        %Record{offset: 10},
        %Record{offset: 7}
      ]

      assert 10 == ResponseHelpers.compute_last_offset(records)
    end
  end

  describe "extract_v5_plus_fields/2" do
    test "extracts all V5+ fields" do
      response = %{throttle_time_ms: 10}

      partition_resp = %{
        partition_header: %{
          last_stable_offset: 500,
          log_start_offset: 100,
          aborted_transactions: [%{producer_id: 1, first_offset: 50}]
        }
      }

      fields = ResponseHelpers.extract_v5_plus_fields(response, partition_resp)

      assert Keyword.get(fields, :throttle_time_ms) == 10
      assert Keyword.get(fields, :last_stable_offset) == 500
      assert Keyword.get(fields, :log_start_offset) == 100
      assert Keyword.get(fields, :aborted_transactions) == [%{producer_id: 1, first_offset: 50}]
    end

    test "does not include preferred_read_replica" do
      response = %{throttle_time_ms: 0}

      partition_resp = %{
        partition_header: %{
          last_stable_offset: 500,
          log_start_offset: 100,
          aborted_transactions: [],
          preferred_read_replica: 2
        }
      }

      fields = ResponseHelpers.extract_v5_plus_fields(response, partition_resp)

      refute Keyword.has_key?(fields, :preferred_read_replica)
    end
  end

  describe "extract_v11_fields/2" do
    test "extracts all V11 fields including preferred_read_replica" do
      response = %{throttle_time_ms: 15}

      partition_resp = %{
        partition_header: %{
          last_stable_offset: 800,
          log_start_offset: 200,
          aborted_transactions: nil,
          preferred_read_replica: 3
        }
      }

      fields = ResponseHelpers.extract_v11_fields(response, partition_resp)

      assert Keyword.get(fields, :throttle_time_ms) == 15
      assert Keyword.get(fields, :last_stable_offset) == 800
      assert Keyword.get(fields, :log_start_offset) == 200
      assert Keyword.get(fields, :aborted_transactions) == nil
      assert Keyword.get(fields, :preferred_read_replica) == 3
    end

    test "handles missing preferred_read_replica gracefully" do
      response = %{throttle_time_ms: 0}

      partition_resp = %{
        partition_header: %{
          last_stable_offset: 500,
          log_start_offset: 100,
          aborted_transactions: []
        }
      }

      fields = ResponseHelpers.extract_v11_fields(response, partition_resp)

      assert Keyword.get(fields, :preferred_read_replica) == nil
    end
  end

  describe "parse_response/2" do
    test "parses successful response" do
      response = %{
        responses: [
          %{
            topic: "test_topic",
            partition_responses: [
              %{
                partition_header: %{
                  partition: 0,
                  error_code: 0,
                  high_watermark: 100
                },
                record_set: %Kayrock.MessageSet{
                  messages: [
                    %Kayrock.MessageSet.Message{
                      offset: 0,
                      key: "key",
                      value: "value",
                      timestamp: 1000,
                      attributes: 0,
                      crc: 12345
                    }
                  ]
                }
              }
            ]
          }
        ],
        throttle_time_ms: 5
      }

      field_extractor = fn resp, _part_resp ->
        [throttle_time_ms: Map.get(resp, :throttle_time_ms, 0)]
      end

      assert {:ok, fetch} = ResponseHelpers.parse_response(response, field_extractor)

      assert fetch.topic == "test_topic"
      assert fetch.partition == 0
      assert fetch.high_watermark == 100
      assert fetch.throttle_time_ms == 5
      assert length(fetch.records) == 1
      assert fetch.last_offset == 0
    end

    test "returns error for empty response" do
      response = %{responses: []}

      field_extractor = fn _resp, _part_resp -> [] end

      assert {:error, error} = ResponseHelpers.parse_response(response, field_extractor)
      assert error.error == :empty_response
    end

    test "returns error for Kafka error code" do
      response = %{
        responses: [
          %{
            topic: "test_topic",
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

      field_extractor = fn _resp, _part_resp -> [] end

      assert {:error, error} = ResponseHelpers.parse_response(response, field_extractor)
      assert error.error == :offset_out_of_range
    end
  end
end
