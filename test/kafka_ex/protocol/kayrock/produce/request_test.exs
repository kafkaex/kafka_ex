defmodule KafkaEx.Protocol.Kayrock.Produce.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Produce.Request
  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers
  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts all required fields" do
      opts = [
        topic: "test-topic",
        partition: 0,
        messages: [%{value: "hello"}],
        acks: -1,
        timeout: 5000,
        compression: :gzip
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.topic == "test-topic"
      assert result.partition == 0
      assert result.messages == [%{value: "hello"}]
      assert result.acks == -1
      assert result.timeout == 5000
      assert result.compression == :gzip
    end

    test "uses defaults for optional fields" do
      opts = [
        topic: "test-topic",
        partition: 0,
        messages: [%{value: "hello"}]
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert result.compression == :none
    end

    test "raises when required fields are missing" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(partition: 0, messages: [])
      end

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(topic: "t", messages: [])
      end

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(topic: "t", partition: 0)
      end
    end
  end

  describe "RequestHelpers.build_message_set/2" do
    test "builds MessageSet with single message" do
      messages = [%{value: "hello", key: "key1"}]

      result = RequestHelpers.build_message_set(messages, :none)

      assert %MessageSet{messages: [msg]} = result
      assert %Message{value: "hello", key: "key1", compression: :none} = msg
    end

    test "builds MessageSet with multiple messages" do
      messages = [
        %{value: "msg1", key: "k1"},
        %{value: "msg2", key: nil},
        %{value: "msg3"}
      ]

      result = RequestHelpers.build_message_set(messages, :gzip)

      assert %MessageSet{messages: msgs} = result
      assert length(msgs) == 3
      assert Enum.all?(msgs, fn m -> m.compression == :gzip end)
    end

    test "handles nil key" do
      messages = [%{value: "hello"}]

      result = RequestHelpers.build_message_set(messages, :none)

      assert %MessageSet{messages: [msg]} = result
      assert msg.key == nil
    end
  end

  describe "RequestHelpers.build_record_batch/2" do
    test "builds RecordBatch with single record" do
      messages = [%{value: "hello", key: "key1"}]

      result = RequestHelpers.build_record_batch(messages, :none)

      assert %RecordBatch{records: [record], attributes: 0} = result
      assert %Record{value: "hello", key: "key1"} = record
    end

    test "builds RecordBatch with headers" do
      messages = [
        %{
          value: "hello",
          key: "key1",
          headers: [{"content-type", "text/plain"}, {"x-custom", "value"}]
        }
      ]

      result = RequestHelpers.build_record_batch(messages, :none)

      assert %RecordBatch{records: [record]} = result
      assert length(record.headers) == 2
      assert Enum.at(record.headers, 0).key == "content-type"
      assert Enum.at(record.headers, 0).value == "text/plain"
    end

    test "builds RecordBatch with timestamp" do
      messages = [%{value: "hello", timestamp: 1_234_567_890}]

      result = RequestHelpers.build_record_batch(messages, :none)

      assert %RecordBatch{records: [record]} = result
      assert record.timestamp == 1_234_567_890
    end

    test "uses -1 for missing timestamp" do
      messages = [%{value: "hello"}]

      result = RequestHelpers.build_record_batch(messages, :none)

      assert %RecordBatch{records: [record]} = result
      assert record.timestamp == -1
    end

    test "handles nil headers" do
      messages = [%{value: "hello", headers: nil}]

      result = RequestHelpers.build_record_batch(messages, :none)

      assert %RecordBatch{records: [record]} = result
      assert record.headers == []
    end
  end

  describe "RequestHelpers.compression_to_attributes/1" do
    test "maps compression types to attribute values" do
      assert RequestHelpers.compression_to_attributes(:none) == 0
      assert RequestHelpers.compression_to_attributes(:gzip) == 1
      assert RequestHelpers.compression_to_attributes(:snappy) == 2
      assert RequestHelpers.compression_to_attributes(:lz4) == 3
      assert RequestHelpers.compression_to_attributes(:zstd) == 4
    end
  end

  describe "V0 Request implementation" do
    test "builds V0 request with MessageSet" do
      template = %Kayrock.Produce.V0.Request{}

      opts = [
        topic: "test-topic",
        partition: 0,
        messages: [%{value: "hello"}],
        acks: 1,
        timeout: 10_000
      ]

      result = Request.build_request(template, opts)

      assert result.acks == 1
      assert result.timeout == 10_000
      assert [%{topic: "test-topic", data: [%{partition: 0, record_set: message_set}]}] = result.topic_data
      assert %MessageSet{} = message_set
    end
  end

  describe "V1 Request implementation" do
    test "builds V1 request with MessageSet" do
      template = %Kayrock.Produce.V1.Request{}

      opts = [
        topic: "events",
        partition: 2,
        messages: [%{value: "event1"}, %{value: "event2"}],
        acks: -1,
        timeout: 5000,
        compression: :snappy
      ]

      result = Request.build_request(template, opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert [%{topic: "events", data: [%{partition: 2, record_set: message_set}]}] = result.topic_data
      assert %MessageSet{messages: messages} = message_set
      assert length(messages) == 2
      assert Enum.all?(messages, fn m -> m.compression == :snappy end)
    end
  end

  describe "V2 Request implementation" do
    test "builds V2 request with MessageSet" do
      template = %Kayrock.Produce.V2.Request{}

      opts = [
        topic: "logs",
        partition: 1,
        messages: [%{value: "log entry", key: "log-1"}],
        acks: 0,
        timeout: 3000
      ]

      result = Request.build_request(template, opts)

      assert result.acks == 0
      assert result.timeout == 3000
      assert [%{topic: "logs", data: [%{partition: 1, record_set: message_set}]}] = result.topic_data
      assert %MessageSet{messages: [msg]} = message_set
      assert msg.value == "log entry"
      assert msg.key == "log-1"
    end
  end

  describe "V3 Request implementation" do
    test "builds V3 request with RecordBatch" do
      template = %Kayrock.Produce.V3.Request{}

      opts = [
        topic: "transactions",
        partition: 0,
        messages: [%{value: "tx-data", key: "tx-1", timestamp: 1_234_567_890}],
        acks: -1,
        timeout: 5000
      ]

      result = Request.build_request(template, opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert result.transactional_id == nil
      assert [%{topic: "transactions", data: [%{partition: 0, record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.value == "tx-data"
      assert record.timestamp == 1_234_567_890
    end

    test "builds V3 request with transactional_id" do
      template = %Kayrock.Produce.V3.Request{}

      opts = [
        topic: "transactions",
        partition: 0,
        messages: [%{value: "tx-data"}],
        transactional_id: "my-transaction-id"
      ]

      result = Request.build_request(template, opts)

      assert result.transactional_id == "my-transaction-id"
    end

    test "builds V3 request with headers" do
      template = %Kayrock.Produce.V3.Request{}

      opts = [
        topic: "events",
        partition: 0,
        messages: [
          %{
            value: "event data",
            headers: [{"event-type", "user.created"}, {"version", "1"}]
          }
        ]
      ]

      result = Request.build_request(template, opts)

      assert [%{data: [%{record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert length(record.headers) == 2
    end

    test "builds V3 request with compression" do
      template = %Kayrock.Produce.V3.Request{}

      opts = [
        topic: "compressed",
        partition: 0,
        messages: [%{value: "data"}],
        compression: :lz4
      ]

      result = Request.build_request(template, opts)

      assert [%{data: [%{record_set: record_batch}]}] = result.topic_data
      # lz4
      assert record_batch.attributes == 3
    end
  end

  describe "V4 Request implementation" do
    test "builds V4 request with RecordBatch (same as V3)" do
      template = %Kayrock.Produce.V4.Request{}

      opts = [
        topic: "v4-topic",
        partition: 1,
        messages: [%{value: "v4-data", key: "v4-key"}],
        acks: -1,
        timeout: 5000
      ]

      result = Request.build_request(template, opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert result.transactional_id == nil
      assert [%{topic: "v4-topic", data: [%{partition: 1, record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.value == "v4-data"
      assert record.key == "v4-key"
    end

    test "builds V4 request with transactional_id" do
      template = %Kayrock.Produce.V4.Request{}

      opts = [
        topic: "transactions",
        partition: 0,
        messages: [%{value: "tx-data"}],
        transactional_id: "v4-tx-id"
      ]

      result = Request.build_request(template, opts)

      assert result.transactional_id == "v4-tx-id"
    end
  end

  describe "V5 Request implementation" do
    test "builds V5 request with RecordBatch (same as V3/V4)" do
      template = %Kayrock.Produce.V5.Request{}

      opts = [
        topic: "v5-topic",
        partition: 2,
        messages: [%{value: "v5-data", headers: [{"trace-id", "abc123"}]}],
        acks: 1,
        timeout: 10_000
      ]

      result = Request.build_request(template, opts)

      assert result.acks == 1
      assert result.timeout == 10_000
      assert [%{topic: "v5-topic", data: [%{partition: 2, record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.value == "v5-data"
      assert length(record.headers) == 1
    end

    test "builds V5 request with transactional_id" do
      template = %Kayrock.Produce.V5.Request{}

      opts = [
        topic: "transactions",
        partition: 0,
        messages: [%{value: "tx-data"}],
        transactional_id: "v5-tx-id"
      ]

      result = Request.build_request(template, opts)

      assert result.transactional_id == "v5-tx-id"
    end

    test "builds V5 request with zstd compression" do
      template = %Kayrock.Produce.V5.Request{}

      opts = [
        topic: "compressed",
        partition: 0,
        messages: [%{value: "data"}],
        compression: :zstd
      ]

      result = Request.build_request(template, opts)

      assert [%{data: [%{record_set: record_batch}]}] = result.topic_data
      # zstd = 4
      assert record_batch.attributes == 4
    end
  end

  describe "V6 Request implementation" do
    test "builds V6 request with RecordBatch (same as V3-V5)" do
      template = %Kayrock.Produce.V6.Request{}

      opts = [
        topic: "v6-topic",
        partition: 0,
        messages: [%{value: "v6-data", key: "v6-key"}],
        acks: -1,
        timeout: 5000
      ]

      result = Request.build_request(template, opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert result.transactional_id == nil
      assert [%{topic: "v6-topic", data: [%{partition: 0, record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.value == "v6-data"
      assert record.key == "v6-key"
    end

    test "builds V6 request with transactional_id" do
      template = %Kayrock.Produce.V6.Request{}

      opts = [
        topic: "transactions",
        partition: 0,
        messages: [%{value: "tx-data"}],
        transactional_id: "v6-tx-id"
      ]

      result = Request.build_request(template, opts)

      assert result.transactional_id == "v6-tx-id"
    end

    test "builds V6 request with headers and compression" do
      template = %Kayrock.Produce.V6.Request{}

      opts = [
        topic: "events",
        partition: 1,
        messages: [
          %{
            value: "event data",
            key: "event-1",
            headers: [{"event-type", "order.created"}],
            timestamp: 1_702_300_000_000
          }
        ],
        compression: :gzip
      ]

      result = Request.build_request(template, opts)

      assert [%{data: [%{partition: 1, record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.value == "event data"
      assert record.key == "event-1"
      assert record.timestamp == 1_702_300_000_000
      assert length(record.headers) == 1
      # gzip = 1
      assert record_batch.attributes == 1
    end
  end

  describe "V7 Request implementation" do
    test "builds V7 request with RecordBatch (same as V3-V6)" do
      template = %Kayrock.Produce.V7.Request{}

      opts = [
        topic: "v7-topic",
        partition: 3,
        messages: [%{value: "v7-data"}],
        acks: 1,
        timeout: 15_000
      ]

      result = Request.build_request(template, opts)

      assert result.acks == 1
      assert result.timeout == 15_000
      assert result.transactional_id == nil
      assert [%{topic: "v7-topic", data: [%{partition: 3, record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.value == "v7-data"
    end

    test "builds V7 request with transactional_id" do
      template = %Kayrock.Produce.V7.Request{}

      opts = [
        topic: "transactions",
        partition: 0,
        messages: [%{value: "tx-data"}],
        transactional_id: "v7-tx-id"
      ]

      result = Request.build_request(template, opts)

      assert result.transactional_id == "v7-tx-id"
    end

    test "builds V7 request with multiple messages" do
      template = %Kayrock.Produce.V7.Request{}

      opts = [
        topic: "batch-topic",
        partition: 0,
        messages: [
          %{value: "msg1", key: "k1"},
          %{value: "msg2", key: "k2"},
          %{value: "msg3", key: "k3"}
        ]
      ]

      result = Request.build_request(template, opts)

      assert [%{data: [%{record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: records} = record_batch
      assert length(records) == 3
    end
  end

  describe "V8 Request implementation" do
    test "builds V8 request with RecordBatch (same as V3-V7)" do
      template = %Kayrock.Produce.V8.Request{}

      opts = [
        topic: "v8-topic",
        partition: 0,
        messages: [%{value: "v8-data", key: "v8-key"}],
        acks: -1,
        timeout: 5000
      ]

      result = Request.build_request(template, opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert result.transactional_id == nil
      assert [%{topic: "v8-topic", data: [%{partition: 0, record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.value == "v8-data"
      assert record.key == "v8-key"
    end

    test "builds V8 request with transactional_id" do
      template = %Kayrock.Produce.V8.Request{}

      opts = [
        topic: "transactions",
        partition: 0,
        messages: [%{value: "tx-data"}],
        transactional_id: "v8-tx-id"
      ]

      result = Request.build_request(template, opts)

      assert result.transactional_id == "v8-tx-id"
    end

    test "builds V8 request with headers and compression" do
      template = %Kayrock.Produce.V8.Request{}

      opts = [
        topic: "events",
        partition: 2,
        messages: [
          %{
            value: "event data",
            headers: [{"trace-id", "xyz789"}, {"event-type", "payment.completed"}],
            timestamp: 1_702_400_000_000
          }
        ],
        compression: :snappy
      ]

      result = Request.build_request(template, opts)

      assert [%{data: [%{partition: 2, record_set: record_batch}]}] = result.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.value == "event data"
      assert record.timestamp == 1_702_400_000_000
      assert length(record.headers) == 2
      # snappy = 2
      assert record_batch.attributes == 2
    end

    test "uses default options when not specified" do
      template = %Kayrock.Produce.V8.Request{}

      opts = [
        topic: "defaults-topic",
        partition: 0,
        messages: [%{value: "data"}]
      ]

      result = Request.build_request(template, opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert result.transactional_id == nil
      assert [%{data: [%{record_set: %RecordBatch{attributes: 0}}]}] = result.topic_data
    end
  end
end
