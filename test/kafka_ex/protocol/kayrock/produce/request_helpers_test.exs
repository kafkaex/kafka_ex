defmodule KafkaEx.Protocol.Kayrock.Produce.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Produce.RequestHelpers
  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  describe "extract_common_fields/1" do
    test "extracts required fields" do
      opts = [topic: "test-topic", partition: 0, messages: [%{value: "hello"}]]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.topic == "test-topic"
      assert result.partition == 0
      assert result.messages == [%{value: "hello"}]
    end

    test "uses default values for optional fields" do
      opts = [topic: "test-topic", partition: 0, messages: []]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert result.compression == :none
    end

    test "allows overriding optional fields" do
      opts = [
        topic: "test-topic",
        partition: 0,
        messages: [],
        acks: 1,
        timeout: 10_000,
        compression: :gzip
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.acks == 1
      assert result.timeout == 10_000
      assert result.compression == :gzip
    end

    test "raises on missing required fields" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(partition: 0, messages: [])
      end

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(topic: "test", messages: [])
      end

      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(topic: "test", partition: 0)
      end
    end
  end

  describe "build_message_set/2" do
    test "builds message set with single message" do
      messages = [%{value: "hello"}]

      result = RequestHelpers.build_message_set(messages, :none)

      assert %MessageSet{messages: [msg]} = result
      assert %Message{value: "hello", key: nil, compression: :none} = msg
    end

    test "builds message set with key" do
      messages = [%{key: "my-key", value: "hello"}]

      result = RequestHelpers.build_message_set(messages, :none)

      assert %MessageSet{messages: [msg]} = result
      assert msg.key == "my-key"
      assert msg.value == "hello"
    end

    test "builds message set with multiple messages" do
      messages = [%{value: "msg1"}, %{value: "msg2"}, %{value: "msg3"}]

      result = RequestHelpers.build_message_set(messages, :none)

      assert %MessageSet{messages: msgs} = result
      assert length(msgs) == 3
    end

    test "applies compression to all messages" do
      messages = [%{value: "hello"}, %{value: "world"}]

      result = RequestHelpers.build_message_set(messages, :gzip)

      assert Enum.all?(result.messages, &(&1.compression == :gzip))
    end
  end

  describe "build_record_batch/2" do
    test "builds record batch with single record" do
      messages = [%{value: "hello"}]

      result = RequestHelpers.build_record_batch(messages, :none)

      assert %RecordBatch{records: [record]} = result
      assert %Record{value: "hello", key: nil, timestamp: -1, headers: []} = record
    end

    test "builds record batch with key and timestamp" do
      messages = [%{key: "my-key", value: "hello", timestamp: 1_234_567_890}]

      result = RequestHelpers.build_record_batch(messages, :none)

      assert %RecordBatch{records: [record]} = result
      assert record.key == "my-key"
      assert record.value == "hello"
      assert record.timestamp == 1_234_567_890
    end

    test "builds record batch with headers" do
      messages = [
        %{
          value: "hello",
          headers: [{"header1", "value1"}, {"header2", "value2"}]
        }
      ]

      result = RequestHelpers.build_record_batch(messages, :none)

      assert %RecordBatch{records: [record]} = result
      assert [h1, h2] = record.headers
      assert %RecordHeader{key: "header1", value: "value1"} = h1
      assert %RecordHeader{key: "header2", value: "value2"} = h2
    end

    test "sets compression attributes" do
      messages = [%{value: "hello"}]

      result = RequestHelpers.build_record_batch(messages, :gzip)

      assert result.attributes == 1
    end
  end

  describe "compression_to_attributes/1" do
    test "returns correct attribute values" do
      assert RequestHelpers.compression_to_attributes(:none) == 0
      assert RequestHelpers.compression_to_attributes(:gzip) == 1
      assert RequestHelpers.compression_to_attributes(:snappy) == 2
      assert RequestHelpers.compression_to_attributes(:lz4) == 3
      assert RequestHelpers.compression_to_attributes(:zstd) == 4
    end

    test "raises FunctionClauseError for unknown compression type" do
      assert_raise FunctionClauseError, fn ->
        RequestHelpers.compression_to_attributes(:brotli)
      end
    end
  end

  describe "build_record_headers/1" do
    test "returns empty list for nil" do
      assert RequestHelpers.build_record_headers(nil) == []
    end

    test "returns empty list for empty list" do
      assert RequestHelpers.build_record_headers([]) == []
    end

    test "converts tuples to RecordHeader structs" do
      headers = [{"key1", "value1"}, {"key2", "value2"}]

      result = RequestHelpers.build_record_headers(headers)

      assert [h1, h2] = result
      assert %RecordHeader{key: "key1", value: "value1"} = h1
      assert %RecordHeader{key: "key2", value: "value2"} = h2
    end
  end

  describe "build_request_v0_v2/2" do
    test "builds request with message set" do
      template = %{}
      opts = [topic: "test-topic", partition: 0, messages: [%{value: "hello"}]]

      result = RequestHelpers.build_request_v0_v2(template, opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert [topic_data] = result.topic_data
      assert topic_data.topic == "test-topic"
      assert [data] = topic_data.data
      assert data.partition == 0
      assert %MessageSet{} = data.record_set
    end
  end

  describe "build_request_v3_plus/2" do
    test "builds request with record batch" do
      template = %{}
      opts = [topic: "test-topic", partition: 0, messages: [%{value: "hello"}]]

      result = RequestHelpers.build_request_v3_plus(template, opts)

      assert result.acks == -1
      assert result.timeout == 5000
      assert result.transactional_id == nil
      assert [topic_data] = result.topic_data
      assert topic_data.topic == "test-topic"
      assert [data] = topic_data.data
      assert data.partition == 0
      assert %RecordBatch{} = data.record_set
    end

    test "includes transactional_id when provided" do
      template = %{}

      opts = [
        topic: "test-topic",
        partition: 0,
        messages: [%{value: "hello"}],
        transactional_id: "my-tx-id"
      ]

      result = RequestHelpers.build_request_v3_plus(template, opts)

      assert result.transactional_id == "my-tx-id"
    end
  end
end
