defmodule KafkaEx.New.AdapterProduceTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Adapter
  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest
  alias KafkaEx.Protocol.Produce.Message, as: ProduceMessage
  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  describe "Adapter.produce_request/1" do
    test "builds V0 request with single message" do
      request = %ProduceRequest{
        topic: "test-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :none,
        messages: [%ProduceMessage{key: "key1", value: "value1"}],
        api_version: 0
      }

      {kayrock_request, topic, partition} = Adapter.produce_request(request)

      assert topic == "test-topic"
      assert partition == 0
      assert kayrock_request.acks == -1
      assert kayrock_request.timeout == 5000
      assert [%{topic: "test-topic", data: [%{partition: 0, record_set: message_set}]}] = kayrock_request.topic_data
      assert %MessageSet{messages: [msg]} = message_set
      assert %Message{key: "key1", value: "value1", compression: :none} = msg
    end

    test "builds V0 request with multiple messages" do
      request = %ProduceRequest{
        topic: "multi-msg-topic",
        partition: 2,
        required_acks: 1,
        timeout: 10_000,
        compression: :none,
        messages: [
          %ProduceMessage{key: "k1", value: "v1"},
          %ProduceMessage{key: "k2", value: "v2"},
          %ProduceMessage{key: nil, value: "v3"}
        ],
        api_version: 0
      }

      {kayrock_request, topic, partition} = Adapter.produce_request(request)

      assert topic == "multi-msg-topic"
      assert partition == 2
      assert kayrock_request.acks == 1
      assert [%{data: [%{record_set: message_set}]}] = kayrock_request.topic_data
      assert %MessageSet{messages: msgs} = message_set
      assert length(msgs) == 3
      assert Enum.at(msgs, 0).key == "k1"
      assert Enum.at(msgs, 1).key == "k2"
      assert Enum.at(msgs, 2).key == nil
    end

    test "builds V1 request with MessageSet" do
      request = %ProduceRequest{
        topic: "v1-topic",
        partition: 1,
        required_acks: 0,
        timeout: 3000,
        compression: :none,
        messages: [%ProduceMessage{value: "hello"}],
        api_version: 1
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert kayrock_request.acks == 0
      assert [%{data: [%{record_set: message_set}]}] = kayrock_request.topic_data
      assert %MessageSet{} = message_set
    end

    test "builds V2 request with MessageSet" do
      request = %ProduceRequest{
        topic: "v2-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :none,
        messages: [%ProduceMessage{key: "k", value: "v"}],
        api_version: 2
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: message_set}]}] = kayrock_request.topic_data
      assert %MessageSet{} = message_set
    end

    test "builds V3 request with RecordBatch" do
      request = %ProduceRequest{
        topic: "v3-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :none,
        messages: [%ProduceMessage{key: "k", value: "v", timestamp: 1_234_567_890}],
        api_version: 3
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: record_batch}]}] = kayrock_request.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert %Record{key: "k", value: "v", timestamp: 1_234_567_890} = record
    end

    test "builds V3 request with headers" do
      request = %ProduceRequest{
        topic: "headers-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :none,
        messages: [
          %ProduceMessage{
            key: "event-1",
            value: "event-data",
            headers: [{"content-type", "application/json"}, {"trace-id", "abc123"}]
          }
        ],
        api_version: 3
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: record_batch}]}] = kayrock_request.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert length(record.headers) == 2
      assert Enum.at(record.headers, 0).key == "content-type"
      assert Enum.at(record.headers, 0).value == "application/json"
    end

    test "builds V3 request with nil timestamp defaults to -1" do
      request = %ProduceRequest{
        topic: "no-timestamp-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :none,
        messages: [%ProduceMessage{value: "v", timestamp: nil}],
        api_version: 3
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: record_batch}]}] = kayrock_request.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.timestamp == -1
    end

    test "raises TimestampNotSupportedError for V0-V2 with timestamp" do
      request = %ProduceRequest{
        topic: "test-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :none,
        messages: [%ProduceMessage{value: "v", timestamp: 1_234_567_890}],
        api_version: 0
      }

      assert_raise KafkaEx.TimestampNotSupportedError, fn ->
        Adapter.produce_request(request)
      end
    end

    test "builds request with gzip compression for V0-V2" do
      request = %ProduceRequest{
        topic: "compressed-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :gzip,
        messages: [%ProduceMessage{value: "compressed-value"}],
        api_version: 2
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: message_set}]}] = kayrock_request.topic_data
      assert %MessageSet{messages: [msg]} = message_set
      assert msg.compression == :gzip
    end

    test "builds request with snappy compression for V0-V2" do
      request = %ProduceRequest{
        topic: "snappy-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :snappy,
        messages: [%ProduceMessage{value: "snappy-value"}],
        api_version: 1
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: message_set}]}] = kayrock_request.topic_data
      assert %MessageSet{messages: [msg]} = message_set
      assert msg.compression == :snappy
    end

    test "builds request with gzip compression for V3+ (RecordBatch attributes)" do
      request = %ProduceRequest{
        topic: "compressed-v3-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :gzip,
        messages: [%ProduceMessage{value: "compressed-value"}],
        api_version: 3
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: record_batch}]}] = kayrock_request.topic_data
      assert %RecordBatch{} = record_batch
      assert record_batch.attributes == 1
    end

    test "builds request with snappy compression for V3+ (RecordBatch attributes)" do
      request = %ProduceRequest{
        topic: "snappy-v3-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :snappy,
        messages: [%ProduceMessage{value: "snappy-value"}],
        api_version: 3
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: record_batch}]}] = kayrock_request.topic_data
      assert record_batch.attributes == 2
    end

    test "handles nil key in message" do
      request = %ProduceRequest{
        topic: "nil-key-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :none,
        messages: [%ProduceMessage{key: nil, value: "value-only"}],
        api_version: 0
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: message_set}]}] = kayrock_request.topic_data
      assert %MessageSet{messages: [msg]} = message_set
      assert msg.key == nil
      assert msg.value == "value-only"
    end

    test "handles nil headers in V3+ request" do
      request = %ProduceRequest{
        topic: "no-headers-topic",
        partition: 0,
        required_acks: -1,
        timeout: 5000,
        compression: :none,
        messages: [%ProduceMessage{value: "v", headers: nil}],
        api_version: 3
      }

      {kayrock_request, _topic, _partition} = Adapter.produce_request(request)

      assert [%{data: [%{record_set: record_batch}]}] = kayrock_request.topic_data
      assert %RecordBatch{records: [record]} = record_batch
      assert record.headers == []
    end
  end

  describe "Adapter.produce_response/1" do
    test "parses successful response with base_offset" do
      response = %{responses: [%{partition_responses: [%{base_offset: 42, error_code: 0}]}]}

      assert {:ok, 42} = Adapter.produce_response(response)
    end

    test "returns error when response contain one" do
      response = %{responses: [%{partition_responses: [%{base_offset: -1, error_code: 3}]}]}

      assert {:error, :unknown_topic_or_partition} = Adapter.produce_response(response)
    end

    test "handles response with additional fields (V2+)" do
      response = %{
        responses: [%{partition_responses: [%{base_offset: 100, error_code: 0, log_append_time: 1_702_000_000_000}]}]
      }

      assert {:ok, 100} = Adapter.produce_response(response)
    end
  end
end
