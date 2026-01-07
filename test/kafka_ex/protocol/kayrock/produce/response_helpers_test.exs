defmodule KafkaEx.Protocol.Kayrock.Produce.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Produce.ResponseHelpers
  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.RecordMetadata

  describe "extract_first_partition_response/1" do
    test "extracts topic and partition response" do
      response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [%{partition: 0, base_offset: 100, error_code: 0}]
          }
        ]
      }

      assert {:ok, "test-topic", partition_resp} =
               ResponseHelpers.extract_first_partition_response(response)

      assert partition_resp.partition == 0
      assert partition_resp.base_offset == 100
    end

    test "returns error for empty responses list" do
      response = %{responses: []}

      assert {:error, :empty_response} = ResponseHelpers.extract_first_partition_response(response)
    end

    test "returns error for missing responses key" do
      response = %{}

      assert {:error, :empty_response} = ResponseHelpers.extract_first_partition_response(response)
    end

    test "extracts first topic when multiple topics present" do
      response = %{
        responses: [
          %{topic: "topic1", partition_responses: [%{partition: 0}]},
          %{topic: "topic2", partition_responses: [%{partition: 0}]}
        ]
      }

      assert {:ok, "topic1", _} = ResponseHelpers.extract_first_partition_response(response)
    end
  end

  describe "check_error/2" do
    test "returns ok for no_error (0)" do
      partition_resp = %{partition: 0, error_code: 0}

      assert {:ok, ^partition_resp} = ResponseHelpers.check_error("topic", partition_resp)
    end

    test "returns error for non-zero error code" do
      partition_resp = %{partition: 0, error_code: 3}

      assert {:error, %Error{error: :unknown_topic_or_partition}} =
               ResponseHelpers.check_error("topic", partition_resp)
    end

    test "returns ok when error_code is missing (defaults to 0)" do
      partition_resp = %{partition: 0}

      assert {:ok, ^partition_resp} = ResponseHelpers.check_error("topic", partition_resp)
    end

    test "includes topic and partition in error metadata" do
      partition_resp = %{partition: 5, error_code: 7}

      assert {:error, %Error{metadata: metadata}} =
               ResponseHelpers.check_error("my-topic", partition_resp)

      assert metadata.topic == "my-topic"
      assert metadata.partition == 5
    end
  end

  describe "build_record_metadata/1" do
    test "builds RecordMetadata from options" do
      opts = [topic: "test-topic", partition: 0, base_offset: 100]

      result = ResponseHelpers.build_record_metadata(opts)

      assert %RecordMetadata{topic: "test-topic", partition: 0, base_offset: 100} = result
    end
  end

  describe "empty_response_error/0" do
    test "returns empty_response error" do
      assert {:error, %Error{error: :empty_response}} = ResponseHelpers.empty_response_error()
    end
  end

  describe "parse_response/2" do
    test "parses successful response" do
      response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [%{partition: 0, base_offset: 100, error_code: 0}]
          }
        ]
      }

      field_extractor = fn _response, _partition_resp -> [] end

      assert {:ok, %RecordMetadata{} = metadata} =
               ResponseHelpers.parse_response(response, field_extractor)

      assert metadata.topic == "test-topic"
      assert metadata.partition == 0
      assert metadata.base_offset == 100
    end

    test "merges fields from extractor" do
      response = %{
        throttle_time_ms: 50,
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [
              %{partition: 0, base_offset: 100, error_code: 0, log_append_time: 1_234_567_890}
            ]
          }
        ]
      }

      field_extractor = fn resp, partition_resp ->
        [
          log_append_time: partition_resp.log_append_time,
          throttle_time_ms: resp.throttle_time_ms
        ]
      end

      assert {:ok, %RecordMetadata{} = metadata} =
               ResponseHelpers.parse_response(response, field_extractor)

      assert metadata.log_append_time == 1_234_567_890
      assert metadata.throttle_time_ms == 50
    end

    test "returns error for empty response" do
      response = %{responses: []}
      field_extractor = fn _, _ -> [] end

      assert {:error, %Error{error: :empty_response}} =
               ResponseHelpers.parse_response(response, field_extractor)
    end

    test "returns error when partition has error code" do
      response = %{
        responses: [
          %{
            topic: "test-topic",
            partition_responses: [%{partition: 0, base_offset: -1, error_code: 3}]
          }
        ]
      }

      field_extractor = fn _, _ -> [] end

      assert {:error, %Error{error: :unknown_topic_or_partition}} =
               ResponseHelpers.parse_response(response, field_extractor)
    end
  end
end
