defmodule KafkaEx.New.Protocols.Kayrock.DeleteTopics.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.DeleteTopics
  alias KafkaEx.New.Protocols.Kayrock.DeleteTopics.RequestHelpers

  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts required fields" do
      opts = [
        topics: ["topic1", "topic2"],
        timeout: 10_000
      ]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.topics == ["topic1", "topic2"]
      assert result.timeout == 10_000
    end

    test "raises when topics is missing" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(timeout: 10_000)
      end
    end

    test "raises when timeout is missing" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(topics: ["test"])
      end
    end
  end

  describe "V0 Request implementation" do
    test "builds request with all fields" do
      request = %Kayrock.DeleteTopics.V0.Request{}

      opts = [
        topics: ["topic1", "topic2", "topic3"],
        timeout: 30_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert %Kayrock.DeleteTopics.V0.Request{} = result
      assert result.timeout == 30_000
      assert result.topics == ["topic1", "topic2", "topic3"]
    end

    test "builds request with single topic" do
      request = %Kayrock.DeleteTopics.V0.Request{}

      opts = [
        topics: ["my-topic"],
        timeout: 10_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert result.topics == ["my-topic"]
      assert result.timeout == 10_000
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.DeleteTopics.V0.Request{
        correlation_id: 42,
        client_id: "test-client"
      }

      opts = [
        topics: ["test-topic"],
        timeout: 10_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert result.correlation_id == 42
      assert result.client_id == "test-client"
    end
  end

  describe "V1 Request implementation" do
    test "builds request with all fields (same as V0)" do
      request = %Kayrock.DeleteTopics.V1.Request{}

      opts = [
        topics: ["topic1", "topic2"],
        timeout: 60_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert %Kayrock.DeleteTopics.V1.Request{} = result
      assert result.timeout == 60_000
      assert result.topics == ["topic1", "topic2"]
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.DeleteTopics.V1.Request{
        correlation_id: 123,
        client_id: "my-client"
      }

      opts = [
        topics: ["test-topic"],
        timeout: 10_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert result.correlation_id == 123
      assert result.client_id == "my-client"
    end
  end
end
