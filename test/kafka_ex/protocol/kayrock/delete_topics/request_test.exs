defmodule KafkaEx.Protocol.Kayrock.DeleteTopics.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.DeleteTopics
  alias KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers

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
      assert result.timeout_ms == 30_000
      assert result.topic_names == ["topic1", "topic2", "topic3"]
    end

    test "builds request with single topic" do
      request = %Kayrock.DeleteTopics.V0.Request{}

      opts = [
        topics: ["my-topic"],
        timeout: 10_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert result.topic_names == ["my-topic"]
      assert result.timeout_ms == 10_000
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
      assert result.timeout_ms == 60_000
      assert result.topic_names == ["topic1", "topic2"]
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

  describe "V2 Request implementation" do
    test "builds request with all fields (identical to V0/V1)" do
      request = %Kayrock.DeleteTopics.V2.Request{}

      opts = [
        topics: ["topic-a", "topic-b"],
        timeout: 20_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert %Kayrock.DeleteTopics.V2.Request{} = result
      assert result.timeout_ms == 20_000
      assert result.topic_names == ["topic-a", "topic-b"]
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.DeleteTopics.V2.Request{
        correlation_id: 200,
        client_id: "v2-client"
      }

      opts = [
        topics: ["test-topic"],
        timeout: 5_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert result.correlation_id == 200
      assert result.client_id == "v2-client"
    end
  end

  describe "V3 Request implementation" do
    test "builds request with all fields (identical to V0-V2)" do
      request = %Kayrock.DeleteTopics.V3.Request{}

      opts = [
        topics: ["topic-x"],
        timeout: 45_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert %Kayrock.DeleteTopics.V3.Request{} = result
      assert result.timeout_ms == 45_000
      assert result.topic_names == ["topic-x"]
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.DeleteTopics.V3.Request{
        correlation_id: 300,
        client_id: "v3-client"
      }

      opts = [
        topics: ["test-topic"],
        timeout: 10_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert result.correlation_id == 300
      assert result.client_id == "v3-client"
    end
  end

  describe "V4 Request implementation (FLEX)" do
    test "builds request with all fields" do
      request = %Kayrock.DeleteTopics.V4.Request{}

      opts = [
        topics: ["flex-topic-1", "flex-topic-2"],
        timeout: 15_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert %Kayrock.DeleteTopics.V4.Request{} = result
      assert result.timeout_ms == 15_000
      assert result.topic_names == ["flex-topic-1", "flex-topic-2"]
    end

    test "preserves tagged_fields default" do
      request = %Kayrock.DeleteTopics.V4.Request{}

      opts = [
        topics: ["test-topic"],
        timeout: 10_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      # V4 struct has tagged_fields field; ensure it is preserved
      assert Map.has_key?(result, :tagged_fields)
    end

    test "preserves existing correlation_id and client_id" do
      request = %Kayrock.DeleteTopics.V4.Request{
        correlation_id: 400,
        client_id: "v4-client"
      }

      opts = [
        topics: ["test-topic"],
        timeout: 10_000
      ]

      result = DeleteTopics.Request.build_request(request, opts)

      assert result.correlation_id == 400
      assert result.client_id == "v4-client"
    end
  end

  describe "cross-version consistency V0-V4" do
    @versions [
      {Kayrock.DeleteTopics.V0.Request, "V0"},
      {Kayrock.DeleteTopics.V1.Request, "V1"},
      {Kayrock.DeleteTopics.V2.Request, "V2"},
      {Kayrock.DeleteTopics.V3.Request, "V3"},
      {Kayrock.DeleteTopics.V4.Request, "V4"}
    ]

    for {mod, label} <- @versions do
      test "#{label} sets topic_names and timeout_ms" do
        request = struct(unquote(mod))
        opts = [topics: ["test-topic"], timeout: 15_000]

        result = DeleteTopics.Request.build_request(request, opts)

        assert result.topic_names == ["test-topic"]
        assert result.timeout_ms == 15_000
      end

      test "#{label} handles multiple topics" do
        request = struct(unquote(mod))
        topics = ["topic-1", "topic-2", "topic-3", "topic-4", "topic-5"]
        opts = [topics: topics, timeout: 30_000]

        result = DeleteTopics.Request.build_request(request, opts)

        assert result.topic_names == topics
        assert result.timeout_ms == 30_000
      end

      test "#{label} raises on missing topics" do
        request = struct(unquote(mod))

        assert_raise KeyError, fn ->
          DeleteTopics.Request.build_request(request, timeout: 10_000)
        end
      end

      test "#{label} raises on missing timeout" do
        request = struct(unquote(mod))

        assert_raise KeyError, fn ->
          DeleteTopics.Request.build_request(request, topics: ["test"])
        end
      end
    end
  end

  describe "Any fallback request implementation" do
    test "handles a plain map with topic_names and timeout_ms" do
      # Simulates an unknown future version struct
      request_template = %{topic_names: nil, timeout_ms: nil, client_id: "any-client"}
      opts = [topics: ["any-topic"], timeout: 25_000]

      result = DeleteTopics.Request.build_request(request_template, opts)

      assert result.topic_names == ["any-topic"]
      assert result.timeout_ms == 25_000
      assert result.client_id == "any-client"
    end

    test "handles a map with tagged_fields (V4-like)" do
      request_template = %{
        topic_names: nil,
        timeout_ms: nil,
        tagged_fields: [],
        client_id: "flex-any"
      }

      opts = [topics: ["flex-topic"], timeout: 12_000]

      result = DeleteTopics.Request.build_request(request_template, opts)

      assert result.topic_names == ["flex-topic"]
      assert result.timeout_ms == 12_000
      assert result.tagged_fields == []
    end
  end
end
