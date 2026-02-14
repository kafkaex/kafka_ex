defmodule KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers

  describe "extract_common_fields/1" do
    test "extracts topics and timeout" do
      opts = [topics: ["topic1", "topic2"], timeout: 30_000]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.topics == ["topic1", "topic2"]
      assert result.timeout == 30_000
    end

    test "handles single topic" do
      opts = [topics: ["only-topic"], timeout: 5_000]

      result = RequestHelpers.extract_common_fields(opts)

      assert result.topics == ["only-topic"]
    end

    test "raises on missing topics" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(timeout: 30_000)
      end
    end

    test "raises on missing timeout" do
      assert_raise KeyError, fn ->
        RequestHelpers.extract_common_fields(topics: ["topic"])
      end
    end
  end

  describe "build_request_from_template/2" do
    test "populates topic_names and timeout_ms on template" do
      template = %{topic_names: nil, timeout_ms: nil, client_id: "test"}
      opts = [topics: ["topic-a", "topic-b"], timeout: 20_000]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert result.topic_names == ["topic-a", "topic-b"]
      assert result.timeout_ms == 20_000
      assert result.client_id == "test"
    end

    test "works with Kayrock V0 struct" do
      template = %Kayrock.DeleteTopics.V0.Request{}
      opts = [topics: ["t1"], timeout: 10_000]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert %Kayrock.DeleteTopics.V0.Request{} = result
      assert result.topic_names == ["t1"]
      assert result.timeout_ms == 10_000
    end

    test "works with Kayrock V4 struct (FLEX)" do
      template = %Kayrock.DeleteTopics.V4.Request{}
      opts = [topics: ["flex-t1"], timeout: 15_000]

      result = RequestHelpers.build_request_from_template(template, opts)

      assert %Kayrock.DeleteTopics.V4.Request{} = result
      assert result.topic_names == ["flex-t1"]
      assert result.timeout_ms == 15_000
      assert Map.has_key?(result, :tagged_fields)
    end

    test "raises on missing topics" do
      template = %{topic_names: nil, timeout_ms: nil}

      assert_raise KeyError, fn ->
        RequestHelpers.build_request_from_template(template, timeout: 10_000)
      end
    end

    test "raises on missing timeout" do
      template = %{topic_names: nil, timeout_ms: nil}

      assert_raise KeyError, fn ->
        RequestHelpers.build_request_from_template(template, topics: ["t"])
      end
    end
  end
end
