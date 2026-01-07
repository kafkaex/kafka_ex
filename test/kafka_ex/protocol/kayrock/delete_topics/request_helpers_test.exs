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
end
