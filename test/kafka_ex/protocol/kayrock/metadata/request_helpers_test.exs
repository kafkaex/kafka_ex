defmodule KafkaEx.Protocol.Kayrock.Metadata.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  describe "build_topics_list/1" do
    test "returns nil for missing topics option" do
      assert RequestHelpers.build_topics_list([]) == nil
    end

    test "returns nil for nil topics" do
      assert RequestHelpers.build_topics_list(topics: nil) == nil
    end

    test "returns nil for empty topics list" do
      assert RequestHelpers.build_topics_list(topics: []) == nil
    end

    test "returns topics list when provided" do
      assert RequestHelpers.build_topics_list(topics: ["topic1", "topic2"]) == [%{name: "topic1"}, %{name: "topic2"}]
    end

    test "returns single topic list" do
      assert RequestHelpers.build_topics_list(topics: ["my-topic"]) == [%{name: "my-topic"}]
    end

    test "passes through already-wrapped topic maps" do
      assert RequestHelpers.build_topics_list(topics: [%{name: "already-wrapped"}]) == [%{name: "already-wrapped"}]
    end
  end

  describe "default_api_version/0" do
    test "returns 1" do
      assert RequestHelpers.default_api_version() == 1
    end
  end

  describe "allow_auto_topic_creation?/1" do
    test "defaults to false" do
      assert RequestHelpers.allow_auto_topic_creation?([]) == false
    end

    test "returns true when explicitly set" do
      assert RequestHelpers.allow_auto_topic_creation?(allow_auto_topic_creation: true) == true
    end

    test "returns false when explicitly set" do
      assert RequestHelpers.allow_auto_topic_creation?(allow_auto_topic_creation: false) == false
    end
  end
end
