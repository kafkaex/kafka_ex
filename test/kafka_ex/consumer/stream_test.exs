defmodule KafkaEx.Consumer.StreamTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Consumer.Stream

  describe "struct" do
    test "has default values" do
      stream = %Stream{}

      assert stream.client == nil
      assert stream.topic == nil
      assert stream.partition == nil
      assert stream.offset == 0
      assert stream.consumer_group == nil
      assert stream.no_wait_at_logend == false
      assert stream.fetch_options == []
      assert stream.api_versions == %{fetch: 0, offset_fetch: 0, offset_commit: 0}
    end

    test "can be initialized with values" do
      stream = %Stream{
        client: :test_client,
        topic: "test-topic",
        partition: 0,
        offset: 100,
        consumer_group: "test-group",
        no_wait_at_logend: true,
        fetch_options: [max_bytes: 1024],
        api_versions: %{fetch: 3, offset_fetch: 1, offset_commit: 2}
      }

      assert stream.client == :test_client
      assert stream.topic == "test-topic"
      assert stream.partition == 0
      assert stream.offset == 100
      assert stream.consumer_group == "test-group"
      assert stream.no_wait_at_logend == true
      assert stream.fetch_options == [max_bytes: 1024]
      assert stream.api_versions == %{fetch: 3, offset_fetch: 1, offset_commit: 2}
    end
  end

  describe "Enumerable protocol" do
    test "count returns error" do
      stream = %Stream{}

      assert Enumerable.count(stream) == {:error, Enumerable.KafkaEx.Consumer.Stream}
    end

    test "member? returns error" do
      stream = %Stream{}

      assert Enumerable.member?(stream, :any) == {:error, Enumerable.KafkaEx.Consumer.Stream}
    end

    test "slice returns error" do
      stream = %Stream{}

      assert Enumerable.slice(stream) == {:error, Enumerable.KafkaEx.Consumer.Stream}
    end
  end
end
