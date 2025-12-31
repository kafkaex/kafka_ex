defmodule KafkaEx.APITest do
  @moduledoc """
  Tests for the KafkaEx.API module.
  """

  use ExUnit.Case, async: true

  describe "module structure" do
    test "KafkaEx.API module exists" do
      assert Code.ensure_loaded?(KafkaEx.API)
    end

    test "KafkaEx.API.Behaviour module exists" do
      assert Code.ensure_loaded?(KafkaEx.API.Behaviour)
    end

    test "defines expected functions" do
      # Offset functions
      assert function_exported?(KafkaEx.API, :latest_offset, 3)
      assert function_exported?(KafkaEx.API, :latest_offset, 4)
      assert function_exported?(KafkaEx.API, :earliest_offset, 3)
      assert function_exported?(KafkaEx.API, :earliest_offset, 4)
      assert function_exported?(KafkaEx.API, :list_offsets, 2)
      assert function_exported?(KafkaEx.API, :list_offsets, 3)

      # Metadata functions
      assert function_exported?(KafkaEx.API, :metadata, 1)
      assert function_exported?(KafkaEx.API, :metadata, 2)
      assert function_exported?(KafkaEx.API, :cluster_metadata, 1)
      assert function_exported?(KafkaEx.API, :topics_metadata, 2)
      assert function_exported?(KafkaEx.API, :topics_metadata, 3)
      assert function_exported?(KafkaEx.API, :api_versions, 1)
      assert function_exported?(KafkaEx.API, :api_versions, 2)

      # Produce functions
      assert function_exported?(KafkaEx.API, :produce, 4)
      assert function_exported?(KafkaEx.API, :produce, 5)
      assert function_exported?(KafkaEx.API, :produce_one, 4)
      assert function_exported?(KafkaEx.API, :produce_one, 5)

      # Fetch functions
      assert function_exported?(KafkaEx.API, :fetch, 4)
      assert function_exported?(KafkaEx.API, :fetch, 5)
      assert function_exported?(KafkaEx.API, :fetch_all, 3)
      assert function_exported?(KafkaEx.API, :fetch_all, 4)

      # Consumer group functions
      assert function_exported?(KafkaEx.API, :describe_group, 2)
      assert function_exported?(KafkaEx.API, :describe_group, 3)
      assert function_exported?(KafkaEx.API, :join_group, 3)
      assert function_exported?(KafkaEx.API, :join_group, 4)
      assert function_exported?(KafkaEx.API, :sync_group, 4)
      assert function_exported?(KafkaEx.API, :sync_group, 5)
      assert function_exported?(KafkaEx.API, :leave_group, 3)
      assert function_exported?(KafkaEx.API, :leave_group, 4)
      assert function_exported?(KafkaEx.API, :heartbeat, 4)
      assert function_exported?(KafkaEx.API, :heartbeat, 5)
      assert function_exported?(KafkaEx.API, :find_coordinator, 2)
      assert function_exported?(KafkaEx.API, :find_coordinator, 3)

      # Offset management functions
      assert function_exported?(KafkaEx.API, :fetch_committed_offset, 4)
      assert function_exported?(KafkaEx.API, :fetch_committed_offset, 5)
      assert function_exported?(KafkaEx.API, :commit_offset, 4)
      assert function_exported?(KafkaEx.API, :commit_offset, 5)

      # Topic management functions
      assert function_exported?(KafkaEx.API, :create_topics, 3)
      assert function_exported?(KafkaEx.API, :create_topics, 4)
      assert function_exported?(KafkaEx.API, :create_topic, 2)
      assert function_exported?(KafkaEx.API, :create_topic, 3)
      assert function_exported?(KafkaEx.API, :delete_topics, 3)
      assert function_exported?(KafkaEx.API, :delete_topics, 4)
      assert function_exported?(KafkaEx.API, :delete_topic, 2)
      assert function_exported?(KafkaEx.API, :delete_topic, 3)

      # Lifecycle functions
      assert function_exported?(KafkaEx.API, :start_client, 0)
      assert function_exported?(KafkaEx.API, :start_client, 1)
      assert function_exported?(KafkaEx.API, :child_spec, 1)
    end
  end

  describe "use macro" do
    defmodule TestKafkaWithClient do
      use KafkaEx.API, client: :test_kafka_client
    end

    defmodule TestKafkaWithOverride do
      use KafkaEx.API

      def client do
        :overridden_client
      end
    end

    test "injected client/0 returns configured client" do
      assert TestKafkaWithClient.client() == :test_kafka_client
    end

    test "client/0 can be overridden" do
      assert TestKafkaWithOverride.client() == :overridden_client
    end

    test "injected functions are defined" do
      # Check that the mixin functions are defined
      assert function_exported?(TestKafkaWithClient, :client, 0)
      assert function_exported?(TestKafkaWithClient, :latest_offset, 2)
      assert function_exported?(TestKafkaWithClient, :latest_offset, 3)
      assert function_exported?(TestKafkaWithClient, :produce, 3)
      assert function_exported?(TestKafkaWithClient, :produce, 4)
      assert function_exported?(TestKafkaWithClient, :fetch, 3)
      assert function_exported?(TestKafkaWithClient, :fetch, 4)
      assert function_exported?(TestKafkaWithClient, :metadata, 0)
      assert function_exported?(TestKafkaWithClient, :metadata, 1)
    end
  end

  describe "use macro without client option" do
    defmodule TestKafkaNoClient do
      use KafkaEx.API
    end

    test "raises error when client/0 not implemented" do
      assert_raise RuntimeError, ~r/must implement client\/0/, fn ->
        TestKafkaNoClient.client()
      end
    end
  end

  describe "child_spec/1" do
    test "returns valid child spec" do
      spec = KafkaEx.API.child_spec(name: TestClient, brokers: [{"localhost", 9092}])

      assert spec.id == TestClient
      assert spec.type == :worker
      assert spec.restart == :permanent
      assert {KafkaEx.Client, :start_link, [opts]} = spec.start
      assert opts[:name] == TestClient
      assert opts[:brokers] == [{"localhost", 9092}]
    end

    test "uses module name as default id" do
      spec = KafkaEx.API.child_spec(brokers: [{"localhost", 9092}])

      assert spec.id == KafkaEx.API
    end
  end
end
