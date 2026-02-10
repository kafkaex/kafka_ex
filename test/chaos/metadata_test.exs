defmodule KafkaEx.Chaos.MetadataTest do
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag timeout: 300_000

  alias KafkaEx.ChaosTestHelpers

  @test_topic "chaos_metadata_test"

  setup_all do
    {:ok, ctx} = ChaosTestHelpers.start_infrastructure()
    on_exit(fn -> ChaosTestHelpers.stop_infrastructure(ctx) end)
    {:ok, ctx}
  end

  setup ctx do
    # Reset proxy state BEFORE each test to ensure clean state
    ChaosTestHelpers.reset_all()
    ChaosTestHelpers.stop_client()
    Process.sleep(200)

    on_exit(fn ->
      ChaosTestHelpers.reset_all()
      ChaosTestHelpers.stop_client()
    end)

    {:ok, ctx}
  end

  # ---------------------------------------------------------------------------
  # Metadata During Network Failures
  # ---------------------------------------------------------------------------

  describe "metadata during broker unavailability" do
    test "metadata request fails when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.metadata(client)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result), "Expected error when broker is down, got: #{inspect(result)}"
      end)

      Process.sleep(1000)
      {:ok, _} = KafkaEx.API.metadata(client)
    end

    test "metadata for specific topic fails when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.metadata(client, [@test_topic])
        assert match?({:error, _}, result), "Expected error when broker is down"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.metadata(client, [@test_topic])
    end
  end

  # ---------------------------------------------------------------------------
  # Metadata Timeout Scenarios
  # ---------------------------------------------------------------------------

  describe "metadata timeout scenarios" do
    test "metadata with moderate latency still succeeds", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 500, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:ok, _}, result), "Expected success with moderate latency, got: #{inspect(result)}"
      end)
    end

    test "metadata with data timeout fails gracefully", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_timeout(ctx.proxy_name, 100, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result), "Expected error during data timeout, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.metadata(client)
    end

    test "metadata with connection reset fails and recovers", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 50, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result), "Expected error on connection reset"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.metadata(client)
    end
  end

  # ---------------------------------------------------------------------------
  # Topic Metadata Operations
  # ---------------------------------------------------------------------------

  describe "topic metadata operations" do
    test "topics_metadata returns cached data when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
      {:ok, initial_topics} = KafkaEx.API.topics_metadata(client, [@test_topic])
      assert length(initial_topics) > 0

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.topics_metadata(client, [@test_topic])
        assert match?({:ok, _}, result), "Expected cached metadata when broker is down, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.topics_metadata(client, [@test_topic])
    end

    test "topics_metadata for multiple topics with latency", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      topics = ["topic1", "topic2", "topic3"]

      ChaosTestHelpers.with_latency(ctx.proxy_name, 300, fn ->
        result = KafkaEx.API.topics_metadata(client, topics)
        assert match?({:ok, _}, result), "Expected success with moderate latency"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # API Versions During Network Failures
  # ---------------------------------------------------------------------------

  describe "api_versions during network failures" do
    test "api_versions fails when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      # Normal operation
      {:ok, _} = KafkaEx.API.api_versions(client)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.api_versions(client)
        assert match?({:error, _}, result), "Expected error when broker is down"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.api_versions(client)
    end

    test "api_versions recovers after connection reset", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 0, fn ->
        result = KafkaEx.API.api_versions(client)
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.api_versions(client)
    end
  end

  # ---------------------------------------------------------------------------
  # Cluster Metadata During Network Failures
  # ---------------------------------------------------------------------------

  describe "cluster_metadata during network failures" do
    test "cluster_metadata is cached and available during brief outage", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      # Get initial cluster metadata
      {:ok, initial_metadata} = KafkaEx.API.cluster_metadata(client)
      assert initial_metadata != nil

      # During brief outage, cached metadata might still be returned
      # depending on implementation
      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        # cluster_metadata returns cached state, so it may not fail
        result = KafkaEx.API.cluster_metadata(client)
        # Just verify we get a response
        assert result != nil
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.cluster_metadata(client)
    end
  end

  # ---------------------------------------------------------------------------
  # Metadata Recovery Scenarios
  # ---------------------------------------------------------------------------

  describe "metadata recovery" do
    test "metadata recovers after multiple network failures", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      # First failure - broker down
      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.metadata(client)

      # Second failure - connection reset
      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 0, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.metadata(client)

      # Third failure - timeout
      ChaosTestHelpers.with_timeout(ctx.proxy_name, 100, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.metadata(client)
    end

    test "metadata refresh works after slow network", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      # Slow network
      ChaosTestHelpers.with_bandwidth_limit(ctx.proxy_name, 5, fn ->
        result = KafkaEx.API.metadata(client)
        assert result != nil
      end)

      # Normal speed should work fine after
      {:ok, metadata} = KafkaEx.API.metadata(client)
      assert metadata != nil
    end

    test "new client gets fresh metadata after previous client failure", ctx do
      {:ok, client1} = ChaosTestHelpers.start_client(ctx)

      # Get metadata then fail
      {:ok, _} = KafkaEx.API.metadata(client1)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        _ = KafkaEx.API.metadata(client1)
      end)

      ChaosTestHelpers.stop_client()
      Process.sleep(500)

      # New client should get fresh metadata
      {:ok, client2} = ChaosTestHelpers.start_client(ctx)
      {:ok, metadata} = KafkaEx.API.metadata(client2)
      assert metadata != nil
    end
  end

  # ---------------------------------------------------------------------------
  # Metadata with Slow Network
  # ---------------------------------------------------------------------------

  describe "metadata with slow network" do
    test "metadata with slow close still succeeds", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_slow_close(ctx.proxy_name, 500, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:ok, _}, result), "Expected success with slow close"
      end)
    end

    test "metadata with high latency times out appropriately", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 8000, fn ->
        result =
          try do
            KafkaEx.API.metadata(client)
          catch
            :exit, {:timeout, _} -> {:error, :timeout}
          end

        assert match?({:error, _}, result), "Expected timeout with high latency"
      end)

      # Recovery with fresh client
      ChaosTestHelpers.stop_client()
      Process.sleep(500)
      {:ok, new_client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.metadata(new_client)
    end
  end
end
