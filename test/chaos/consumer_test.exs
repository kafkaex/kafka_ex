defmodule KafkaEx.Chaos.ConsumerTest do
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag timeout: 300_000

  alias KafkaEx.ChaosTestHelpers

  @test_topic "chaos_consumer_test"

  setup_all do
    {:ok, ctx} = ChaosTestHelpers.start_infrastructure()
    {:ok, client} = ChaosTestHelpers.start_client(ctx)

    messages = Enum.map(1..20, fn i -> %{key: "key_#{i}", value: "value_#{i}"} end)
    {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, messages)

    ChaosTestHelpers.stop_client()
    Process.sleep(500)

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
  # Fetch During Network Failures
  # ---------------------------------------------------------------------------

  describe "fetch during broker unavailability" do
    test "fetch fails when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.fetch(client, @test_topic, 0, 0)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, 0)
        assert match?({:error, _}, result), "Expected error when broker is down, got: #{inspect(result)}"
      end)

      Process.sleep(1000)
      {:ok, _} = KafkaEx.API.fetch(client, @test_topic, 0, 0)
    end

    test "fetch fails with connection reset", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 50, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, 0)
        assert match?({:error, _}, result), "Expected error on connection reset, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.fetch(client, @test_topic, 0, 0)
    end
  end

  # ---------------------------------------------------------------------------
  # Fetch Timeout Scenarios
  # ---------------------------------------------------------------------------

  describe "fetch timeout scenarios" do
    test "fetch with moderate latency still succeeds", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 500, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, 0)
        assert match?({:ok, _}, result), "Expected success with moderate latency, got: #{inspect(result)}"
      end)
    end

    test "fetch with data timeout fails gracefully", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_timeout(ctx.proxy_name, 100, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, 0)
        assert match?({:error, _}, result), "Expected error during data timeout, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.fetch(client, @test_topic, 0, 0)
    end
  end

  # ---------------------------------------------------------------------------
  # Fetch with Different Options
  # ---------------------------------------------------------------------------

  describe "fetch with different options under network stress" do
    test "fetch with max_bytes limit works under bandwidth constraint", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_bandwidth_limit(ctx.proxy_name, 10, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, 0, max_bytes: 1024)
        assert result != nil, "Expected fetch to complete under bandwidth limit"
      end)
    end

    test "fetch from specific offset works after network recovery", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, fetch_result} = KafkaEx.API.fetch(client, @test_topic, 0, 0)

      # Fail and recover
      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        _ = KafkaEx.API.fetch(client, @test_topic, 0, 0)
      end)

      Process.sleep(500)

      {:ok, result} = KafkaEx.API.fetch(client, @test_topic, 0, 5)
      assert result != nil
    end
  end

  # ---------------------------------------------------------------------------
  # Offset Operations During Network Failures
  # ---------------------------------------------------------------------------

  describe "offset operations during network failures" do
    test "earliest_offset fails when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.earliest_offset(client, @test_topic, 0)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.earliest_offset(client, @test_topic, 0)
        assert match?({:error, _}, result), "Expected error when broker is down"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.earliest_offset(client, @test_topic, 0)
    end

    test "latest_offset works after recovery", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 0, fn ->
        result = KafkaEx.API.latest_offset(client, @test_topic, 0)
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, result} = KafkaEx.API.latest_offset(client, @test_topic, 0)
      assert result != nil
    end
  end

  # ---------------------------------------------------------------------------
  # Consumer Recovery Scenarios
  # ---------------------------------------------------------------------------

  describe "consumer recovery" do
    test "consumer recovers after multiple network failures", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, 0)
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.fetch(client, @test_topic, 0, 0)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 0, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, 0)
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.fetch(client, @test_topic, 0, 0)
    end

    test "fetch resumes after network failure", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.fetch(client, @test_topic, 0, 0)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, 0)
        assert match?({:error, _}, result)
      end)

      Process.sleep(1000)
      {:ok, result} = KafkaEx.API.fetch(client, @test_topic, 0, 0)
      assert result != nil
    end
  end

  # ---------------------------------------------------------------------------
  # Consumer with Slow Network
  # ---------------------------------------------------------------------------

  describe "consumer with slow network" do
    test "fetch with slow close still returns data", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_slow_close(ctx.proxy_name, 500, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, 0)
        assert match?({:ok, _}, result), "Expected success with slow close, got: #{inspect(result)}"
      end)
    end

    test "fetch with high latency times out appropriately", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 8000, fn ->
        result =
          try do
            KafkaEx.API.fetch(client, @test_topic, 0, 0)
          catch
            :exit, {:timeout, _} -> {:error, :timeout}
          end

        assert match?({:error, _}, result), "Expected timeout with high latency"
      end)

      ChaosTestHelpers.stop_client()
      Process.sleep(500)
      {:ok, new_client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.fetch(new_client, @test_topic, 0, 0)
    end
  end
end
