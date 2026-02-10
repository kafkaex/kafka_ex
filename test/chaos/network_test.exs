defmodule KafkaEx.Chaos.NetworkTest do
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag timeout: 300_000

  alias KafkaEx.ChaosTestHelpers

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
  # Broker Unavailability Tests
  # ---------------------------------------------------------------------------

  describe "broker unavailability" do
    test "client handles broker down and recovers", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(client)

      # Simulate broker down (connection refused)
      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result), "Expected error when broker is down, got: #{inspect(result)}"
      end)

      Process.sleep(1000)
      {:ok, _metadata} = KafkaEx.API.metadata(client)
    end
  end

  # ---------------------------------------------------------------------------
  # Connection Timeout Tests
  # ---------------------------------------------------------------------------

  describe "connection timeout" do
    test "client handles connection timeout (data stops flowing)", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(client)

      # Simulate timeout - connection stays open but data stops after 100ms
      ChaosTestHelpers.with_timeout(ctx.proxy_name, 100, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result), "Expected error during timeout, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _metadata} = KafkaEx.API.metadata(client)
    end

    test "client times out with high latency exceeding request timeout", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(client)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 10_000, fn ->
        # High latency causes GenServer.call to exit with timeout
        # We catch this exit and verify it's a timeout
        result =
          try do
            KafkaEx.API.metadata(client)
          catch
            :exit, {:timeout, _} -> {:error, :timeout}
          end

        assert match?({:error, _}, result), "Expected timeout error with high latency, got: #{inspect(result)}"
      end)

      # Client may be in a bad state after timeout, start a fresh one
      ChaosTestHelpers.stop_client()
      Process.sleep(500)
      {:ok, new_client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(new_client)
    end
  end

  # ---------------------------------------------------------------------------
  # Connection Reset Tests (TCP RST)
  # ---------------------------------------------------------------------------

  describe "connection reset" do
    test "client handles TCP reset (RST)", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(client)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 50, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result), "Expected error on connection reset, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _metadata} = KafkaEx.API.metadata(client)
    end

    test "client handles immediate connection reset", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(client)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 0, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:error, _}, result), "Expected error on immediate reset, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _metadata} = KafkaEx.API.metadata(client)
    end
  end

  # ---------------------------------------------------------------------------
  # Latency Tests
  # ---------------------------------------------------------------------------

  describe "network latency" do
    test "client handles moderate latency gracefully", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(client)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 500, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:ok, _}, result), "Expected success with moderate latency, got: #{inspect(result)}"
      end)
    end

    test "client handles latency spike", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.metadata(client)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 2000, fn ->
        result = KafkaEx.API.metadata(client)
        assert result != nil, "Expected a response (success or error)"
      end)

      {:ok, _metadata} = KafkaEx.API.metadata(client)
    end
  end

  # ---------------------------------------------------------------------------
  # Bandwidth Throttling Tests
  # ---------------------------------------------------------------------------

  describe "bandwidth throttling" do
    test "client handles severely throttled bandwidth", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(client)

      ChaosTestHelpers.with_bandwidth_limit(ctx.proxy_name, 1, fn ->
        result = KafkaEx.API.metadata(client)
        assert result != nil, "Expected a response under bandwidth limit"
      end)

      {:ok, _metadata} = KafkaEx.API.metadata(client)
    end
  end

  # ---------------------------------------------------------------------------
  # Slow Close Tests
  # ---------------------------------------------------------------------------

  describe "slow close" do
    test "client handles delayed connection close", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(client)

      ChaosTestHelpers.with_slow_close(ctx.proxy_name, 1000, fn ->
        result = KafkaEx.API.metadata(client)
        assert match?({:ok, _}, result), "Expected success with slow close, got: #{inspect(result)}"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent Operations Under Failure
  # ---------------------------------------------------------------------------

  describe "concurrent operations under failure" do
    test "concurrent API calls get clean errors during broker failure", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.metadata(client)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        tasks =
          Enum.map(1..10, fn _i ->
            Task.async(fn -> KafkaEx.API.metadata(client) end)
          end)

        results = Task.await_many(tasks, 15_000)

        # All tasks should complete with error (not hang, not crash)
        Enum.each(results, fn result ->
          assert match?({:error, _}, result),
                 "Concurrent call should get error, got: #{inspect(result)}"
        end)
      end)

      # Client should still be alive and recover
      Process.sleep(1_000)
      assert Process.alive?(client)
      {:ok, _} = KafkaEx.API.metadata(client)
    end
  end

  # ---------------------------------------------------------------------------
  # Rapid Reconnection Cycling
  # ---------------------------------------------------------------------------

  describe "rapid reconnection cycling" do
    test "client survives 10 rapid disconnect/reconnect cycles", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.metadata(client)

      Enum.each(1..10, fn i ->
        ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
          _ = KafkaEx.API.metadata(client)
        end)

        Process.sleep(500)
        {:ok, _} = KafkaEx.API.metadata(client)
        assert Process.alive?(client), "Client died after cycle #{i}"
      end)

      # Final verification - client is healthy
      {:ok, metadata} = KafkaEx.API.metadata(client)
      assert metadata != nil
    end
  end

  # ---------------------------------------------------------------------------
  # Combined/Complex Scenarios
  # ---------------------------------------------------------------------------

  describe "complex network scenarios" do
    test "new client can connect after network recovers", ctx do
      # While broker is down, client creation should fail (can't negotiate API versions)
      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        # Trap exits to catch the linked process crash during GenServer.init
        old_trap = Process.flag(:trap_exit, true)

        result =
          try do
            ChaosTestHelpers.start_client(ctx)
          catch
            # Client init raises when it can't connect to any broker
            :exit, _ -> {:error, :connection_failed}
          after
            # Drain any exit messages
            receive do
              {:EXIT, _, _} -> :ok
            after
              100 -> :ok
            end

            Process.flag(:trap_exit, old_trap)
          end

        assert match?({:error, _}, result), "Expected connection to fail when broker is down, got: #{inspect(result)}"
      end)

      # After broker recovers, new client should connect successfully
      Process.sleep(500)
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _metadata} = KafkaEx.API.metadata(client)
    end
  end
end
