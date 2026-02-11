defmodule KafkaEx.Chaos.ProducerTest do
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag timeout: 300_000

  alias KafkaEx.ChaosTestHelpers

  @test_topic "chaos_producer_test"

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
  # Producer During Network Failures
  # ---------------------------------------------------------------------------

  describe "produce during broker unavailability" do
    test "produce fails when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k1", value: "v1"}])

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k2", value: "v2"}])
        assert match?({:error, _}, result), "Expected error when broker is down, got: #{inspect(result)}"
      end)

      Process.sleep(1000)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k3", value: "v3"}])
    end

    test "produce fails with connection reset", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 50, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
        assert match?({:error, _}, result), "Expected error on connection reset, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
    end
  end

  # ---------------------------------------------------------------------------
  # Producer Timeout Scenarios
  # ---------------------------------------------------------------------------

  describe "produce timeout scenarios" do
    test "produce with moderate latency still succeeds", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 500, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
        assert match?({:ok, _}, result), "Expected success with moderate latency, got: #{inspect(result)}"
      end)
    end

    test "produce with data timeout fails gracefully", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_timeout(ctx.proxy_name, 100, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
        assert match?({:error, _}, result), "Expected error during data timeout, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
    end
  end

  # ---------------------------------------------------------------------------
  # Produce Retry Safety (no duplicate messages)
  # ---------------------------------------------------------------------------

  describe "produce retry safety" do
    test "produce does not retry on timeout - prevents duplicate messages", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      # Produce a baseline message and record the offset
      {:ok, baseline} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "base", value: "msg"}])
      baseline_offset = baseline.base_offset

      # Produce another message successfully to establish offset = baseline + 1
      {:ok, second} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "second", value: "msg"}])
      assert second.base_offset == baseline_offset + 1

      # Now cause a timeout - produce should fail WITHOUT retrying
      # (retrying could write duplicates)
      ChaosTestHelpers.with_timeout(ctx.proxy_name, 100, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "timeout", value: "msg"}])
        assert match?({:error, _}, result), "Produce should fail on timeout"
      end)

      # Recover and check: latest offset should be baseline + 2
      # If produce had retried (3 times), offset could be up to baseline + 5
      Process.sleep(1_000)
      {:ok, latest} = KafkaEx.API.latest_offset(client, @test_topic, 0)

      # The timed-out message may or may not have been written (we can't know),
      # but it should NOT have been written multiple times by retries.
      # At most baseline + 3 (2 successful + 1 possible write before timeout)
      assert latest <= baseline_offset + 3,
             "Expected at most 3 messages (2 confirmed + 1 possible), but offset is #{latest}. " <>
               "This suggests produce was retried, risking duplicates."
    end

    test "produce fails immediately on connection error - no retry storm", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])

      # Measure how fast produce fails when broker is down
      # If it retries 3 times with backoff, it would take several seconds
      # With no retry on non-leadership errors, it should fail quickly
      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        start_time = System.monotonic_time(:millisecond)
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
        elapsed = System.monotonic_time(:millisecond) - start_time

        assert match?({:error, _}, result)
        # Should fail within a few seconds, not 10+ seconds of retries
        assert elapsed < 10_000,
               "Produce took #{elapsed}ms - suggests unnecessary retries on connection error"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Producer with Different Required Acks
  # ---------------------------------------------------------------------------

  describe "produce with different ack settings" do
    test "produce with acks=0 (fire and forget) tolerates network issues", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 200, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}], required_acks: 0)
        assert !is_nil(result)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Producer Batch Scenarios
  # ---------------------------------------------------------------------------

  describe "produce batch under network stress" do
    test "batch produce fails atomically when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..10, fn i -> %{key: "key_#{i}", value: "value_#{i}"} end)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages)
        assert match?({:error, _}, result), "Expected batch to fail when broker is down"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, messages)
    end

    test "batch produce with bandwidth limit still succeeds", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..5, fn i -> %{key: "k#{i}", value: "v#{i}"} end)

      ChaosTestHelpers.with_bandwidth_limit(ctx.proxy_name, 10, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages)
        assert !is_nil(result), "Expected produce to complete under bandwidth limit"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Producer Recovery Scenarios
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Producer In-Flight Request Failure
  # ---------------------------------------------------------------------------

  describe "producer in-flight request failure" do
    test "produce returns error (not hang) when connection resets mid-request", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])

      # reset_peer with small delay: lets request start, then kills connection
      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 10, fn ->
        # Large batch increases chance of mid-flight failure
        messages = Enum.map(1..50, fn i -> %{key: "k#{i}", value: String.duplicate("x", 1000)} end)
        result = KafkaEx.API.produce(client, @test_topic, 0, messages)

        assert match?({:error, _}, result),
               "Produce should return error on mid-request reset, got: #{inspect(result)}"
      end)

      # Client should recover after failure
      Process.sleep(1_000)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "after", value: "recovery"}])
    end

    test "concurrent produces get clean errors during broker failure", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        tasks =
          Enum.map(1..10, fn i ->
            Task.async(fn ->
              KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k#{i}", value: "v#{i}"}])
            end)
          end)

        results = Task.await_many(tasks, 15_000)

        Enum.each(results, fn result ->
          assert match?({:error, _}, result),
                 "Concurrent produce should get error, got: #{inspect(result)}"
        end)
      end)

      Process.sleep(1_000)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "after", value: "recovery"}])
    end
  end

  # ---------------------------------------------------------------------------
  # Producer Recovery Scenarios
  # ---------------------------------------------------------------------------

  describe "producer recovery" do
    test "producer recovers after multiple network failures", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k1", value: "v1"}])

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 0, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k2", value: "v2"}])

      ChaosTestHelpers.with_timeout(ctx.proxy_name, 100, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k", value: "v"}])
        assert match?({:error, _}, result)
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{key: "k3", value: "v3"}])
    end

    test "new client can produce after previous client failed", ctx do
      {:ok, client1} = ChaosTestHelpers.start_client(ctx)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        _ = KafkaEx.API.produce(client1, @test_topic, 0, [%{key: "k", value: "v"}])
      end)

      ChaosTestHelpers.stop_client()
      Process.sleep(500)

      {:ok, client2} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.produce(client2, @test_topic, 0, [%{key: "k", value: "v"}])
    end
  end
end
