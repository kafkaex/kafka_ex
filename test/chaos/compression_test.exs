defmodule KafkaEx.Chaos.CompressionTest do
  use ExUnit.Case, async: false

  @moduletag :chaos
  @moduletag timeout: 300_000

  alias KafkaEx.ChaosTestHelpers

  @test_topic "chaos_compression_test"

  setup_all do
    {:ok, ctx} = ChaosTestHelpers.start_infrastructure()
    on_exit(fn -> ChaosTestHelpers.stop_infrastructure(ctx) end)
    {:ok, ctx}
  end

  setup ctx do
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
  # Gzip Compression Under Network Failures
  # ---------------------------------------------------------------------------

  describe "gzip produce during broker unavailability" do
    test "gzip produce fails when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..10, fn i -> %{value: "gzip-msg-#{i}"} end)

      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)
        assert match?({:error, _}, result), "Expected error when broker is down, got: #{inspect(result)}"
      end)

      Process.sleep(1000)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)
    end

    test "gzip produce fails with connection reset", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..10, fn i -> %{value: "gzip-reset-#{i}"} end)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 50, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)
        assert match?({:error, _}, result), "Expected error on connection reset, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)
    end
  end

  # ---------------------------------------------------------------------------
  # Gzip Compression Under Latency & Bandwidth
  # ---------------------------------------------------------------------------

  describe "gzip produce under network degradation" do
    test "gzip produce with moderate latency succeeds", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..20, fn i -> %{value: "gzip-latency-#{i}"} end)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 500, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)
        assert match?({:ok, _}, result), "Expected success with moderate latency, got: #{inspect(result)}"
      end)
    end

    test "gzip produce with bandwidth limit succeeds", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..5, fn i -> %{value: "gzip-bw-#{i}"} end)

      ChaosTestHelpers.with_bandwidth_limit(ctx.proxy_name, 20, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)
        assert !is_nil(result), "Expected produce to complete under bandwidth limit"
      end)
    end

    test "gzip produce with data timeout fails gracefully", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..10, fn i -> %{value: "gzip-timeout-#{i}"} end)

      ChaosTestHelpers.with_timeout(ctx.proxy_name, 100, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)
        assert match?({:error, _}, result), "Expected error during data timeout, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)
    end
  end

  # ---------------------------------------------------------------------------
  # Snappy Compression Under Network Failures
  # ---------------------------------------------------------------------------

  describe "snappy produce during broker unavailability" do
    test "snappy produce fails when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..10, fn i -> %{value: "snappy-msg-#{i}"} end)

      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :snappy)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :snappy)
        assert match?({:error, _}, result), "Expected error when broker is down, got: #{inspect(result)}"
      end)

      Process.sleep(1000)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :snappy)
    end

    test "snappy produce with connection reset", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..10, fn i -> %{value: "snappy-reset-#{i}"} end)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 50, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :snappy)
        assert match?({:error, _}, result), "Expected error on connection reset, got: #{inspect(result)}"
      end)

      Process.sleep(500)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :snappy)
    end
  end

  # ---------------------------------------------------------------------------
  # Compressed Fetch Under Network Failures
  # ---------------------------------------------------------------------------

  describe "fetch compressed data during network degradation" do
    test "fetch gzip-compressed data with moderate latency", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..20, fn i -> %{value: "fetch-gzip-#{i}"} end)

      {:ok, produce_result} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)

      ChaosTestHelpers.with_latency(ctx.proxy_name, 500, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, produce_result.base_offset, max_bytes: 1_000_000)
        assert match?({:ok, _}, result), "Expected fetch to succeed with latency, got: #{inspect(result)}"

        {:ok, fetch_result} = result
        assert length(fetch_result.records) >= 20
      end)
    end

    test "fetch gzip-compressed data fails when broker is down", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..10, fn i -> %{value: "fetch-down-#{i}"} end)

      {:ok, produce_result} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)

      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, produce_result.base_offset)
        assert match?({:error, _}, result), "Expected error when broker is down"
      end)

      Process.sleep(1000)
      {:ok, fetch_result} = KafkaEx.API.fetch(client, @test_topic, 0, produce_result.base_offset, max_bytes: 1_000_000)
      assert length(fetch_result.records) >= 10
    end

    test "fetch gzip-compressed data with bandwidth limit", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      messages = Enum.map(1..5, fn i -> %{value: "fetch-bw-#{i}"} end)

      {:ok, produce_result} = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)

      ChaosTestHelpers.with_bandwidth_limit(ctx.proxy_name, 20, fn ->
        result = KafkaEx.API.fetch(client, @test_topic, 0, produce_result.base_offset, max_bytes: 100_000)
        assert !is_nil(result), "Expected fetch to complete under bandwidth limit"
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Large Compressed Batch Under Failure
  # ---------------------------------------------------------------------------

  describe "large compressed batch during mid-flight failure" do
    test "large gzip batch returns error on connection reset mid-request", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "warmup"}], compression: :gzip)

      # Large highly-compressible batch - tests that compression + network failure
      # doesn't cause hangs or crashes
      messages = Enum.map(1..100, fn i -> %{value: String.duplicate("compress-me-#{i}-", 100)} end)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 10, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :gzip)

        assert match?({:error, _}, result),
               "Produce should return error on mid-request reset, got: #{inspect(result)}"
      end)

      Process.sleep(1_000)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "after-recovery"}], compression: :gzip)
    end

    test "large snappy batch returns error on connection reset mid-request", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "warmup"}], compression: :snappy)

      messages = Enum.map(1..100, fn i -> %{value: String.duplicate("snappy-#{i}-", 100)} end)

      ChaosTestHelpers.with_reset_peer(ctx.proxy_name, 10, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, messages, compression: :snappy)

        assert match?({:error, _}, result),
               "Produce should return error on mid-request reset, got: #{inspect(result)}"
      end)

      Process.sleep(1_000)
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "after-recovery"}], compression: :snappy)
    end
  end

  # ---------------------------------------------------------------------------
  # Compression Recovery
  # ---------------------------------------------------------------------------

  describe "compression recovery after failures" do
    test "gzip produce/fetch round-trip works after broker failure", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      # Successful produce
      messages1 = Enum.map(1..5, fn i -> %{value: "before-failure-#{i}"} end)
      {:ok, result1} = KafkaEx.API.produce(client, @test_topic, 0, messages1, compression: :gzip)

      # Broker goes down - produce fails
      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        result = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "during-failure"}], compression: :gzip)
        assert match?({:error, _}, result)
      end)

      # Broker comes back - produce and verify full round-trip
      Process.sleep(1000)
      messages2 = Enum.map(1..5, fn i -> %{value: "after-failure-#{i}"} end)
      {:ok, result2} = KafkaEx.API.produce(client, @test_topic, 0, messages2, compression: :gzip)

      # Fetch all messages and verify data integrity
      {:ok, fetch_result} = KafkaEx.API.fetch(client, @test_topic, 0, result1.base_offset, max_bytes: 1_000_000)
      values = Enum.map(fetch_result.records, & &1.value)

      # Messages before failure should be present
      assert "before-failure-1" in values
      assert "before-failure-5" in values

      # Messages after recovery should be present
      {:ok, fetch_result2} = KafkaEx.API.fetch(client, @test_topic, 0, result2.base_offset, max_bytes: 1_000_000)
      values2 = Enum.map(fetch_result2.records, & &1.value)
      assert "after-failure-1" in values2
    end

    test "alternating compression codecs work after network failure", ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      # Gzip produce
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "gzip-1"}], compression: :gzip)

      # Network failure
      ChaosTestHelpers.with_broker_down(ctx.proxy_name, fn ->
        _ = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "fail"}], compression: :snappy)
      end)

      Process.sleep(1000)

      # Snappy produce after recovery
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "snappy-1"}], compression: :snappy)

      # No compression after recovery
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "none-1"}])

      # Gzip again
      {:ok, _} = KafkaEx.API.produce(client, @test_topic, 0, [%{value: "gzip-2"}], compression: :gzip)
    end
  end
end
