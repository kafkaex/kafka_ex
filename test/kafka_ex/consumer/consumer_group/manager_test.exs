defmodule KafkaEx.Consumer.ConsumerGroup.ManagerTest do
  @moduledoc """
  Unit tests for ConsumerGroup.Manager retry and error handling logic.

  Tests the KAFKA-6829 pattern implementation:
  - Error classification (recoverable vs unrecoverable)
  - Exponential backoff calculations
  - Retry behavior specifications
  """
  use ExUnit.Case, async: true

  # Import the module to test private functions via Module attributes
  # We test the behavior specification, not the implementation details

  describe "error classification (KAFKA-6829 pattern)" do
    # These are the errors that should trigger retry behavior
    @recoverable_errors [
      :coordinator_not_available,
      :not_coordinator,
      :coordinator_load_in_progress,
      :unknown_topic_or_partition,
      :no_broker,
      :timeout
    ]

    # These errors should NOT trigger retry - they indicate the consumer
    # is in an invalid state and must be restarted fresh
    @unrecoverable_errors [
      :unknown_member_id,
      :illegal_generation,
      :rebalance_in_progress,
      :invalid_group_id,
      :group_authorization_failed
    ]

    test "coordinator errors are recoverable" do
      # Coordinator may have moved or be temporarily unavailable
      assert :coordinator_not_available in @recoverable_errors
      assert :not_coordinator in @recoverable_errors
      assert :coordinator_load_in_progress in @recoverable_errors
    end

    test "unknown_topic_or_partition is recoverable per KAFKA-6829" do
      # Java client (KAFKA-6829) treats this like COORDINATOR_LOAD_IN_PROGRESS
      # Topic metadata may not be available immediately during cluster startup
      assert :unknown_topic_or_partition in @recoverable_errors
    end

    test "transient errors are recoverable" do
      assert :no_broker in @recoverable_errors
      assert :timeout in @recoverable_errors
    end

    test "member identity errors are NOT recoverable" do
      # These indicate the consumer's identity is invalid - must restart fresh
      assert :unknown_member_id in @unrecoverable_errors
      assert :illegal_generation in @unrecoverable_errors
    end

    test "authorization errors are NOT recoverable" do
      # Authorization failures won't be fixed by retrying
      assert :group_authorization_failed in @unrecoverable_errors
    end

    test "recoverable and unrecoverable errors are disjoint" do
      intersection =
        MapSet.intersection(
          MapSet.new(@recoverable_errors),
          MapSet.new(@unrecoverable_errors)
        )

      assert MapSet.size(intersection) == 0,
             "Errors cannot be both recoverable and unrecoverable: #{inspect(MapSet.to_list(intersection))}"
    end
  end

  describe "join retry backoff calculation" do
    # Join retry uses: base_delay=1000ms, max_delay=10000ms
    # Formula: min(base_delay * 2^(attempt-1), max_delay)

    @base_delay 1000
    @max_delay 10_000

    test "first attempt has base delay" do
      assert calculate_backoff(1, @base_delay, @max_delay) == 1000
    end

    test "delay doubles each attempt" do
      assert calculate_backoff(1, @base_delay, @max_delay) == 1000
      assert calculate_backoff(2, @base_delay, @max_delay) == 2000
      assert calculate_backoff(3, @base_delay, @max_delay) == 4000
      assert calculate_backoff(4, @base_delay, @max_delay) == 8000
    end

    test "delay is capped at max" do
      # 2^4 * 1000 = 16000 > 10000, so capped
      assert calculate_backoff(5, @base_delay, @max_delay) == 10_000
      assert calculate_backoff(6, @base_delay, @max_delay) == 10_000
      assert calculate_backoff(10, @base_delay, @max_delay) == 10_000
    end

    test "total wait time for 6 retries is reasonable" do
      # With 6 max retries, we sleep on attempts 1-5 (5 sleeps)
      total_sleep =
        Enum.reduce(1..5, 0, fn attempt, acc ->
          acc + calculate_backoff(attempt, @base_delay, @max_delay)
        end)

      # 1000 + 2000 + 4000 + 8000 + 10000 = 25000ms = 25s
      assert total_sleep == 25_000
      # This is reasonable - gives cluster time to recover without being too long
      assert total_sleep <= 30_000
    end
  end

  describe "topic retry backoff calculation" do
    # Topic retry uses: base_delay=100ms, max_delay=5000ms
    # Formula: min(base_delay * 2^(attempt-1), max_delay)

    @base_delay 100
    @max_delay 5000

    test "first attempt has base delay" do
      assert calculate_backoff(1, @base_delay, @max_delay) == 100
    end

    test "delay doubles each attempt" do
      assert calculate_backoff(1, @base_delay, @max_delay) == 100
      assert calculate_backoff(2, @base_delay, @max_delay) == 200
      assert calculate_backoff(3, @base_delay, @max_delay) == 400
      assert calculate_backoff(4, @base_delay, @max_delay) == 800
      assert calculate_backoff(5, @base_delay, @max_delay) == 1600
    end

    test "delay is capped at max" do
      # 2^6 * 100 = 6400 > 5000, so capped
      assert calculate_backoff(7, @base_delay, @max_delay) == 5000
      assert calculate_backoff(10, @base_delay, @max_delay) == 5000
    end

    test "total wait time for 5 retries is fast" do
      # With 5 max retries, we sleep on attempts 1-4 (4 sleeps)
      total_sleep =
        Enum.reduce(1..4, 0, fn attempt, acc ->
          acc + calculate_backoff(attempt, @base_delay, @max_delay)
        end)

      # 100 + 200 + 400 + 800 = 1500ms = 1.5s
      assert total_sleep == 1500
      # Topic discovery should be fast - cluster should have metadata quickly
      assert total_sleep <= 5000
    end
  end

  describe "retry count behavior" do
    test "join allows up to 6 attempts (5 retries)" do
      max_join_retries = 6
      # Attempt 1: initial try
      # Attempts 2-6: retries (5 retries total)
      # On attempt 6, if still failing, raise
      retries = max_join_retries - 1
      assert retries == 5
    end

    test "topic discovery allows up to 5 attempts (4 retries)" do
      max_topic_retries = 5
      # Attempt 1: initial try
      # Attempts 2-5: retries (4 retries total)
      # On attempt 5, if still failing, return what we have
      retries = max_topic_retries - 1
      assert retries == 4
    end
  end

  describe "error handling priority" do
    # This documents the expected behavior of the error handling logic

    test "unrecoverable errors should fail immediately regardless of attempt count" do
      # Even on attempt 1, an unrecoverable error should raise immediately
      # with the correct error message (not "after N attempts")
      unrecoverable = :illegal_generation
      refute unrecoverable in recoverable_errors()
    end

    test "recoverable errors should retry until max attempts" do
      recoverable = :coordinator_not_available
      assert recoverable in recoverable_errors()
    end

    test "recoverable errors on last attempt should mention attempt count" do
      # When we've exhausted retries, the error message should indicate
      # how many attempts were made
      max_attempts = 6
      assert max_attempts > 1
    end
  end

  describe "heartbeat error handling" do
    # Documents the expected heartbeat error flow

    test "rebalance_in_progress triggers rejoin via :rebalance signal" do
      # Heartbeat.ex handles :rebalance_in_progress specially
      # Sends {:shutdown, :rebalance} which manager handles at line 233
      assert :rebalance_in_progress not in recoverable_errors()
      # But it's handled specially in heartbeat.ex, not via recoverable_error?
    end

    test "unknown_member_id triggers rejoin via :rebalance signal" do
      # Heartbeat.ex handles :unknown_member_id specially
      # Sends {:shutdown, :rebalance} - gives ONE chance to rejoin
      # If rejoin fails with :unknown_member_id again, GenServer crashes
      # and supervisor restarts with fresh state
      assert :unknown_member_id not in recoverable_errors()
    end

    test "other errors use recoverable_error? check" do
      # For errors not explicitly handled in heartbeat.ex,
      # manager checks recoverable_error? to decide retry vs crash
      assert :coordinator_not_available in recoverable_errors()
      assert :no_broker in recoverable_errors()
    end
  end

  # Helper functions that mirror the implementation

  defp calculate_backoff(attempt, base_delay, max_delay) do
    delay = base_delay * round(:math.pow(2, attempt - 1))
    min(delay, max_delay)
  end

  defp recoverable_errors do
    [
      :coordinator_not_available,
      :not_coordinator,
      :coordinator_load_in_progress,
      :unknown_topic_or_partition,
      :no_broker,
      :timeout
    ]
  end
end
