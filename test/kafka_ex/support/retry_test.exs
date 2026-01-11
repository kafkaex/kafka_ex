defmodule KafkaEx.Support.RetryTest do
  @moduledoc """
  Tests for KafkaEx.Support.Retry module.

  Validates exponential backoff calculation, error classification,
  and retry behavior.
  """
  use ExUnit.Case, async: true

  alias KafkaEx.Support.Retry

  describe "backoff_delay/3" do
    test "first attempt (0) returns base delay" do
      assert Retry.backoff_delay(0, 100) == 100
    end

    test "second attempt (1) doubles the delay" do
      assert Retry.backoff_delay(1, 100) == 200
    end

    test "third attempt (2) quadruples the delay" do
      assert Retry.backoff_delay(2, 100) == 400
    end

    test "follows 2^n pattern" do
      delays = Enum.map(0..4, &Retry.backoff_delay(&1, 100))
      assert delays == [100, 200, 400, 800, 1600]
    end

    test "respects max_ms cap" do
      assert Retry.backoff_delay(10, 100, 5000) == 5000
    end

    test "no cap with :infinity" do
      assert Retry.backoff_delay(10, 100, :infinity) == 102_400
    end

    test "works with different base delays" do
      assert Retry.backoff_delay(0, 50) == 50
      assert Retry.backoff_delay(1, 50) == 100
      assert Retry.backoff_delay(2, 50) == 200
    end
  end

  describe "with_retry/2" do
    test "returns success immediately on first try" do
      result = Retry.with_retry(fn -> {:ok, :success} end)
      assert result == {:ok, :success}
    end

    test "returns error when all retries exhausted" do
      counter = :counters.new(1, [])

      result =
        Retry.with_retry(
          fn ->
            :counters.add(counter, 1, 1)
            {:error, :always_fails}
          end,
          max_retries: 3,
          base_delay_ms: 1
        )

      assert result == {:error, :always_fails}
      assert :counters.get(counter, 1) == 3
    end

    test "retries on retryable error and succeeds" do
      counter = :counters.new(1, [])

      result =
        Retry.with_retry(
          fn ->
            :counters.add(counter, 1, 1)

            if :counters.get(counter, 1) < 3 do
              {:error, :transient}
            else
              {:ok, :success}
            end
          end,
          max_retries: 5,
          base_delay_ms: 1
        )

      assert result == {:ok, :success}
      assert :counters.get(counter, 1) == 3
    end

    test "does not retry non-retryable errors" do
      counter = :counters.new(1, [])

      result =
        Retry.with_retry(
          fn ->
            :counters.add(counter, 1, 1)
            {:error, :not_retryable}
          end,
          max_retries: 5,
          base_delay_ms: 1,
          retryable?: fn error -> error == :retryable end
        )

      assert result == {:error, :not_retryable}
      assert :counters.get(counter, 1) == 1
    end

    test "calls on_retry callback" do
      errors = :ets.new(:errors, [:set, :public])
      :ets.insert(errors, {:attempts, []})

      Retry.with_retry(
        fn -> {:error, :test_error} end,
        max_retries: 3,
        base_delay_ms: 1,
        on_retry: fn error, attempt, delay ->
          [{:attempts, attempts}] = :ets.lookup(errors, :attempts)
          :ets.insert(errors, {:attempts, [{error, attempt, delay} | attempts]})
        end
      )

      [{:attempts, attempts}] = :ets.lookup(errors, :attempts)
      :ets.delete(errors)

      # Should have 2 retries (first attempt doesn't trigger on_retry)
      assert length(attempts) == 2
      assert Enum.all?(attempts, fn {error, _attempt, _delay} -> error == :test_error end)
    end

    test "uses default options when not specified" do
      # Default: max_retries: 3, base_delay_ms: 100, retryable?: fn _ -> true end
      counter = :counters.new(1, [])

      # This would be slow with default 100ms delay, so we override base_delay_ms
      Retry.with_retry(
        fn ->
          :counters.add(counter, 1, 1)
          {:error, :fail}
        end,
        base_delay_ms: 1
      )

      # Default max_retries is 3
      assert :counters.get(counter, 1) == 3
    end
  end

  describe "coordinator_error?/1" do
    test "coordinator_not_available is coordinator error" do
      assert Retry.coordinator_error?(:coordinator_not_available)
    end

    test "not_coordinator is coordinator error" do
      assert Retry.coordinator_error?(:not_coordinator)
    end

    test "coordinator_load_in_progress is coordinator error" do
      assert Retry.coordinator_error?(:coordinator_load_in_progress)
    end

    test "timeout is NOT coordinator error" do
      refute Retry.coordinator_error?(:timeout)
    end

    test "not_leader_for_partition is NOT coordinator error" do
      refute Retry.coordinator_error?(:not_leader_for_partition)
    end
  end

  describe "transient_error?/1" do
    test "timeout is transient" do
      assert Retry.transient_error?(:timeout)
    end

    test "request_timed_out is transient" do
      assert Retry.transient_error?(:request_timed_out)
    end

    test "parse_error is transient" do
      assert Retry.transient_error?(:parse_error)
    end

    test "closed is transient" do
      assert Retry.transient_error?(:closed)
    end

    test "no_broker is transient" do
      assert Retry.transient_error?(:no_broker)
    end

    test "coordinator errors are transient" do
      assert Retry.transient_error?(:coordinator_not_available)
      assert Retry.transient_error?(:not_coordinator)
    end

    test "not_leader_for_partition is NOT transient" do
      refute Retry.transient_error?(:not_leader_for_partition)
    end

    test "authorization errors are NOT transient" do
      refute Retry.transient_error?(:group_authorization_failed)
    end
  end

  describe "leadership_error?/1" do
    test "not_leader_for_partition is leadership error" do
      assert Retry.leadership_error?(:not_leader_for_partition)
    end

    test "leader_not_available is leadership error" do
      assert Retry.leadership_error?(:leader_not_available)
    end

    test "not_leader_or_follower is leadership error" do
      assert Retry.leadership_error?(:not_leader_or_follower)
    end

    test "fenced_leader_epoch is leadership error" do
      assert Retry.leadership_error?(:fenced_leader_epoch)
    end

    test "unknown_topic_or_partition is leadership error" do
      assert Retry.leadership_error?(:unknown_topic_or_partition)
    end

    test "timeout is NOT leadership error" do
      refute Retry.leadership_error?(:timeout)
    end

    test "coordinator errors are NOT leadership errors" do
      refute Retry.leadership_error?(:coordinator_not_available)
    end
  end

  describe "produce_retryable?/1" do
    test "leadership errors are produce-retryable" do
      assert Retry.produce_retryable?(:not_leader_for_partition)
      assert Retry.produce_retryable?(:leader_not_available)
      assert Retry.produce_retryable?(:not_leader_or_follower)
    end

    test "timeout is NOT produce-retryable (unsafe)" do
      refute Retry.produce_retryable?(:timeout)
    end

    test "request_timed_out is NOT produce-retryable (unsafe)" do
      refute Retry.produce_retryable?(:request_timed_out)
    end

    test "coordinator errors are NOT produce-retryable" do
      refute Retry.produce_retryable?(:coordinator_not_available)
    end
  end

  describe "consumer_group_retryable?/1" do
    test "unknown_topic_or_partition is consumer group retryable" do
      assert Retry.consumer_group_retryable?(:unknown_topic_or_partition)
    end

    test "coordinator errors are consumer group retryable" do
      assert Retry.consumer_group_retryable?(:coordinator_not_available)
      assert Retry.consumer_group_retryable?(:not_coordinator)
    end

    test "transient errors are consumer group retryable" do
      assert Retry.consumer_group_retryable?(:timeout)
      assert Retry.consumer_group_retryable?(:request_timed_out)
    end

    test "authorization errors are NOT consumer group retryable" do
      refute Retry.consumer_group_retryable?(:group_authorization_failed)
    end
  end

  describe "commit_retryable?/1" do
    test "transient errors are commit retryable" do
      assert Retry.commit_retryable?(:timeout)
      assert Retry.commit_retryable?(:request_timed_out)
      assert Retry.commit_retryable?(:coordinator_not_available)
      assert Retry.commit_retryable?(:not_coordinator)
    end

    test "authorization errors are NOT commit retryable" do
      refute Retry.commit_retryable?(:group_authorization_failed)
    end
  end
end
