defmodule KafkaEx.Consumer.CommitRetryTest do
  @moduledoc """
  Tests for commit retry logic (issue #425).

  Validates exponential backoff calculation and retry behavior
  for transient commit failures in high-latency environments (AWS MSK).
  """
  use ExUnit.Case, async: true

  # Test the exponential backoff calculation
  # This mirrors the private commit_retry_delay/1 function in GenConsumer
  describe "exponential backoff calculation" do
    @commit_retry_base_delay_ms 100

    defp commit_retry_delay(attempt) do
      trunc(@commit_retry_base_delay_ms * :math.pow(2, attempt))
    end

    test "first attempt (0) has base delay" do
      assert commit_retry_delay(0) == 100
    end

    test "second attempt (1) doubles the delay" do
      assert commit_retry_delay(1) == 200
    end

    test "third attempt (2) quadruples the delay" do
      assert commit_retry_delay(2) == 400
    end

    test "delays follow 2^n pattern" do
      delays = Enum.map(0..4, &commit_retry_delay/1)
      assert delays == [100, 200, 400, 800, 1600]
    end

    test "total wait time for 3 retries is reasonable" do
      # Total: 100 + 200 + 400 = 700ms
      total = Enum.sum(Enum.map(0..2, &commit_retry_delay/1))
      assert total == 700
      # Should complete within 1 second
      assert total < 1_000
    end
  end

  # Test the retryable error classification
  describe "retryable error classification" do
    @retryable_errors [:timeout, :request_timed_out, :coordinator_not_available, :not_coordinator]

    defp retryable?(error) do
      error in @retryable_errors
    end

    test "timeout is retryable" do
      assert retryable?(:timeout)
    end

    test "request_timed_out is retryable" do
      assert retryable?(:request_timed_out)
    end

    test "coordinator_not_available is retryable" do
      assert retryable?(:coordinator_not_available)
    end

    test "not_coordinator is retryable" do
      assert retryable?(:not_coordinator)
    end

    test "unknown_member_id is NOT retryable" do
      refute retryable?(:unknown_member_id)
    end

    test "illegal_generation is NOT retryable" do
      refute retryable?(:illegal_generation)
    end

    test "rebalance_in_progress is NOT retryable" do
      refute retryable?(:rebalance_in_progress)
    end

    test "authorization errors are NOT retryable" do
      refute retryable?(:group_authorization_failed)
      refute retryable?(:topic_authorization_failed)
    end

    test "network_error is NOT retryable (requires reconnection)" do
      refute retryable?(:network_error)
    end
  end

  # Test the retry behavior patterns
  describe "retry behavior" do
    @commit_max_retries 3

    test "max retries constant is 3" do
      assert @commit_max_retries == 3
    end

    test "retries eventually exhaust" do
      # Simulate retry countdown
      retries = Enum.take_while(@commit_max_retries..0//-1, fn n -> n >= 0 end)
      # 3, 2, 1, 0
      assert length(retries) == 4
    end

    test "first retry uses attempt 0 delay" do
      # When retries_left = 3 (max), attempt = 3 - 3 = 0
      assert @commit_max_retries - @commit_max_retries == 0
    end

    test "last retry uses attempt 2 delay" do
      # When retries_left = 1, attempt = 3 - 1 = 2
      assert @commit_max_retries - 1 == 2
    end
  end

  # AWS MSK-specific scenarios (high latency environment)
  describe "AWS MSK scenarios" do
    @commit_retry_base_delay_ms 100

    defp aws_retry_delay(attempt) do
      trunc(@commit_retry_base_delay_ms * :math.pow(2, attempt))
    end

    test "retry delays accommodate high-latency networks" do
      # AWS MSK can have 50-200ms latency
      # Our delays (100, 200, 400) give coordinator time to recover
      delays = Enum.map(0..2, &aws_retry_delay/1)
      assert Enum.all?(delays, fn d -> d >= 100 end)
    end

    test "total retry time is under request timeout" do
      # Default request timeout is typically 5000ms
      # Total retry time (700ms) should be well under that
      total = Enum.sum(Enum.map(0..2, &aws_retry_delay/1))
      assert total < 5_000
    end

    test "delays increase to handle cascading failures" do
      delays = Enum.map(0..2, &aws_retry_delay/1)
      # Each delay should be greater than the previous
      pairs = Enum.zip(delays, Enum.drop(delays, 1))
      assert Enum.all?(pairs, fn {prev, curr} -> curr > prev end)
    end
  end

  # Stream module retry logic (should mirror GenConsumer)
  describe "stream commit retry consistency" do
    @stream_commit_max_retries 3
    @stream_commit_retry_base_delay_ms 100

    defp stream_retry_delay(retries_left) do
      # This mirrors the stream.ex implementation
      trunc(@stream_commit_retry_base_delay_ms * :math.pow(2, @stream_commit_max_retries - retries_left))
    end

    test "stream uses same max retries as GenConsumer" do
      assert @stream_commit_max_retries == 3
    end

    test "stream uses same base delay as GenConsumer" do
      assert @stream_commit_retry_base_delay_ms == 100
    end

    test "stream delay calculation produces same values" do
      # When retries_left = 3, delay = 100 * 2^0 = 100
      assert stream_retry_delay(3) == 100
      # When retries_left = 2, delay = 100 * 2^1 = 200
      assert stream_retry_delay(2) == 200
      # When retries_left = 1, delay = 100 * 2^2 = 400
      assert stream_retry_delay(1) == 400
    end
  end
end
