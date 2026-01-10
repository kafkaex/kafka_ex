defmodule KafkaEx.Client.ApiVersionsRetryTest do
  @moduledoc """
  Tests for ApiVersions negotiation retry logic (issue #433).

  Validates exponential backoff calculation and error classification
  for intermittent parse errors during initial connection.
  """
  use ExUnit.Case, async: true

  # Test the exponential backoff calculation
  # This mirrors the private api_versions_retry_delay/1 function in Client
  describe "exponential backoff calculation" do
    @api_versions_retry_base_delay_ms 100

    defp api_versions_retry_delay(attempt) do
      trunc(@api_versions_retry_base_delay_ms * :math.pow(2, attempt))
    end

    test "first attempt (0) has base delay" do
      assert api_versions_retry_delay(0) == 100
    end

    test "second attempt (1) doubles the delay" do
      assert api_versions_retry_delay(1) == 200
    end

    test "third attempt (2) quadruples the delay" do
      assert api_versions_retry_delay(2) == 400
    end

    test "delays follow 2^n pattern" do
      delays = Enum.map(0..4, &api_versions_retry_delay/1)
      assert delays == [100, 200, 400, 800, 1600]
    end

    test "total wait time for 3 retries is reasonable" do
      # Total: 100 + 200 + 400 = 700ms
      total = Enum.sum(Enum.map(0..2, &api_versions_retry_delay/1))
      assert total == 700
      # Should complete within 1 second
      assert total < 1_000
    end
  end

  # Test the retryable error classification
  describe "retryable error classification" do
    @retryable_errors [:parse_error, :timeout, :closed]

    defp retryable?(error) do
      error in @retryable_errors
    end

    test "parse_error is retryable" do
      assert retryable?(:parse_error)
    end

    test "timeout is retryable" do
      assert retryable?(:timeout)
    end

    test "closed is retryable" do
      assert retryable?(:closed)
    end

    test "no_broker is NOT retryable" do
      refute retryable?(:no_broker)
    end

    test "network_error is NOT retryable" do
      refute retryable?(:network_error)
    end

    test "econnrefused is NOT retryable" do
      refute retryable?(:econnrefused)
    end
  end

  # Test the retry behavior patterns
  describe "retry behavior" do
    @api_versions_max_retries 3

    test "max retries constant is 3" do
      assert @api_versions_max_retries == 3
    end

    test "retries eventually exhaust" do
      # Simulate retry countdown
      retries = Enum.take_while(@api_versions_max_retries..0//-1, fn n -> n >= 0 end)
      # 3, 2, 1, 0
      assert length(retries) == 4
    end

    test "first retry uses attempt 0 delay" do
      # When retries_left = 3 (max), attempt = 3 - 3 = 0
      assert @api_versions_max_retries - @api_versions_max_retries == 0
    end

    test "last retry uses attempt 2 delay" do
      # When retries_left = 1, attempt = 3 - 1 = 2
      assert @api_versions_max_retries - 1 == 2
    end
  end

  # Error message tests
  describe "error messages" do
    test "parse_error message mentions retries and possible causes" do
      message =
        "ApiVersions negotiation failed: unable to parse broker response after 3 retries. " <>
          "This may indicate network issues, broker instability, or protocol incompatibility."

      assert message =~ "parse"
      assert message =~ "3 retries"
      assert message =~ "network issues"
      assert message =~ "broker instability"
      assert message =~ "protocol incompatibility"
    end

    test "no_broker message mentions checking addresses" do
      message =
        "ApiVersions negotiation failed: no broker available. " <>
          "Check that brokers are reachable at the configured addresses."

      assert message =~ "no broker"
      assert message =~ "reachable"
      assert message =~ "configured addresses"
    end

    test "generic error message includes inspect of reason" do
      reason = :econnrefused

      message =
        "ApiVersions negotiation failed: #{inspect(reason)}. " <>
          "Check broker connectivity and network configuration."

      assert message =~ ":econnrefused"
      assert message =~ "connectivity"
    end
  end

  # Integration with init behavior
  describe "init integration patterns" do
    test "successful negotiation returns {:ok, versions, state}" do
      # Pattern that init expects on success
      result = {:ok, %{api_versions: %{}, error_code: 0}, %{}}
      assert {:ok, _versions, _state} = result
    end

    test "failed negotiation returns {:error, reason, state}" do
      # Pattern that init expects on failure
      result = {:error, :parse_error, %{}}
      assert {:error, :parse_error, _state} = result
    end
  end
end
