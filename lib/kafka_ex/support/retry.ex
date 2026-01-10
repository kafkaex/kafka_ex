defmodule KafkaEx.Support.Retry do
  @moduledoc """
  Unified retry logic with exponential backoff for KafkaEx operations.

  This module provides:
  - Exponential backoff calculation with optional cap
  - Generic retry wrapper function
  - Error classifiers for common Kafka error patterns

  ## Usage

      # Simple retry with defaults (3 retries, 100ms base delay)
      Retry.with_retry(fn -> some_operation() end)

      # Custom retry configuration
      Retry.with_retry(
        fn -> some_operation() end,
        max_retries: 5,
        base_delay_ms: 200,
        max_delay_ms: 5000,
        retryable?: &Retry.transient_error?/1
      )

      # Just calculate backoff delay
      delay = Retry.backoff_delay(attempt, 100, 5000)
  """

  require Logger

  @type error :: atom() | term()
  @type retry_result :: {:ok, term()} | {:error, error()}

  @type retry_opts :: [
          max_retries: non_neg_integer(),
          base_delay_ms: non_neg_integer(),
          max_delay_ms: non_neg_integer() | :infinity,
          retryable?: (error() -> boolean()),
          on_retry: (error(), non_neg_integer(), non_neg_integer() -> :ok) | nil
        ]

  # Default retry settings
  @default_max_retries 3
  @default_base_delay_ms 100
  @default_max_delay_ms :infinity

  @doc """
  Calculate exponential backoff delay.

  ## Parameters
    - `attempt` - Zero-based attempt number (0 = first retry)
    - `base_ms` - Base delay in milliseconds
    - `max_ms` - Maximum delay cap (default: `:infinity`)

  ## Examples

      iex> Retry.backoff_delay(0, 100)
      100

      iex> Retry.backoff_delay(1, 100)
      200

      iex> Retry.backoff_delay(2, 100)
      400

      iex> Retry.backoff_delay(10, 100, 5000)
      5000
  """
  @spec backoff_delay(non_neg_integer(), non_neg_integer(), non_neg_integer() | :infinity) ::
          non_neg_integer()
  def backoff_delay(attempt, base_ms, max_ms \\ @default_max_delay_ms) do
    delay = trunc(base_ms * :math.pow(2, attempt))

    case max_ms do
      :infinity -> delay
      cap when is_integer(cap) -> min(delay, cap)
    end
  end

  @doc """
  Execute a function with retry and exponential backoff.

  ## Options
    - `:max_retries` - Maximum number of retry attempts (default: 3)
    - `:base_delay_ms` - Base delay for exponential backoff (default: 100)
    - `:max_delay_ms` - Maximum delay cap (default: `:infinity`)
    - `:retryable?` - Function to determine if error is retryable (default: always true)
    - `:on_retry` - Optional callback `(error, attempt, delay) -> :ok` for logging

  ## Returns
    - `{:ok, result}` on success
    - `{:error, last_error}` after all retries exhausted or non-retryable error

  ## Examples

      # Retry any error up to 3 times
      Retry.with_retry(fn -> fetch_data() end)

      # Only retry specific errors
      Retry.with_retry(
        fn -> commit_offset() end,
        retryable?: &Retry.coordinator_error?/1
      )
  """
  @spec with_retry((-> retry_result()), retry_opts()) :: retry_result()
  def with_retry(fun, opts \\ []) do
    max_retries = Keyword.get(opts, :max_retries, @default_max_retries)
    base_delay = Keyword.get(opts, :base_delay_ms, @default_base_delay_ms)
    max_delay = Keyword.get(opts, :max_delay_ms, @default_max_delay_ms)
    retryable? = Keyword.get(opts, :retryable?, fn _ -> true end)
    on_retry = Keyword.get(opts, :on_retry)

    do_retry(fun, 0, max_retries, base_delay, max_delay, retryable?, on_retry, nil)
  end

  defp do_retry(_fun, attempt, max, _base, _max_delay, _retryable?, _on_retry, last_error)
       when attempt >= max do
    {:error, last_error}
  end

  defp do_retry(fun, attempt, max, base, max_delay, retryable?, on_retry, _last_error) do
    case fun.() do
      {:ok, result} ->
        {:ok, result}

      {:error, error} ->
        if attempt < max - 1 and retryable?.(error) do
          delay = backoff_delay(attempt, base, max_delay)

          if on_retry do
            on_retry.(error, attempt + 1, delay)
          end

          Process.sleep(delay)
          do_retry(fun, attempt + 1, max, base, max_delay, retryable?, on_retry, error)
        else
          {:error, error}
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Error Classifiers
  # ---------------------------------------------------------------------------

  @doc """
  Check if error is a coordinator-related error that may resolve with retry.

  These errors typically occur during consumer group operations when the
  coordinator is unavailable or changing.
  """
  @spec coordinator_error?(error()) :: boolean()
  def coordinator_error?(:coordinator_not_available), do: true
  def coordinator_error?(:not_coordinator), do: true
  def coordinator_error?(:coordinator_load_in_progress), do: true
  def coordinator_error?(_), do: false

  @doc """
  Check if error is a transient error that may resolve with retry.

  Includes timeouts, parse errors, connection issues, and coordinator errors.
  """
  @spec transient_error?(error()) :: boolean()
  def transient_error?(:timeout), do: true
  def transient_error?(:request_timed_out), do: true
  def transient_error?(:parse_error), do: true
  def transient_error?(:closed), do: true
  def transient_error?(:no_broker), do: true
  def transient_error?(error), do: coordinator_error?(error)

  @doc """
  Check if error is a leadership-related error requiring metadata refresh.

  These errors indicate the client has stale partition leadership information.
  Safe to retry for ALL request types including produce.
  """
  @spec leadership_error?(error()) :: boolean()
  def leadership_error?(:not_leader_for_partition), do: true
  def leadership_error?(:leader_not_available), do: true
  def leadership_error?(:not_leader_or_follower), do: true
  def leadership_error?(:fenced_leader_epoch), do: true
  def leadership_error?(:unknown_topic_or_partition), do: true
  def leadership_error?(_), do: false

  @doc """
  Check if error is safe to retry for produce operations.

  Produce retries are only safe for leadership errors where we know the
  message wasn't written. Timeout errors are NOT safe because the message
  may have been written but the response lost.

  Note: For truly idempotent produces, enable `enable.idempotence=true`
  on the Kafka producer (requires Kafka 0.11+).
  """
  @spec produce_retryable?(error()) :: boolean()
  def produce_retryable?(error), do: leadership_error?(error)

  @doc """
  Check if error is safe to retry for consumer group operations.

  Following Java client pattern (KAFKA-6829): includes coordinator errors,
  transient errors, and UNKNOWN_TOPIC_OR_PARTITION.
  """
  @spec consumer_group_retryable?(error()) :: boolean()
  def consumer_group_retryable?(:unknown_topic_or_partition), do: true
  def consumer_group_retryable?(error), do: transient_error?(error)

  @doc """
  Check if error is safe to retry for commit operations.

  Commits are idempotent so we can safely retry on transient errors.
  """
  @spec commit_retryable?(error()) :: boolean()
  def commit_retryable?(error), do: transient_error?(error)
end
