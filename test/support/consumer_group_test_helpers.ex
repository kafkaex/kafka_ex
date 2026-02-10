defmodule KafkaEx.TestSupport.ConsumerGroupHelpers do
  @moduledoc """
  Helper functions for consumer group integration tests.

  Provides utilities for:
  - Waiting for consumer group state transitions
  - Starting/stopping consumer groups with test-friendly defaults
  - Collecting and counting messages

  ## Usage

      use KafkaEx.TestSupport.ConsumerGroupHelpers

      test "my consumer group test", ctx do
        cg_pid = start_test_consumer_group(ctx, "my_test")
        assert {:ok, :active} = wait_for_active(cg_pid)
        assert {:ok, assignments} = wait_for_assignments(cg_pid)
      end
  """

  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.TestSupport.TestGenConsumer

  @default_timeout 15_000
  @poll_interval 200

  @doc """
  Wait for a consumer group to become active.

  Returns `{:ok, :active}` when the consumer group reports as active,
  or `{:error, :timeout}` if the timeout is exceeded.

  ## Options
    * `:timeout` - Maximum time to wait in milliseconds (default: #{@default_timeout})
  """
  @spec wait_for_active(pid(), keyword()) :: {:ok, :active} | {:error, :timeout}
  def wait_for_active(cg_pid, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_wait_for_active(cg_pid, deadline)
  end

  defp do_wait_for_active(cg_pid, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      try do
        if ConsumerGroup.active?(cg_pid) do
          {:ok, :active}
        else
          Process.sleep(@poll_interval)
          do_wait_for_active(cg_pid, deadline)
        end
      catch
        :exit, _ ->
          Process.sleep(@poll_interval)
          do_wait_for_active(cg_pid, deadline)
      end
    end
  end

  @doc """
  Wait for a consumer group to receive partition assignments.

  Returns `{:ok, assignments}` when assignments are available,
  or `{:error, :timeout}` if the timeout is exceeded.

  ## Options
    * `:timeout` - Maximum time to wait in milliseconds (default: #{@default_timeout})
  """
  @spec wait_for_assignments(pid(), keyword()) :: {:ok, list()} | {:error, :timeout}
  def wait_for_assignments(cg_pid, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_wait_for_assignments(cg_pid, deadline)
  end

  defp do_wait_for_assignments(cg_pid, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      try do
        assignments = ConsumerGroup.assignments(cg_pid)

        if is_list(assignments) and length(assignments) > 0 do
          {:ok, assignments}
        else
          Process.sleep(@poll_interval)
          do_wait_for_assignments(cg_pid, deadline)
        end
      catch
        :exit, _ ->
          Process.sleep(@poll_interval)
          do_wait_for_assignments(cg_pid, deadline)
      end
    end
  end

  @doc """
  Start a consumer group with test-friendly defaults.

  Uses `TestGenConsumer` as the consumer module and configures reasonable
  defaults for testing (short heartbeat interval, etc.).

  ## Options
    * `:uris` - Required. List of broker URIs (e.g., `[{"localhost", 9092}]`)
    * `:topics` - Required. List of topics to consume
    * `:group_prefix` - Prefix for the consumer group name (default: "test_group")
    * `:group_suffix` - Suffix to add to the group name (default: "")
    * `:consumer_module` - Consumer module to use (default: TestGenConsumer)
    * `:test_pid` - Process to notify of received messages (default: `self()`)
    * `:heartbeat_interval` - Heartbeat interval in ms (default: 1000)
    * `:session_timeout` - Session timeout in ms (default: 30000)
    * `:commit_interval` - Commit interval in ms (default: 1000)
    * `:auto_offset_reset` - Where to start consuming (default: :earliest)
  """
  @spec start_test_consumer_group(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_test_consumer_group(opts) do
    uris = Keyword.fetch!(opts, :uris)
    topics = Keyword.fetch!(opts, :topics)
    group_prefix = Keyword.get(opts, :group_prefix, "test_group")
    group_suffix = Keyword.get(opts, :group_suffix, "")
    consumer_module = Keyword.get(opts, :consumer_module, TestGenConsumer)
    test_pid = Keyword.get(opts, :test_pid, self())

    group_name = build_group_name(group_prefix, group_suffix)

    consumer_opts = [
      uris: uris,
      heartbeat_interval: Keyword.get(opts, :heartbeat_interval, 1_000),
      session_timeout: Keyword.get(opts, :session_timeout, 30_000),
      commit_interval: Keyword.get(opts, :commit_interval, 1_000),
      auto_offset_reset: Keyword.get(opts, :auto_offset_reset, :earliest),
      extra_consumer_args: %{test_pid: test_pid}
    ]

    ConsumerGroup.start_link(consumer_module, group_name, topics, consumer_opts)
  end

  @doc """
  Start a consumer group using context from ChaosTestHelpers.

  Convenience function that extracts the proxy port from the context.

  ## Examples

      {:ok, cg_pid} = start_test_consumer_group(ctx, "my_test", topic: "my-topic")
  """
  @spec start_test_consumer_group(map(), String.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start_test_consumer_group(ctx, group_suffix, opts \\ []) do
    proxy_port = Map.fetch!(ctx, :proxy_port)
    topics = Keyword.get(opts, :topics, ["chaos_cg_test"])

    merged_opts =
      opts
      |> Keyword.put(:uris, [{"localhost", proxy_port}])
      |> Keyword.put(:topics, topics)
      |> Keyword.put(:group_suffix, group_suffix)

    start_test_consumer_group(merged_opts)
  end

  defp build_group_name(prefix, suffix) do
    random_id = :rand.uniform(100_000)

    case suffix do
      "" -> "#{prefix}_#{random_id}"
      _ -> "#{prefix}_#{suffix}_#{random_id}"
    end
  end

  @doc """
  Safely stop a consumer group.

  Handles cases where the process is already dead or doesn't respond.
  Always returns `:ok`.
  """
  @spec stop_consumer_group(pid() | nil) :: :ok
  def stop_consumer_group(nil), do: :ok

  def stop_consumer_group(cg_pid) when is_pid(cg_pid) do
    if Process.alive?(cg_pid) do
      try do
        Supervisor.stop(cg_pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @doc """
  Wait for a minimum number of messages to be received.

  Collects `{:messages_received, count}` messages sent to the current process
  until `min_count` is reached or the timeout expires.

  Returns the total count of messages received.

  ## Options
    * `:timeout` - Maximum time to wait in milliseconds (default: #{@default_timeout})
  """
  @spec wait_for_message_count(non_neg_integer(), keyword()) :: non_neg_integer()
  def wait_for_message_count(min_count, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_collect_messages(0, min_count, deadline)
  end

  defp do_collect_messages(current_count, min_count, deadline) do
    if current_count >= min_count do
      current_count
    else
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        current_count
      else
        receive do
          {:messages_received, count} ->
            do_collect_messages(current_count + count, min_count, deadline)
        after
          min(remaining, 1000) ->
            do_collect_messages(current_count, min_count, deadline)
        end
      end
    end
  end

  @doc """
  Register a consumer group for cleanup on test exit.

  Call this in your test setup to ensure the consumer group is stopped
  even if the test fails.

  ## Example

      test "my test", ctx do
        {:ok, cg_pid} = start_test_consumer_group(ctx, "my_test")
        register_consumer_group_cleanup(cg_pid)

        # Test code - no need for try/after
        assert {:ok, :active} = wait_for_active(cg_pid)
      end
  """
  @spec register_consumer_group_cleanup(pid()) :: :ok
  def register_consumer_group_cleanup(cg_pid) do
    ExUnit.Callbacks.on_exit(fn ->
      stop_consumer_group(cg_pid)
    end)

    :ok
  end
end
