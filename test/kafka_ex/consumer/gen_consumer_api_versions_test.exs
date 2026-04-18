defmodule KafkaEx.Consumer.GenConsumerApiVersionsTest do
  use ExUnit.Case

  alias KafkaEx.Consumer.GenConsumer
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Test.MockClient

  defmodule TestConsumer do
    use KafkaEx.Consumer.GenConsumer

    def handle_message_set(_messages, state), do: {:async_commit, state}
  end

  # Helper to start a GenConsumer with a mock client and capture the opts
  # passed through to the client for offset_fetch, fetch, and offset_commit calls.
  #
  # opts keyword list supports:
  #   :expected_calls - number of MockClient calls to wait for (default 2: offset_fetch + fetch)
  #   :commit_threshold - forwarded to GenConsumer to trigger commits quickly (default not set)
  defp start_consumer(api_versions_opt, app_config, opts \\ []) do
    expected_calls = Keyword.get(opts, :expected_calls, 2)
    commit_threshold = Keyword.get(opts, :commit_threshold)

    # Set up mock responses so the consumer can progress through init:
    # 1. offset_fetch → returns offset 0 (triggers fetch)
    # 2. fetch → returns a message (triggers message handling and possible commit)
    # 3. offset_commit → returns ok so async commits succeed
    offset_fetch_response =
      {:ok, [%{partition_offsets: [%{offset: 0, error_code: :no_error}]}]}

    fetch_response =
      {:ok,
       %Fetch{
         topic: "test-topic",
         partition: 0,
         records: [%Record{offset: 0, value: "msg", key: nil}],
         last_offset: 0
       }}

    {:ok, client} =
      MockClient.start_link(%{
        offset_fetch: offset_fetch_response,
        fetch: fetch_response,
        offset_commit: {:ok, []}
      })

    # Optionally set Application config
    prev_config = Application.get_env(:kafka_ex, :api_versions)

    if app_config do
      Application.put_env(:kafka_ex, :api_versions, app_config)
    else
      Application.delete_env(:kafka_ex, :api_versions)
    end

    consumer_opts =
      [client: client]
      |> then(fn acc ->
        case api_versions_opt do
          :omit -> acc
          versions -> Keyword.put(acc, :api_versions, versions)
        end
      end)
      |> then(fn acc ->
        if commit_threshold do
          Keyword.put(acc, :commit_threshold, commit_threshold)
        else
          acc
        end
      end)

    {:ok, pid} =
      GenServer.start_link(
        GenConsumer,
        {TestConsumer, "test-group", "test-topic", 0, consumer_opts}
      )

    # Poll until expected_calls have been recorded (no arbitrary sleep)
    calls = MockClient.wait_for_calls(client, expected_calls)

    # Restore previous Application config
    if prev_config do
      Application.put_env(:kafka_ex, :api_versions, prev_config)
    else
      Application.delete_env(:kafka_ex, :api_versions)
    end

    # Stop consumer cleanly
    Process.unlink(pid)
    GenServer.stop(pid, :normal, 1_000)

    {calls, client}
  end

  defp find_offset_fetch_opts(calls) do
    Enum.find_value(calls, fn
      {:offset_fetch, _group, _tp, opts} -> opts
      _ -> nil
    end)
  end

  defp find_fetch_opts(calls) do
    Enum.find_value(calls, fn
      {:fetch, _topic, _partition, _offset, opts} -> opts
      _ -> nil
    end)
  end

  defp find_commit_opts(calls) do
    Enum.find_value(calls, fn
      {:offset_commit, _group, _tp, opts} -> opts
      _ -> nil
    end)
  end

  describe "api_versions resolution — GenConsumer only injects supervisor opts" do
    test "supervisor opts inject api_version into request opts" do
      {calls, _client} =
        start_consumer(
          %{fetch: 5, offset_fetch: 3},
          nil
        )

      fetch_opts = find_fetch_opts(calls)
      assert Keyword.get(fetch_opts, :api_version) == 5

      offset_fetch_opts = find_offset_fetch_opts(calls)
      assert Keyword.get(offset_fetch_opts, :api_version) == 3
    end

    test "application config is NOT read by GenConsumer (handled by RequestBuilder)" do
      # App config is set, but GenConsumer should NOT inject it into opts.
      # RequestBuilder handles app config as tier 2 centrally.
      {calls, _client} =
        start_consumer(
          :omit,
          %{fetch: 4, offset_fetch: 2}
        )

      # No api_version in opts — GenConsumer doesn't read app config
      offset_fetch_opts = find_offset_fetch_opts(calls)
      assert Keyword.get(offset_fetch_opts, :api_version) == nil

      fetch_opts = find_fetch_opts(calls)
      assert Keyword.get(fetch_opts, :api_version) == nil
    end

    test "no api_version in opts when neither supervisor nor app config set" do
      {calls, _client} = start_consumer(:omit, nil)

      offset_fetch_opts = find_offset_fetch_opts(calls)
      assert Keyword.get(offset_fetch_opts, :api_version) == nil

      fetch_opts = find_fetch_opts(calls)
      assert Keyword.get(fetch_opts, :api_version) == nil
    end

    test "supervisor opts present, app config ignored by GenConsumer" do
      # supervisor sets fetch only; app config sets offset_fetch
      # GenConsumer only sees supervisor opts
      {calls, _client} =
        start_consumer(
          %{fetch: 7},
          %{offset_fetch: 2}
        )

      # fetch: from supervisor
      fetch_opts = find_fetch_opts(calls)
      assert Keyword.get(fetch_opts, :api_version) == 7

      # offset_fetch: NOT from app config (GenConsumer doesn't read it)
      offset_fetch_opts = find_offset_fetch_opts(calls)
      assert Keyword.get(offset_fetch_opts, :api_version) == nil
    end

    test "empty supervisor map means no api_version in opts" do
      {calls, _client} =
        start_consumer(
          %{},
          %{fetch: 3, offset_fetch: 1}
        )

      fetch_opts = find_fetch_opts(calls)
      assert Keyword.get(fetch_opts, :api_version) == nil

      offset_fetch_opts = find_offset_fetch_opts(calls)
      assert Keyword.get(offset_fetch_opts, :api_version) == nil
    end

    test "partial supervisor opts — only specified keys get api_version" do
      # Supervisor: fetch=5 only
      # Expected: fetch=5, offset_fetch=nil (no version injected)
      {calls, _client} =
        start_consumer(
          %{fetch: 5},
          nil
        )

      fetch_opts = find_fetch_opts(calls)
      assert Keyword.get(fetch_opts, :api_version) == 5

      offset_fetch_opts = find_offset_fetch_opts(calls)
      assert Keyword.get(offset_fetch_opts, :api_version) == nil
    end

    test "supervisor opts inject api_version for offset_commit" do
      # TestConsumer returns {:async_commit, state} which triggers a commit.
      # commit_threshold: 1 ensures the commit fires immediately after 1 message,
      # so we can reliably wait for 3 calls: offset_fetch + fetch + offset_commit.
      {calls, _client} =
        start_consumer(
          %{fetch: 5, offset_fetch: 3, offset_commit: 2},
          nil,
          expected_calls: 3,
          commit_threshold: 1
        )

      commit_opts = find_commit_opts(calls)

      assert commit_opts != nil, "expected offset_commit call, got: #{inspect(calls)}"
      assert Keyword.get(commit_opts, :api_version) == 2
    end

    test "no api_version in offset_commit opts when not configured" do
      # commit_threshold: 1 ensures the commit fires immediately after 1 message.
      {calls, _client} =
        start_consumer(
          :omit,
          nil,
          expected_calls: 3,
          commit_threshold: 1
        )

      commit_opts = find_commit_opts(calls)

      assert commit_opts != nil, "expected offset_commit call, got: #{inspect(calls)}"
      assert Keyword.get(commit_opts, :api_version) == nil
    end
  end
end
