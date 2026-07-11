defmodule KafkaEx.API.CoordinatorTimeoutTest do
  @moduledoc """
  JoinGroup/SyncGroup derive a per-attempt `:network_timeout` (the socket-recv
  deadline) sized to how long the broker legitimately holds the response, and
  thread it through as a control opt (popped by the client before building the
  protocol request). Regression coverage for the consumer-group cold-start bug:
  the old code let these fall back to the 1000ms generic timeout while the broker
  held JoinGroup for >= group.initial.rebalance.delay.ms.
  """
  use ExUnit.Case, async: true

  alias KafkaEx.Test.CapturingMockClient

  describe "join_group/4" do
    test "derives network_timeout = rebalance_timeout + 5000 (no explicit timeout)" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.join_group(client, "g", "", rebalance_timeout: 90_000)

      assert [{:join_group, "g", "", opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :network_timeout) == 95_000
      refute Keyword.has_key?(opts, :timeout)
    end

    test "uses the default rebalance_timeout when unset" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.join_group(client, "g", "")

      assert [{:join_group, _, _, opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :network_timeout) == 65_000
    end

    # Backward-compat: pre-1.1 callers passed :timeout to bound the join. It is
    # now honored as the per-attempt deadline override (rather than silently dropped).
    test "honors an explicit :timeout override" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.join_group(client, "g", "", timeout: 12_345)

      assert [{:join_group, _, _, opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :network_timeout) == 12_345
      refute Keyword.has_key?(opts, :timeout)
    end

    test ":network_timeout wins over :timeout" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.join_group(client, "g", "", network_timeout: 111, timeout: 222)

      assert [{:join_group, _, _, opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :network_timeout) == 111
    end
  end

  describe "sync_group/5" do
    test "derives network_timeout from the caller :timeout (session + padding) and drops :timeout" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.sync_group(client, "g", 1, "m", timeout: 40_000)

      assert [{:sync_group, "g", 1, "m", opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :network_timeout) == 40_000
      refute Keyword.has_key?(opts, :timeout)
    end

    test "falls back to the default when no timeout is supplied" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.sync_group(client, "g", 1, "m")

      assert [{:sync_group, _, _, _, opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :network_timeout) == 35_000
    end

    test ":network_timeout wins over :timeout" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.sync_group(client, "g", 1, "m", network_timeout: 111, timeout: 222)

      assert [{:sync_group, _, _, _, opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :network_timeout) == 111
    end
  end

  describe "create_topics/4 and delete_topics/4" do
    test "size the per-attempt socket-recv to the broker op timeout (timeout + buffer)" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.create_topics(client, ["t"], 10_000)
      assert {:ok, _} = KafkaEx.API.delete_topics(client, ["t"], 30_000)

      calls = CapturingMockClient.calls(client)
      assert {:create_topics, ["t"], 10_000, create_opts} = Enum.at(calls, 0)
      assert {:delete_topics, ["t"], 30_000, delete_opts} = Enum.at(calls, 1)
      # broker timeout + @coordinator_call_timeout_buffer (5_000)
      assert Keyword.get(create_opts, :network_timeout) == 15_000
      assert Keyword.get(delete_opts, :network_timeout) == 35_000
    end
  end

  describe "heartbeat/5" do
    test "uses the caller-supplied short network_timeout (heartbeat_interval), not the generic default" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.heartbeat(client, "g", "m", 1, network_timeout: 5_000)

      assert [{:heartbeat, "g", "m", 1, opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :network_timeout) == 5_000
    end

    test "falls back to the generic :request_timeout when no network_timeout is supplied" do
      {:ok, client} = CapturingMockClient.start_link()

      assert {:ok, _} = KafkaEx.API.heartbeat(client, "g", "m", 1)

      assert [{:heartbeat, _, _, _, opts}] = CapturingMockClient.calls(client)
      assert Keyword.get(opts, :network_timeout) == KafkaEx.Config.request_timeout()
    end
  end
end
