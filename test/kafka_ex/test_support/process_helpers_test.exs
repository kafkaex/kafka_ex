defmodule KafkaEx.TestSupport.ProcessHelpersTest do
  use ExUnit.Case, async: true

  import KafkaEx.TestSupport.ProcessHelpers

  test "stops a live process and returns :ok" do
    {:ok, pid} = Agent.start(fn -> :state end)
    assert stop_safely(pid) == :ok
    refute Process.alive?(pid)
  end

  test "returns :ok on an already-dead pid without raising" do
    {:ok, pid} = Agent.start(fn -> :state end)
    :ok = Agent.stop(pid)
    refute Process.alive?(pid)
    assert stop_safely(pid) == :ok
  end

  test "returns :ok on an unregistered name without raising" do
    assert stop_safely(:no_such_registered_process) == :ok
  end

  test "returns :ok for a :via name whose registry no longer exists (raises ArgumentError, not exit)" do
    # Resolving {:via, Registry, ...} against a torn-down registry raises
    # ArgumentError during name lookup — a raise, not an :exit. Teardown must
    # still yield :ok. (Regression: ManagerRecoverable/StartClient :via teardown.)
    assert stop_safely({:via, Registry, {:no_such_registry, :some_key}}) == :ok
  end
end
