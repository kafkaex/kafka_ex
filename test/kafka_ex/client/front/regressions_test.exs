defmodule KafkaEx.Client.Front.RegressionsTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Front.Core
  alias KafkaEx.TestSupport.FrontPolicies

  @b1_coordinator FrontPolicies.conn(1, :coordinator)
  @b1_data FrontPolicies.conn(1, :data)

  test "a coordinator error is not re-sent at once because another key's epoch moved" do
    core = Core.new(seed: {1, 2, 3})

    {core, [_deadline, {:resolve, 1, _}]} =
      Core.handle(core, {0, {:admit, 1, FrontPolicies.policy(:heartbeat, 0), 5_000}})

    {core, [{:dispatch, %{ref: ref}}]} =
      Core.handle(core, {0, {:routed, 1, {:node, @b1_coordinator, %{node: 1, replica?: false, epoch: 0}}}})

    {core, effects} = Core.handle(core, {10, {:done, ref, {:error, :coordinator_not_available}}})
    assert {:resolve, 1, _} = List.last(effects)

    assert {core, []} = Core.handle(core, {10, {:swapped, %{"t" => 1}}})

    {_core, effects} =
      Core.handle(core, {10, {:routed, 1, {:node, @b1_coordinator, %{node: 1, replica?: false, epoch: 0}}}})

    assert effects == []
  end

  test "a connection that goes down as its last produce expires leaves no gate slot held" do
    core = Core.new(seed: {1, 2, 3})
    {core, _} = Core.handle(core, {0, {:admit, 1, FrontPolicies.policy(:produce, 0), 100}})

    {core, [{:dispatch, %{ref: _ref}}]} =
      Core.handle(core, {0, {:routed, 1, {:node, @b1_data, %{node: 1, replica?: false, epoch: 0}}}})

    {core, [{:end, 1, {:reply, {:error, :timeout}}}]} = Core.handle(core, {120, {:conn_down, @b1_data, true}})
    assert Core.idle?(core)
  end

  test "a produce queued behind :serial that expires before reaching the gate ends cleanly" do
    core = Core.new(seed: {1, 2, 3}, concurrency: :serial)
    {core, _} = Core.handle(core, {0, {:admit, 1, FrontPolicies.policy(:fetch, 0), 5_000}})
    {core, _} = Core.handle(core, {0, {:admit, 2, FrontPolicies.policy(:produce, 0), 100}})
    assert {core, [{:end, 2, {:reply, {:error, :timeout}}}]} = Core.handle(core, {100, {:tick, {:deadline, 2}}})
    assert Core.gate(core, {"t", 0}) == %{holders: [], waiters: []}
  end

  test "a second reply for a timed-out produce does not free its slot before :dispatch_done" do
    core = Core.new(seed: {1, 2, 3})
    {core, _} = Core.handle(core, {0, {:admit, 1, FrontPolicies.policy(:produce, 0), 5_000}})

    {core, [{:dispatch, %{ref: ref}}]} =
      Core.handle(core, {0, {:routed, 1, {:node, @b1_data, %{node: 1, replica?: false, epoch: 0}}}})

    {core, _} = Core.handle(core, {5, {:done, ref, {:error, :timeout}}})
    {core, []} = Core.handle(core, {6, {:done, ref, {:ok, :late}}})
    assert Core.gate(core, {"t", 0}).holders == [{:wire, ref}]
  end
end
