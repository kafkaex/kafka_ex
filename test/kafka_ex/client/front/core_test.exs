defmodule KafkaEx.Client.Front.CoreTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Front.Core
  alias KafkaEx.TestSupport.FrontPolicies

  @b1 FrontPolicies.conn(1, :data)
  @hit {:node, @b1, %{node: 1, replica?: false, epoch: 0}}

  defp admit(core, id, class, now \\ 0, deadline \\ 5_000) do
    Core.handle(core, {now, {:admit, id, FrontPolicies.policy(class, 0), deadline}})
  end

  test "admission arms the deadline and resolves" do
    assert {_core, [{:timer, {:deadline, 1}, 5_000}, {:resolve, 1, _}]} = admit(Core.new(seed: {1, 2, 3}), 1, :fetch)
  end

  test "a second produce to the same partition waits for the first to end" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, [{:timer, {:deadline, 2}, _}]} = admit(core, 2, :produce)
    assert Core.gate(core, {"t", 0}) == %{holders: [1], waiters: [2]}
    assert {_core, [{:end, 1, _}, {:resolve, 2, _}]} = Core.handle(core, {5, {:done, ref, {:ok, :answer}}})
  end

  test "a produce that timed out keeps its slot until :dispatch_done" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :produce)
    {core, effects} = Core.handle(core, {5, {:done, ref, {:error, :timeout}}})
    refute Enum.any?(effects, &match?({:resolve, 2, _}, &1))
    assert Core.gate(core, {"t", 0}).holders == [{:wire, ref}]
    assert {_core, [{:resolve, 2, _}]} = Core.handle(core, {9, {:dispatch_done, ref}})
  end

  test "a connection that goes down frees the slots of its timed-out produces" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = Core.handle(core, {5, {:done, ref, {:error, :timeout}}})
    {core, []} = Core.handle(core, {6, {:conn_down, @b1, true}})
    assert Core.idle?(core)
  end

  test ":serial starts the next entry only when the active one ends" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}, concurrency: :serial), 1, :fetch)
    {core, [{:timer, {:deadline, 2}, _}]} = admit(core, 2, :fetch)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    assert {_core, [{:end, 1, _}, {:resolve, 2, _}]} = Core.handle(core, {5, {:done, ref, {:ok, :answer}}})
  end

  test "a waiting entry ends at its deadline or when its caller goes down" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}, concurrency: :serial), 1, :fetch)
    {core, _} = admit(core, 2, :fetch, 0, 100)
    {core, _} = admit(core, 3, :fetch)
    {core, [{:end, 2, {:reply, {:error, :timeout}}}]} = Core.handle(core, {100, {:tick, {:deadline, 2}}})
    {core, [{:end, 3, :caller_down}]} = Core.handle(core, {101, {:caller_down, 3}})
    assert Core.started(core) == [1]
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {102, {:routed, 1, @hit}})
    {core, [{:end, 1, _}]} = Core.handle(core, {103, {:done, ref, {:ok, :answer}}})
    assert Core.idle?(core)
  end

  test "an encode failure ends the produce and frees its slot at once" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :produce)

    assert {core,
            [
              {:classified, 1, :timeout, :not_retryable},
              {:end, 1, {:reply, {:error, :timeout}}},
              {:resolve, 2, _}
            ]} = Core.handle(core, {5, {:encode_failed, ref}})

    assert Core.gate(core, {"t", 0}) == %{holders: [2], waiters: []}
  end

  test "an encode failure for a produce that already ended frees its wire slot" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :produce)
    {core, [{:end, 1, :caller_down}]} = Core.handle(core, {1, {:caller_down, 1}})
    assert Core.gate(core, {"t", 0}).holders == [{:wire, ref}]
    assert {core, [{:resolve, 2, _}]} = Core.handle(core, {2, {:encode_failed, ref}})
    assert Core.gate(core, {"t", 0}) == %{holders: [2], waiters: []}
  end

  test "a parked entry with a written failure ends at the deadline with that error" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce, 0, 100)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, effects} = Core.handle(core, {5, {:done, ref, {:error, :not_leader_for_partition}}})
    assert {:resolve, 1, _} = List.last(effects)

    assert {core, [{:end, 1, {:reply, {:error, :not_leader_for_partition}}}]} =
             Core.handle(core, {100, {:tick, {:deadline, 1}}})

    assert Core.idle?(core)
  end

  test "a woken produce whose caller died before the send frees its slot on :dispatch_done" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce)
    {core, [{:dispatch, %{ref: first}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :produce)
    {core, _} = Core.handle(core, {5, {:done, first, {:error, :not_leader_for_partition}}})
    {core, [{:resolve, 1, _}]} = Core.handle(core, {6, {:swapped, %{"t" => 1}}})
    newer = {:node, @b1, %{node: 1, replica?: false, epoch: 1}}
    {core, [{:dispatch, %{ref: unsent, woken?: true}}]} = Core.handle(core, {7, {:routed, 1, newer}})
    {core, [{:end, 1, :caller_down}]} = Core.handle(core, {8, {:caller_down, 1}})
    assert {core, [{:resolve, 2, _}]} = Core.handle(core, {8, {:dispatch_done, unsent}})
    assert Core.gate(core, {"t", 0}) == %{holders: [2], waiters: []}
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {9, {:routed, 2, @hit}})
    {core, [{:end, 2, _}]} = Core.handle(core, {10, {:done, ref, {:ok, :answer}}})
    assert Core.idle?(core)
  end

  test "a timeout for a produce that already ended holds its slot until :dispatch_done" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :produce)
    {core, [{:end, 1, :caller_down}]} = Core.handle(core, {1, {:caller_down, 1}})
    assert {core, []} = Core.handle(core, {2, {:done, ref, {:error, :timeout}}})
    assert Core.gate(core, {"t", 0}).holders == [{:wire, ref}]
    assert {_core, [{:resolve, 2, _}]} = Core.handle(core, {3, {:dispatch_done, ref}})
  end

  test "a produce its connection never wrote before the deadline frees its slot on :dispatch_done" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce, 0, 100)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :produce)
    assert {core, []} = Core.handle(core, {100, {:dispatch_done, ref}})

    assert {core, [{:end, 1, {:reply, {:error, :timeout}}}, {:resolve, 2, _}]} =
             Core.handle(core, {100, {:tick, {:deadline, 1}}})

    assert Core.gate(core, {"t", 0}) == %{holders: [2], waiters: []}
    assert {_core, [{:dispatch, %{id: 2}}]} = Core.handle(core, {100, {:routed, 2, @hit}})
  end

  test "an answer for a produce that already ended frees its slot at once" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :produce)
    {core, [{:end, 1, :caller_down}]} = Core.handle(core, {1, {:caller_down, 1}})
    assert {_core, [{:resolve, 2, _}]} = Core.handle(core, {2, {:done, ref, {:ok, :late}}})
  end

  test "a connection that goes down as its last produce expires leaves no slot held" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce, 0, 100)
    {core, [{:dispatch, _}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, [{:end, 1, {:reply, {:error, :timeout}}}]} = Core.handle(core, {120, {:conn_down, @b1, true}})
    assert Core.idle?(core)
  end

  test "produce waiters are granted in arrival order and an expired one leaves the queue" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :produce)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :produce, 0, 100)
    {core, _} = admit(core, 3, :produce)
    assert Core.gate(core, {"t", 0}) == %{holders: [1], waiters: [2, 3]}
    {core, [{:end, 2, {:reply, {:error, :timeout}}}]} = Core.handle(core, {100, {:tick, {:deadline, 2}}})
    assert Core.gate(core, {"t", 0}) == %{holders: [1], waiters: [3]}
    assert {_core, [{:end, 1, _}, {:resolve, 3, _}]} = Core.handle(core, {101, {:done, ref, {:ok, :answer}}})
  end

  test "an acks 0 produce frees its slot when its write completes" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :acks0)
    {core, [{:dispatch, %{ref: ref, reply?: false}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :acks0)

    assert {_core, [{:end, 1, {:reply, {:ok, :no_response}}}, {:resolve, 2, _}]} =
             Core.handle(core, {1, {:done, ref, {:ok, :no_response}}})
  end

  test "an acks 0 produce that timed out holds its slot until :dispatch_done" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :acks0)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, _} = admit(core, 2, :acks0)
    {core, effects} = Core.handle(core, {1, {:done, ref, {:error, :timeout}}})
    assert [{:end, 1, {:reply, {:error, :timeout}}}] = Enum.filter(effects, &match?({:end, _, _}, &1))
    refute Enum.any?(effects, &match?({:resolve, 2, _}, &1))
    assert Core.gate(core, {"t", 0}).holders == [{:wire, ref}]
    assert {_core, [{:resolve, 2, _}]} = Core.handle(core, {2, {:dispatch_done, ref}})
  end

  test "a :serial front with an active entry is not idle" do
    core = Core.new(seed: {1, 2, 3}, concurrency: :serial)
    assert Core.idle?(core)
    refute Core.idle?(%{core | active: 1})
  end

  test "a reply for an entry that already ended changes nothing" do
    {core, _} = admit(Core.new(seed: {1, 2, 3}), 1, :fetch)
    {core, [{:dispatch, %{ref: ref}}]} = Core.handle(core, {0, {:routed, 1, @hit}})
    {core, [{:end, 1, :caller_down}]} = Core.handle(core, {1, {:caller_down, 1}})
    assert Core.handle(core, {2, {:done, ref, {:ok, :late}}}) == {core, []}
  end
end
