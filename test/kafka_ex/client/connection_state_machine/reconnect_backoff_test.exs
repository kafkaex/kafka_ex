defmodule KafkaEx.Client.ConnectionStateMachine.ReconnectBackoffTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.ConnectionStateMachine.ReconnectBackoff

  @key {"broker", 9092}

  test "the first claim is :ok and an immediate second one waits one first-failure delay" do
    backoff = ReconnectBackoff.new()

    assert ReconnectBackoff.claim(backoff, @key, 1_000) == :ok
    assert {:wait, ms} = ReconnectBackoff.claim(backoff, @key, 1_000)
    assert ms in 80..120
  end

  test "after connected/3 a claim at the same time is :ok" do
    backoff = ReconnectBackoff.new()
    :ok = ReconnectBackoff.claim(backoff, @key, 1_000)

    assert ReconnectBackoff.connected(backoff, @key, 1_000)
    assert ReconnectBackoff.claim(backoff, @key, 1_000) == :ok
  end

  test "stable/2 resets the failure count" do
    backoff = ReconnectBackoff.new()
    for now <- [0, 1_000, 2_000, 3_000], do: :ok = ReconnectBackoff.claim(backoff, @key, now)
    ReconnectBackoff.stable(backoff, @key)

    :ok = ReconnectBackoff.claim(backoff, @key, 10_000)
    assert {:wait, ms} = ReconnectBackoff.claim(backoff, @key, 10_000)
    assert ms in 80..120
  end

  test "delay/1 doubles from [80, 120] and is capped to [400, 500]" do
    for failures <- 1..10 do
      delays = for _ <- 1..1_000, do: ReconnectBackoff.delay(failures)

      assert Enum.all?(delays, &is_integer/1)
      assert Enum.all?(delays, &(&1 in 80..500))
      if failures == 1, do: assert(Enum.all?(delays, &(&1 in 80..120)))
      if failures >= 4, do: assert(Enum.all?(delays, &(&1 in 400..500)))
    end
  end

  test "50 concurrent claims at one time yield exactly one :ok" do
    backoff = ReconnectBackoff.new()

    results =
      1..50
      |> Enum.map(fn _ -> Task.async(fn -> ReconnectBackoff.claim(backoff, @key, 5_000) end) end)
      |> Task.await_many(5_000)

    assert Enum.count(results, &(&1 == :ok)) == 1
    assert Enum.all?(results, &(&1 == :ok or match?({:wait, _}, &1)))
  end

  test "connected/3 wakes each first connect awaiting a slot on that key once, and no other" do
    backoff = ReconnectBackoff.new()
    :ok = ReconnectBackoff.claim(backoff, @key, 1_000)
    ReconnectBackoff.await_slot(backoff, @key, self())
    other = spawn(fn -> :ok end)
    ReconnectBackoff.await_slot(backoff, {"other", 9092}, other)

    ReconnectBackoff.connected(backoff, @key, 1_010)
    ReconnectBackoff.connected(backoff, @key, 1_020)

    assert_received :reconnect_slot
    refute_received :reconnect_slot
    assert ReconnectBackoff.claim(backoff, @key, 1_020) == :ok
    assert :ets.lookup(backoff, {:first_connect, {"other", 9092}, other}) != []
  end

  test "two keys are independent" do
    backoff = ReconnectBackoff.new()
    :ok = ReconnectBackoff.claim(backoff, @key, 1_000)

    assert ReconnectBackoff.claim(backoff, {"other", 9092}, 1_000) == :ok
  end
end
