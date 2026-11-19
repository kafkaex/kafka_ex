defmodule KafkaEx.Client.Connection.PacingTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Connection.Pacing

  @key {"broker", 9092}

  test "the first claim is :ok and an immediate second one waits one first-failure delay" do
    pacing = Pacing.new()

    assert Pacing.claim(pacing, @key, 1_000) == :ok
    assert {:wait, ms} = Pacing.claim(pacing, @key, 1_000)
    assert ms in 75..150
  end

  test "after connected/3 a claim at the same time is :ok" do
    pacing = Pacing.new()
    :ok = Pacing.claim(pacing, @key, 1_000)

    assert Pacing.connected(pacing, @key, 1_000)
    assert Pacing.claim(pacing, @key, 1_000) == :ok
  end

  test "stable/2 resets the failure count" do
    pacing = Pacing.new()
    for now <- [0, 1_000, 2_000, 3_000], do: :ok = Pacing.claim(pacing, @key, now)
    Pacing.stable(pacing, @key)

    :ok = Pacing.claim(pacing, @key, 10_000)
    assert {:wait, ms} = Pacing.claim(pacing, @key, 10_000)
    assert ms in 75..150
  end

  test "delay/1 doubles from [75, 150] and is capped to [400, 500]" do
    for failures <- 1..10 do
      delays = for _ <- 1..1_000, do: Pacing.delay(failures)

      assert Enum.all?(delays, &is_integer/1)
      assert Enum.all?(delays, &(&1 in 75..500))
      if failures == 1, do: assert(Enum.all?(delays, &(&1 in 75..150)))
      if failures >= 4, do: assert(Enum.all?(delays, &(&1 in 400..500)))
    end
  end

  test "50 concurrent claims at one time yield exactly one :ok" do
    pacing = Pacing.new()

    results =
      1..50
      |> Enum.map(fn _ -> Task.async(fn -> Pacing.claim(pacing, @key, 5_000) end) end)
      |> Task.await_many(5_000)

    assert Enum.count(results, &(&1 == :ok)) == 1
    assert Enum.all?(results, &(&1 == :ok or match?({:wait, _}, &1)))
  end

  test "two keys are independent" do
    pacing = Pacing.new()
    :ok = Pacing.claim(pacing, @key, 1_000)

    assert Pacing.claim(pacing, {"other", 9092}, 1_000) == :ok
  end
end
