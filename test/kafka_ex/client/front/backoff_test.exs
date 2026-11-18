defmodule KafkaEx.Client.Front.BackoffTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias KafkaEx.Client.Front.Backoff

  @seed String.to_integer(System.get_env("KAFKA_EX_PROPERTY_SEED", "1494"))
  @runs String.to_integer(System.get_env("KAFKA_EX_PROPERTY_RUNS", "3000"))

  @config Backoff.default()

  test "the delay doubles per spent attempt and stops at the cap" do
    assert Enum.map(0..5, &Backoff.delay(&1, 0.5, @config)) == [100, 100, 200, 400, 800, 1_000]
  end

  test "the jitter spans 20 % either side and never goes past the cap" do
    assert Enum.map(0..5, &Backoff.floor(&1, @config)) == [80, 80, 160, 320, 640, 800]
    assert Enum.map([0.0, 0.999_999], &Backoff.delay(2, &1, @config)) == [160, 240]
    assert Backoff.delay(5, 0.999_999, @config) == 1_000
  end

  property "a delay stays between the floor and the cap" do
    check all(
            spent <- integer(0..40),
            u <- unit(),
            initial_seed: @seed,
            max_runs: @runs
          ) do
      delay = Backoff.delay(spent, u, @config)
      assert delay >= Backoff.floor(spent, @config)
      assert delay <= @config.cap
    end
  end

  property "a wake is always strictly before the deadline" do
    check all(
            now <- integer(0..10_000),
            budget <- integer(0..3_000),
            spent <- integer(0..5),
            u <- unit(),
            initial_seed: @seed,
            max_runs: @runs
          ) do
      case Backoff.wake_at(now, spent, u, now + budget, @config) do
        {:ok, at} -> assert at < now + budget and at - now >= Backoff.floor(spent, @config)
        :no_time -> assert now + Backoff.delay(spent, u, @config) >= now + budget
      end
    end
  end

  # StreamData's bounded float generator slows down sharply as the size grows; a millionth grid covers [0, 1) as well.
  defp unit, do: map(integer(0..999_999), &(&1 / 1_000_000))
end
