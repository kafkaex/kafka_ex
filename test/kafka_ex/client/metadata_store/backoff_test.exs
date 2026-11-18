defmodule KafkaEx.Client.MetadataStore.BackoffTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.MetadataStore.Backoff

  @topics %{min_interval: 100, base: 100, cap: 1_000}
  @coordinator %{min_interval: 100, base: 500, cap: 1_000}
  @samples 1_000

  test "delay/3 stays within ±20 % of the capped exponential" do
    for _ <- 1..@samples do
      assert Backoff.delay(1, 100, 1_000) in 80..120
      assert Backoff.delay(Enum.random(5..64), 100, 1_000) in 800..1_200
    end
  end

  test "a leaderless key's schedule fits at most 9 requests in 5 000 ms" do
    for _ <- 1..@samples do
      times = requests(@topics, 5_000)
      assert length(times) <= 9

      for {gap, failures} <- Enum.with_index(gaps(times), 1) do
        nominal = min(100 * Integer.pow(2, failures - 1), 1_000)
        assert gap >= max(100, floor(nominal * 0.8)) and gap <= max(100, ceil(nominal * 1.2))
      end
    end
  end

  test "a down coordinator's schedule never has two requests closer than 400 ms" do
    for _ <- 1..@samples do
      gaps = gaps(requests(@coordinator, 10_000))

      assert Enum.all?(gaps, &(&1 >= 400))
      assert gaps |> Enum.drop(1) |> Enum.all?(&(&1 >= 800))
    end
  end

  test "an :ok outcome resets failures and leaves only min_interval" do
    bad = Enum.reduce(1..4, nil, fn _, entry -> Backoff.next_at(entry, :bad, 0, @topics) end)
    assert bad.failures == 4

    assert Backoff.next_at(bad, :ok, 7_000, @topics) == %{failures: 0, next_at: 7_100}
    assert Backoff.next_at(nil, :ok, 7_000, @topics) == %{failures: 0, next_at: 7_100}
  end

  test "a :failed outcome keeps failures and leaves only min_interval" do
    bad = Enum.reduce(1..4, nil, fn _, entry -> Backoff.next_at(entry, :bad, 0, @topics) end)

    assert Backoff.next_at(bad, :failed, 7_000, @topics) == %{failures: 4, next_at: 7_100}
    assert Backoff.next_at(nil, :failed, 7_000, @topics) == %{failures: 0, next_at: 7_100}
  end

  defp requests(config, window) do
    Stream.unfold({0, nil}, fn {now, entry} ->
      if now < window do
        entry = Backoff.next_at(entry, :bad, now, config)
        {now, {entry.next_at, entry}}
      end
    end)
    |> Enum.to_list()
  end

  defp gaps(times), do: times |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> b - a end)
end
