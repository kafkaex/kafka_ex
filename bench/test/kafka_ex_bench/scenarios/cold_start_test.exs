defmodule KafkaExBench.Scenarios.ColdStartTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Scenarios.ColdStart

  defp layout(leaders), do: leaders |> Enum.with_index() |> Map.new(fn {leader, index} -> {index, leader} end)

  describe "evenly_spread?/2" do
    test "100 partitions split 34/33/33 are spread evenly" do
      leaders = List.duplicate(1, 34) ++ List.duplicate(2, 33) ++ List.duplicate(3, 33)

      assert ColdStart.evenly_spread?(layout(leaders), 3)
    end

    test "100 partitions split 40/30/30 are not" do
      leaders = List.duplicate(1, 40) ++ List.duplicate(2, 30) ++ List.duplicate(3, 30)

      refute ColdStart.evenly_spread?(layout(leaders), 3)
    end

    test "10 partitions split 4/3/3 are spread evenly" do
      assert ColdStart.evenly_spread?(layout([1, 2, 3, 1, 2, 3, 1, 2, 3, 1]), 3)
    end

    test "a broker without any leader is not spread evenly" do
      refute ColdStart.evenly_spread?(layout([1, 2, 1, 2, 1, 2]), 3)
    end

    test "a single partition is trivially spread" do
      assert ColdStart.evenly_spread?(layout([2]), 3)
    end

    test "a leaderless partition is not spread evenly" do
      refute ColdStart.evenly_spread?(layout([1, 2, 3, -1, 1, 2]), 3)
    end
  end

  describe "job_specs/0 and params/0" do
    test "one job per partition count with 15 repetitions after one discarded" do
      assert ColdStart.job_specs() == [{"cold_start_p1", 1}, {"cold_start_p10", 1}, {"cold_start_p100", 1}]
      assert ColdStart.params().partition_counts == [1, 10, 100]
      assert ColdStart.params().repetitions == 15
      assert ColdStart.params().discarded_repetitions == 1
    end
  end

  describe "first_success/4" do
    defp scripted(steps) do
      {:ok, agent} = Agent.start_link(fn -> steps end)
      fn -> Agent.get_and_update(agent, fn [next | rest] -> {next, rest} end).() end
    end

    defp far, do: System.monotonic_time(:millisecond) + 10_000

    test "succeeds at once without retries" do
      assert {:ok, at, 0} = ColdStart.first_success(scripted([fn -> {:ok, %{}} end]), 0, far())
      assert is_integer(at)
    end

    test "retries after an error, an exit and a raise, counting each" do
      fetch =
        scripted([
          fn -> {:error, :leader_not_available} end,
          fn -> exit({:timeout, {GenServer, :call, [:client, :fetch, 5_000]}}) end,
          fn -> raise "boom" end,
          fn -> {:ok, %{}} end
        ])

      assert {:ok, _at, 3} = ColdStart.first_success(fetch, 0, far())
    end

    test "past the deadline it fails naming the partition and the last reason, also for an exit" do
      assert {:error, message} =
               ColdStart.first_success(fn -> exit(:client_gone) end, 7, System.monotonic_time(:millisecond) - 1)

      assert message =~ "partition 7 not readable"
      assert message =~ "client_gone"
    end
  end

  test "every job discards one warm-up repetition" do
    jobs = ColdStart.jobs(self(), %{topics: [{1, "t", %{0 => 1}}], profile: :direct})

    assert Enum.all?(jobs, &(&1.discard == 1 and &1.repetitions == 15))
  end

  describe "elapsed_us/2" do
    test "is the latest success minus the start" do
      assert ColdStart.elapsed_us(1_000, [1_500, 4_000, 2_000]) == 3_000
    end
  end
end
