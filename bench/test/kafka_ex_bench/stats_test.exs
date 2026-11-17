defmodule KafkaExBench.StatsTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Stats

  test "percentile uses nearest rank" do
    samples = Enum.to_list(1..100)
    assert Stats.percentile(samples, 50) == 50
    assert Stats.percentile(samples, 99) == 99
    assert Stats.percentile(samples, 100) == 100
    assert Stats.percentile([7], 99) == 7
    assert Stats.percentile([], 99) == nil
  end

  test "percentile is exact on integer ranks" do
    assert Stats.percentile(Enum.to_list(1..100), 55) == 55
    assert Stats.percentile(Enum.to_list(1..100), 7) == 7
    assert Stats.percentile(Enum.to_list(1..1000), 99) == 990
    assert Stats.percentile([5, 1, 4, 2, 3], 50) == 3
    assert Stats.percentile([1, 2], 0) == 1
  end

  test "percentile accepts only integer p in 0..100" do
    assert_raise FunctionClauseError, fn -> Stats.percentile([1], 99.9) end
    assert_raise FunctionClauseError, fn -> Stats.percentile([1], 101) end
    assert_raise FunctionClauseError, fn -> Stats.percentile([1], -1) end
  end

  test "median of odd and even lists" do
    assert Stats.median([3, 1, 2]) == 2
    assert Stats.median([4, 1, 3, 2]) == 2.5
    assert Stats.median([]) == nil
  end

  test "cv is sample stddev over mean" do
    assert_in_delta Stats.cv([10, 12, 14]), 2 / 12, 1.0e-9
    assert Stats.cv([5]) == nil
    assert Stats.cv([0, 0]) == nil
  end
end
