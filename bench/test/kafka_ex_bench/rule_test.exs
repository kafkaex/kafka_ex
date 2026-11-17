defmodule KafkaExBench.RuleTest do
  use ExUnit.Case, async: true

  alias KafkaExBench.Rule

  @relative_lower {:p50_us, :lower_better, {:relative}}
  @relative_higher {:throughput_ops, :higher_better, {:relative}}
  @absolute {:gap_us, :lower_better, {:absolute_us, 1_000}}

  defp runs(metric, values), do: Enum.map(values, &(%{valid: true, error: nil} |> Map.put(metric, &1)))
  defp p50(values), do: runs(:p50_us, values)

  defp steady(center, spread), do: Enum.map(-2..2, &(center + &1 * spread))

  describe "relative gate" do
    test "fail when the candidate is worse beyond the threshold" do
      result = Rule.verdict(p50(steady(1000, 1)), p50(steady(1100, 1)), @relative_lower)

      assert result.verdict == :fail
      assert_in_delta result.delta_pct, 10.0, 0.01
      assert result.base_median == 1000
      assert result.cand_median == 1100
      assert result.limit_pct == 5.0
    end

    test "pass when within the threshold" do
      result = Rule.verdict(p50(steady(1000, 1)), p50(steady(1030, 1)), @relative_lower)
      assert result.verdict == :pass
    end

    test "exactly at the limit passes" do
      result = Rule.verdict(p50(List.duplicate(100, 5)), p50(List.duplicate(105, 5)), @relative_lower)
      assert result.delta_pct == 5.0
      assert result.verdict == :pass
    end

    test "just beyond the limit fails" do
      result = Rule.verdict(p50(List.duplicate(100, 5)), p50(List.duplicate(105.5, 5)), @relative_lower)
      assert result.verdict == :fail
    end

    test "improvement when better beyond the limit" do
      result = Rule.verdict(p50(steady(1000, 1)), p50(steady(900, 1)), @relative_lower)
      assert result.verdict == :improvement
      assert result.delta_pct < 0
    end

    test "A/A-like small shift below the limit passes, not improvement" do
      base = p50([99.2, 100, 100.8, 99.6, 100.4])
      cand = p50([96.6, 97.4, 98.2, 97.0, 97.8])
      result = Rule.verdict(base, cand, @relative_lower)

      assert_in_delta result.delta_pct, -2.6, 0.01
      assert result.noise_pct < 1.0
      assert result.verdict == :pass
    end

    test "improvement only beyond the same limit as a regression" do
      base = p50(List.duplicate(100, 5))
      assert Rule.verdict(base, p50(List.duplicate(95.5, 5)), @relative_lower).verdict == :pass
      assert Rule.verdict(base, p50(List.duplicate(94.5, 5)), @relative_lower).verdict == :improvement
    end

    test "small improvement within noise passes" do
      base = p50([100, 102, 98, 101, 99])
      cand = p50([99, 101, 97, 100, 98])
      assert Rule.verdict(base, cand, @relative_lower).verdict == :pass
    end

    test "higher_better direction: a drop is positive delta and fails" do
      base = runs(:throughput_ops, steady(1000, 1))
      cand = runs(:throughput_ops, steady(900, 1))
      result = Rule.verdict(base, cand, @relative_higher)

      assert result.verdict == :fail
      assert_in_delta result.delta_pct, 10.0, 0.01
    end

    test "higher_better direction: a rise is an improvement" do
      base = runs(:throughput_ops, steady(1000, 1))
      cand = runs(:throughput_ops, steady(1100, 1))
      assert Rule.verdict(base, cand, @relative_higher).verdict == :improvement
    end

    test "limit widens to twice the noise" do
      base = p50([100, 100, 100, 100, 100])
      cand = p50([108, 112, 110, 108, 112])
      result = Rule.verdict(base, cand, @relative_lower, threshold_pct: 5.0)

      assert result.noise_pct > 1.0
      assert result.limit_pct == max(5.0, 2 * result.noise_pct)
      assert result.verdict == :fail
    end

    test "noise-widened limit flips a fail into a pass" do
      base = p50(List.duplicate(100, 5))
      noisy = p50([101, 104, 107, 110, 113])
      calm = p50([106.8, 107, 107, 107, 107.2])

      widened = Rule.verdict(base, noisy, @relative_lower)
      assert widened.limit_pct > 7.0
      assert widened.verdict == :pass

      tight = Rule.verdict(base, calm, @relative_lower)
      assert tight.limit_pct == 5.0
      assert tight.verdict == :fail
    end

    test "threshold is configurable" do
      base = p50(List.duplicate(100, 5))
      cand = p50(List.duplicate(108, 5))
      assert Rule.verdict(base, cand, @relative_lower).verdict == :fail
      assert Rule.verdict(base, cand, @relative_lower, threshold_pct: 10.0).verdict == :pass
    end

    test "a gate's own threshold replaces the global one" do
      base = p50(List.duplicate(100, 5))
      cand = p50(List.duplicate(108, 5))
      own = {:p50_us, :lower_better, {:relative, 10.0}}

      assert Rule.verdict(base, cand, @relative_lower).verdict == :fail
      assert Rule.verdict(base, cand, own).verdict == :pass
      assert Rule.verdict(base, cand, own, threshold_pct: 1.0).verdict == :pass
      assert Rule.verdict(base, cand, own).limit_pct == 10.0
      assert Rule.verdict(base, p50(List.duplicate(112, 5)), own).verdict == :fail
      assert Rule.verdict(base, p50(List.duplicate(88, 5)), own).verdict == :improvement
    end

    test "a gate's own threshold still widens to twice the noise" do
      base = p50(List.duplicate(100, 5))
      cand = p50([101, 104, 107, 110, 113])
      result = Rule.verdict(base, cand, {:p50_us, :lower_better, {:relative, 2.0}})
      assert result.limit_pct == 2 * result.noise_pct
    end

    test "noise above 10 percent is inconclusive on either side" do
      noisy = p50([50, 100, 150, 100, 100])
      calm = p50(steady(100, 1))

      assert Rule.verdict(noisy, calm, @relative_lower).verdict == :inconclusive
      assert Rule.verdict(calm, noisy, @relative_lower).verdict == :inconclusive
      assert Rule.verdict(noisy, calm, @relative_lower).noise_pct > 10.0
    end

    test "noise is the larger CV of both sides" do
      base = p50(steady(1000, 1))
      cand = p50(steady(1000, 20))
      result = Rule.verdict(base, cand, @relative_lower)
      assert_in_delta result.noise_pct, 100 * KafkaExBench.Stats.cv(Enum.map(cand, & &1.p50_us)), 1.0e-9
    end

    test "zero baseline median is inconclusive" do
      result = Rule.verdict(p50(List.duplicate(0, 5)), p50(List.duplicate(5, 5)), @relative_lower)
      assert result.verdict == :inconclusive
    end
  end

  describe "validity" do
    test "baseline with too few valid runs is inconclusive" do
      base = p50(steady(1000, 1)) |> List.update_at(0, &Map.put(&1, :valid, false)) |> Enum.take(4)
      result = Rule.verdict(base, p50(steady(1000, 1)), @relative_lower)

      assert result.verdict == :inconclusive
      assert result.reason =~ "baseline invalid"
    end

    test "candidate with too few valid runs fails and names the errors" do
      bad = %{valid: false, error: "timeout in run", p50_us: nil}
      cand = [bad, bad | Enum.take(p50(steady(1000, 1)), 3)]
      result = Rule.verdict(p50(steady(1000, 1)), cand, @relative_lower)

      assert result.verdict == :fail
      assert result.reason =~ "candidate invalid runs"
      assert result.reason =~ "timeout in run"
    end

    test "candidate with more unusable runs than the base is inconclusive" do
      bad = %{valid: false, error: "timeout", p50_us: nil}
      cand = [bad | p50(steady(1000, 1))]
      result = Rule.verdict(p50(steady(1000, 1)), cand, @relative_lower)

      assert result.verdict == :inconclusive
      assert result.reason =~ "candidate had 1 unusable of 6 runs (base 0 of 5)"
      assert result.reason =~ "(usable: base 5/5, candidate 5/6)"
      assert {result.base_usable, result.base_total, result.cand_usable, result.cand_total} == {5, 5, 5, 6}
    end

    test "equal unusable counts on both sides still decide" do
      bad = %{valid: false, error: "timeout", p50_us: nil}
      base = [bad | p50(steady(1000, 1))]
      cand = [bad | p50(steady(1000, 1))]
      assert Rule.verdict(base, cand, @relative_lower).verdict == :pass
    end

    test "every reason ends with usable counts" do
      result = Rule.verdict(p50(steady(1000, 1)), p50(steady(1000, 1)), @relative_lower)
      assert result.reason =~ ~r/\(usable: base 5\/5, candidate 5\/5\)$/
    end

    test "baseline is checked before the candidate" do
      result = Rule.verdict([], [], @relative_lower)
      assert result.verdict == :inconclusive
    end

    test "runs lacking the metric do not count as valid" do
      base = p50(steady(1000, 1))
      cand = Enum.take(p50(steady(1000, 1)), 4) ++ [%{valid: true, error: nil, p50_us: nil}]
      assert Rule.verdict(base, cand, @relative_lower).verdict == :fail
    end

    test "min_valid is configurable" do
      base = p50(Enum.take(steady(1000, 1), 3))
      cand = p50(Enum.take(steady(1000, 1), 3))

      assert Rule.verdict(base, cand, @relative_lower).verdict == :inconclusive
      assert Rule.verdict(base, cand, @relative_lower, min_valid: 3).verdict == :pass
    end
  end

  describe "absolute gate" do
    test "worse beyond the limit fails" do
      result =
        Rule.verdict(runs(:gap_us, [1000, 1100, 900]), runs(:gap_us, [2500, 2600, 2400]), @absolute, min_valid: 3)

      assert result.verdict == :fail
    end

    test "better beyond the limit is an improvement" do
      result =
        Rule.verdict(runs(:gap_us, [5000, 5100, 4900]), runs(:gap_us, [1000, 1100, 900]), @absolute, min_valid: 3)

      assert result.verdict == :improvement
    end

    test "exactly at the limit passes in both directions" do
      assert Rule.verdict(runs(:gap_us, [1000]), runs(:gap_us, [2000]), @absolute, min_valid: 1).verdict == :pass
      assert Rule.verdict(runs(:gap_us, [2000]), runs(:gap_us, [1000]), @absolute, min_valid: 1).verdict == :pass
    end

    test "higher_better absolute direction flips the sign" do
      gate = {:ops, :higher_better, {:absolute_us, 10}}
      result = Rule.verdict(runs(:ops, [100]), runs(:ops, [80]), gate, min_valid: 1)
      assert result.verdict == :fail
    end

    test "noise as a percentage is reported but not used" do
      gate = {:gap_us, :lower_better, {:absolute_us, 5_000}}
      base = runs(:gap_us, [100, 1000, 3000])
      result = Rule.verdict(base, base, gate, min_valid: 3)

      assert result.noise_pct > 10.0
      assert result.verdict == :pass
    end

    test "run-to-run spread above the limit on either side is inconclusive" do
      wide = runs(:gap_us, [100, 1000, 1200])
      calm = runs(:gap_us, [1000, 1100, 900])

      for {base, cand} <- [{wide, calm}, {calm, wide}, {wide, wide}] do
        result = Rule.verdict(base, cand, @absolute, min_valid: 3)
        assert result.verdict == :inconclusive
        assert result.reason =~ "run-to-run spread 1100.00 us exceeds the absolute limit 1000"
      end
    end

    test "a spread inside the limit still decides" do
      result = Rule.verdict(runs(:gap_us, [1000, 1900]), runs(:gap_us, [3500, 4400]), @absolute, min_valid: 2)
      assert result.verdict == :fail
    end

    test "the spread guard uses per-run medians of list samples" do
      gate = {:gap_us, :lower_better, {:absolute_us, 1_000}}
      base = runs(:gap_us, [[0, 5_000, 100], [100]])
      cand = runs(:gap_us, [[1_000], [1_100]])
      assert Rule.verdict(base, cand, gate, min_valid: 2).verdict == :pass
    end

    test "list extras use the median of each run's list" do
      gate = {:gap_other_leaders_us, :lower_better, {:absolute_us, 250_000}}
      base = runs(:gap_other_leaders_us, [[100_000, 120_000, 110_000]])
      cand = runs(:gap_other_leaders_us, [[400_000, 500_000, 450_000]])
      result = Rule.verdict(base, cand, gate, min_valid: 1)

      assert result.base_median == 110_000
      assert result.cand_median == 450_000
      assert result.verdict == :fail
    end

    test "a run with an empty list does not count as valid" do
      gate = {:gap_other_leaders_us, :lower_better, {:absolute_us, 250_000}}
      base = runs(:gap_other_leaders_us, [[1]])
      cand = runs(:gap_other_leaders_us, [[]])
      assert Rule.verdict(base, cand, gate, min_valid: 1).verdict == :fail
    end
  end

  describe "usable-only gate" do
    @usable_only {:gap_us, :lower_better, {:usable_only}}

    test "passes whatever the size of the gap and says the value is not judged" do
      result =
        Rule.verdict(runs(:gap_us, [1000, 1100, 900]), runs(:gap_us, [9000, 6200, 7000]), @usable_only, min_valid: 3)

      assert result.verdict == :pass
      assert result.limit_pct == nil
      assert result.reason =~ "usable runs only, value not judged"
    end

    test "still applies the three validity branches" do
      ok = runs(:gap_us, [1000, 1100, 900])
      broken = [%{valid: false, error: "no success", gap_us: nil} | Enum.take(ok, 2)]

      assert Rule.verdict(ok, Enum.take(broken, 2), @usable_only, min_valid: 3).verdict == :fail
      assert Rule.verdict(Enum.take(ok, 2), ok, @usable_only, min_valid: 3).verdict == :inconclusive
      assert Rule.verdict(ok ++ ok, broken ++ ok, @usable_only, min_valid: 3).verdict == :inconclusive
    end
  end

  test "noise is reported per side next to the combined noise" do
    base = runs(:gap_us, [100, 100, 100])
    cand = runs(:gap_us, [90, 100, 110])
    result = Rule.verdict(base, cand, {:gap_us, :lower_better, {:relative}}, min_valid: 3)

    assert result.base_noise_pct == 0.0
    assert result.cand_noise_pct > 5.0
    assert result.noise_pct == result.cand_noise_pct
  end

  describe "aggregation" do
    test "job verdict is the worst gate result" do
      assert Rule.job_verdict([%{verdict: :pass}, %{verdict: :improvement}]) == :pass
      assert Rule.job_verdict([%{verdict: :pass}, %{verdict: :inconclusive}]) == :inconclusive
      assert Rule.job_verdict([%{verdict: :inconclusive}, %{verdict: :fail}, %{verdict: :pass}]) == :fail
      assert Rule.job_verdict([%{verdict: :improvement}]) == :improvement
    end

    test "overall is the worst job verdict" do
      assert Rule.overall([:improvement, :pass]) == :pass
      assert Rule.overall([:pass, :inconclusive, :improvement]) == :inconclusive
      assert Rule.overall([:pass, :fail, :inconclusive]) == :fail
    end

    test "empty input fails closed" do
      assert Rule.job_verdict([]) == :inconclusive
      assert Rule.overall([]) == :inconclusive
    end

    test "exit_status rejects unknown verdicts" do
      verdict = Enum.random([:bogus])
      assert_raise FunctionClauseError, fn -> Rule.exit_status(verdict) end
    end

    test "exit status is non-zero only for fail and inconclusive" do
      assert Rule.exit_status(:pass) == 0
      assert Rule.exit_status(:improvement) == 0
      assert Rule.exit_status(:fail) == 1
      assert Rule.exit_status(:inconclusive) == 1
    end
  end

  describe "informational" do
    test "returns the numbers without a verdict" do
      result = Rule.informational(p50(steady(1000, 1)), p50(steady(1100, 1)), :p50_us)

      assert result.base_median == 1000
      assert result.cand_median == 1100
      assert_in_delta result.delta_pct, 10.0, 0.01
      assert result.noise_pct > 0
      refute Map.has_key?(result, :verdict)
    end

    test "a known direction makes positive delta mean worse" do
      higher =
        Rule.informational(runs(:throughput_ops, [100]), runs(:throughput_ops, [80]), :throughput_ops, :higher_better)

      assert_in_delta higher.delta_pct, 20.0, 0.01
      assert higher.direction == :higher_better

      lower = Rule.informational(p50(steady(1000, 1)), p50(steady(900, 1)), :p50_us, :lower_better)
      assert_in_delta lower.delta_pct, -10.0, 0.01
    end

    test "an unknown direction keeps the raw delta and says so" do
      result = Rule.informational(p50(steady(1000, 1)), p50(steady(900, 1)), :p50_us)
      assert_in_delta result.delta_pct, -10.0, 0.01
      assert result.direction == nil
    end

    test "ignores invalid runs and handles empty sides" do
      result = Rule.informational([%{valid: false, p50_us: nil}], [], :p50_us)

      assert result.base_median == nil
      assert result.cand_median == nil
      assert result.delta_pct == nil
      assert result.noise_pct == nil
    end
  end
end
