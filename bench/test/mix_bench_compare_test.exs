defmodule Mix.Tasks.Bench.CompareTest do
  use ExUnit.Case, async: true
  alias Mix.Tasks.Bench.Compare

  test "defaults" do
    config = Compare.parse!(~w(--base aaa --candidate bbb --out out))

    assert config == %{
             base: "aaa",
             candidate: "bbb",
             scenarios: ~w(basic mixed one_broker large_fetch cold_start restart_recovery),
             profiles: [:latency, :direct],
             runs: 6,
             threshold_pct: 5.0,
             min_valid: nil,
             allow_ungated: false,
             candidate_env: [],
             out: "out",
             time: nil,
             allow_short: false,
             allow_dirty: false
           }
  end

  test "explicit values, repeated candidate env and integer thresholds" do
    config =
      Compare.parse!(
        ~w(--base a --candidate b --out o --scenarios basic,mixed --profiles latency --runs 3 --threshold 7 --min-valid 3 --candidate-env BENCH_LATENCY_MS=3 --candidate-env X=a=b --time 4 --allow-short --allow-dirty --allow-ungated)
      )

    assert %{scenarios: ["basic", "mixed"], profiles: [:latency], runs: 3, threshold_pct: 7.0, min_valid: 3} = config
    assert config.candidate_env == [{"BENCH_LATENCY_MS", "3"}, {"X", "a=b"}]
    assert %{time: 4, allow_short: true, allow_dirty: true, allow_ungated: true} = config
  end

  test "scenarios and profiles are deduplicated" do
    config =
      Compare.parse!(~w(--base a --candidate b --out o --scenarios basic,mixed,basic --profiles direct,latency,direct))

    assert config.scenarios == ["basic", "mixed"]
    assert config.profiles == [:direct, :latency]
  end

  test "required options and bad values are refused" do
    assert_raise Mix.Error, ~r/--base/, fn -> Compare.parse!(~w(--candidate b --out o)) end
    assert_raise Mix.Error, ~r/--candidate/, fn -> Compare.parse!(~w(--base a --out o)) end
    assert_raise Mix.Error, ~r/--out/, fn -> Compare.parse!(~w(--base a --candidate b)) end

    assert_raise Mix.Error, ~r/unknown scenario nope/, fn ->
      Compare.parse!(~w(--base a --candidate b --out o --scenarios nope))
    end

    assert_raise Mix.Error, ~r/KEY=VAL/, fn ->
      Compare.parse!(~w(--base a --candidate b --out o --candidate-env oops))
    end

    assert_raise Mix.Error, ~r/--runs/, fn -> Compare.parse!(~w(--base a --candidate b --out o --runs 0)) end

    assert_raise Mix.Error, ~r/--min-valid must be at least 2/, fn ->
      Compare.parse!(~w(--base a --candidate b --out o --min-valid 1))
    end

    assert_raise Mix.Error, ~r/--min-valid/, fn -> Compare.parse!(~w(--base a --candidate b --out o --min-valid 0)) end
    assert Compare.parse!(~w(--base a --candidate b --out o --min-valid 2)).min_valid == 2
  end
end
