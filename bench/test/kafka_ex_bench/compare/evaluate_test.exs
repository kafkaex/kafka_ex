defmodule KafkaExBench.Compare.EvaluateTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Compare.Evaluate

  @gates %{"a" => [{:p50_us, :lower_better, {:relative}}, {:throughput_ops, :higher_better, {:relative}}]}

  defp spec(profile, gates \\ @gates, jobs \\ ["a"]),
    do: %{profile: profile, scenario: "basic", gates: gates, job_names: jobs}

  defp entry(job_maps, overrides \\ %{}) do
    report = %{
      config: %{"time" => 10, "warmup" => 2, "short" => false},
      scenario_params: %{},
      runs: [%{run: 1, cluster_topic_count: 1, leaked_bench_topics: 0, jobs: job_maps}]
    }

    {:ok, Map.merge(report, overrides)}
  end

  defp job(values), do: Map.merge(%{job: "a", valid: true, error: nil, p99_us: 9.0}, Map.new(values))

  defp entries(p50s, throughputs \\ nil) do
    throughputs = throughputs || Enum.map(p50s, fn _ -> 1000.0 end)
    for {p50, thr} <- Enum.zip(p50s, throughputs), do: entry([job(p50_us: p50, throughput_ops: thr)])
  end

  test "latency profile applies every gate of every job and takes the worst" do
    cell =
      Evaluate.cell(
        spec(:latency),
        entries([100.0, 100.0, 100.0, 100.0, 100.0]),
        entries([120.0, 120.0, 120.0, 120.0, 120.0]),
        []
      )

    assert cell.gated
    assert cell.verdict == :fail
    assert [%{job: "a", verdict: :fail, gates: [p50, thr]}] = cell.jobs

    assert %{metric: :p50_us, verdict: :fail, base_median: 100.0, cand_median: 120.0, delta_pct: 20.0, rule: "relative"} =
             p50

    assert %{metric: :throughput_ops, verdict: :pass} = thr
  end

  test "a per-gate threshold overrides the cell option and shows in the rule label" do
    gates = %{"a" => [{:p50_us, :lower_better, {:relative, 10.0}}]}
    base = entries([100.0, 100.0, 100.0, 100.0, 100.0])
    cand = entries([108.0, 108.0, 108.0, 108.0, 108.0])
    cell = Evaluate.cell(spec(:latency, gates), base, cand, threshold_pct: 5.0)
    assert cell.verdict == :pass
    assert [%{gates: [%{rule: "relative 10.0%", limit_pct: 10.0}]}] = cell.jobs
  end

  test "noise above ten percent makes a gated relative metric inconclusive" do
    noisy = entries([50.0, 150.0, 100.0, 60.0, 140.0])
    cell = Evaluate.cell(spec(:latency), noisy, entries([100.0, 100.0, 100.0, 100.0, 100.0]), [])
    assert cell.verdict == :inconclusive
  end

  test "threshold and min_valid options reach the rule" do
    base = entries([100.0, 100.0, 100.0])
    cand = entries([108.0, 108.0, 108.0])
    assert Evaluate.cell(spec(:latency), base, cand, []).verdict == :inconclusive
    assert Evaluate.cell(spec(:latency), base, cand, min_valid: 3).verdict == :fail
    assert Evaluate.cell(spec(:latency), base, cand, min_valid: 3, threshold_pct: 10.0).verdict == :pass
  end

  test "a failed candidate run is an invalid run for every job of that side" do
    base = entries([100.0, 100.0, 100.0, 100.0, 100.0])
    cand = entries([100.0, 100.0, 100.0, 100.0]) ++ [{:failed, "bench.run exited with 1"}]
    cell = Evaluate.cell(spec(:latency), base, cand, [])
    assert cell.verdict == :fail
    assert [%{gates: [%{reason: reason} | _]}] = cell.jobs
    assert reason =~ "bench.run exited with 1"
  end

  test "list-valued one-shot metrics use the per-run median and absolute rules apply" do
    gates = %{"restart" => [{:gap_other_leaders_us, :lower_better, {:absolute_us, 250_000}}]}

    run = fn gaps -> entry([%{job: "restart", valid: true, error: nil, gap_other_leaders_us: gaps}]) end
    base = for _ <- 1..5, do: run.([10_000, 20_000, 30_000])
    cand = for _ <- 1..5, do: run.([400_000, 410_000, 420_000])
    cell = Evaluate.cell(spec(:latency, gates, ["restart"]), base, cand, [])
    assert cell.verdict == :fail
    assert [%{gates: [%{rule: "absolute 250000 us", base_median: 20_000, cand_median: 410_000}]}] = cell.jobs
  end

  test "nil metrics are inconclusive on the baseline and failing on the candidate" do
    nil_runs = for _ <- 1..5, do: entry([job(p50_us: nil, throughput_ops: nil)])
    good = entries([100.0, 100.0, 100.0, 100.0, 100.0])
    assert Evaluate.cell(spec(:latency), nil_runs, good, []).verdict == :inconclusive
    assert Evaluate.cell(spec(:latency), good, nil_runs, []).verdict == :fail
  end

  describe "usable-only restart gates" do
    @restart_gates %{
      "restart" => [
        {:gap_other_leaders_us, :lower_better, {:usable_only}},
        {:gap_restarted_leader_us, :lower_better, {:usable_only}}
      ]
    }

    defp restart_entry(valid, other, restarted),
      do:
        entry([
          %{
            job: "restart",
            valid: valid,
            error: if(valid, do: nil, else: "partitions [0] had no success within 120000 ms"),
            gap_other_leaders_us: other,
            gap_restarted_leader_us: restarted
          }
        ])

    defp restart_cell(base, cand, opts \\ [min_valid: 3]),
      do: Evaluate.cell(spec(:latency, @restart_gates, ["restart"]), base, cand, opts)

    test "a candidate whose restart runs are all invalid fails the cell and the overall verdict" do
      base = for _ <- 1..6, do: restart_entry(true, [1_000_000], [1_000_000])
      cand = for _ <- 1..6, do: restart_entry(false, [], [])
      cell = restart_cell(base, cand)
      assert cell.gated
      assert cell.verdict == :fail
      assert Evaluate.overall([cell]) == :fail
    end

    test "more unusable candidate runs than base is inconclusive" do
      base = for _ <- 1..6, do: restart_entry(true, [1_000_000], [1_000_000])
      cand = [restart_entry(false, [], []) | for(_ <- 1..5, do: restart_entry(true, [1_000_000], [1_000_000]))]
      assert restart_cell(base, cand).verdict == :inconclusive
    end

    test "the size of the gap is never judged" do
      gaps = [1_000_000, 2_500_000, 6_200_000]
      base = for gap <- gaps ++ gaps, do: restart_entry(true, [1_000_000], [gap])
      cand = for gap <- gaps ++ gaps, do: restart_entry(true, [6_200_000], [gap * 3])
      cell = restart_cell(base, cand)
      assert cell.verdict == :pass

      assert [%{gates: [%{reason: reason, limit_pct: nil, rule: "usable runs only"}, _]}] = cell.jobs
      assert reason =~ "value not judged"
    end
  end

  test "informational rows of a non-gated cell carry the usable counts" do
    base = entries([100.0, 100.0, 100.0, 100.0, 100.0])
    cand = entries([100.0, 100.0, 100.0]) ++ [entry([job(valid: false, error: "x")]), entry([job(valid: false)])]
    cell = Evaluate.cell(spec(:direct), base, cand, [])

    assert %{base_usable: 5, base_total: 5, cand_usable: 3, cand_total: 5} =
             Enum.find(cell.informational, &(&1.metric == :p50_us))
  end

  test "extras aggregate over valid runs only" do
    run = fn valid, errors -> entry([job(valid: valid, errors_during_repetitions: errors, p50_us: 1.0)]) end
    base = [run.(true, 1), run.(true, 2)]
    cand = [run.(true, 1), run.(false, 100)]
    cell = Evaluate.cell(spec(:direct), base, cand, [])

    assert %{base: 3, candidate: 1} = Enum.find(cell.extras, &(&1.metric == :errors_during_repetitions))
  end

  test "direct profile is informational only: deltas, no verdict, no gates" do
    cell =
      Evaluate.cell(
        spec(:direct),
        entries([100.0, 100.0, 100.0, 100.0, 100.0]),
        entries([200.0, 200.0, 200.0, 200.0, 200.0]),
        []
      )

    refute cell.gated
    assert cell.verdict == nil
    assert cell.jobs == []

    assert %{job: "a", metric: :p50_us, base_median: 100.0, cand_median: 200.0, delta_pct: 100.0} =
             Enum.find(cell.informational, &(&1.metric == :p50_us))

    refute Enum.any?(cell.informational, &Map.has_key?(&1, :verdict))
  end

  test "informational rows use the direction the job's metrics declare and fall back to raw" do
    directed = %{"p50_us" => "lower_better", "throughput_ops" => "higher_better"}
    run = fn p50, thr -> entry([job(p50_us: p50, throughput_ops: thr, metrics: directed)]) end
    base = for _ <- 1..5, do: run.(100.0, 1000.0)
    cand = for _ <- 1..5, do: run.(100.0, 800.0)
    cell = Evaluate.cell(spec(:direct), base, cand, [])
    row = fn metric -> Enum.find(cell.informational, &(&1.metric == metric)) end

    assert %{direction: :higher_better, delta_pct: 20.0} = row.(:throughput_ops)
    assert %{direction: :lower_better, delta_pct: +0.0} = row.(:p50_us)
    assert %{direction: nil} = row.(:p99_us)
  end

  test "extras show base and candidate medians or sums when present" do
    run = fn extras -> entry([job(Keyword.merge([p50_us: 1.0, throughput_ops: 1.0], extras))]) end

    base = [
      run.(background_ops: 100, errors_during_repetitions: 1),
      run.(background_ops: 300, errors_during_repetitions: 2)
    ]

    cand = [run.(background_ops: 200, errors_during_repetitions: 0, fetch_retries: 4), run.(rejoin_ms: [10, 30])]
    cell = Evaluate.cell(spec(:direct), base, cand, [])
    extra = fn metric -> Enum.find(cell.extras, &(&1.metric == metric)) end

    assert %{job: "a", base: 200.0, candidate: 200, agg: :median} = extra.(:background_ops)
    assert %{base: 3, candidate: 0, agg: :sum} = extra.(:errors_during_repetitions)
    assert %{base: nil, candidate: 4, agg: :sum} = extra.(:fetch_retries)
    assert %{base: nil, candidate: 20.0, agg: :median} = extra.(:rejoin_ms)
  end

  test "no extras when no job reports any" do
    assert Evaluate.cell(spec(:direct), entries([1.0]), entries([1.0]), []).extras == []
  end

  test "latency informational rows skip gated and all-nil metrics" do
    cell =
      Evaluate.cell(
        spec(:latency),
        entries([100.0, 100.0, 100.0, 100.0, 100.0]),
        entries([100.0, 100.0, 100.0, 100.0, 100.0]),
        []
      )

    metrics = Enum.map(cell.informational, & &1.metric)
    refute :p50_us in metrics
    refute :throughput_ops in metrics
    assert :p99_us in metrics
  end

  test "a refused pairing makes the cell inconclusive with the refusal recorded" do
    base = [entry([job(p50_us: 1.0)])]
    cand = [entry([job(p50_us: 1.0)], %{config: %{"time" => 3, "warmup" => 2, "short" => false}})]
    cell = Evaluate.cell(spec(:latency), base, cand, [])
    assert cell.verdict == :inconclusive
    assert [message] = cell.refusal
    assert message =~ "time"
    assert cell.jobs == []
  end

  test "topic count warnings and counts are carried on the cell" do
    base = [entry([job(p50_us: 1.0)])]
    {:ok, report} = entry([job(p50_us: 1.0)])
    cand = [{:ok, put_in(report, [:runs, Access.at(0), :cluster_topic_count], 7)}]
    cell = Evaluate.cell(spec(:latency), base, cand, [])
    assert [warning] = cell.warnings
    assert warning =~ "cluster_topic_count"
    assert cell.topic_counts == %{base: [1], candidate: [7]}
  end

  test "overall is the worst gated cell verdict and ungated cells do not count" do
    gated = fn v -> %{gated: true, verdict: v} end
    assert Evaluate.overall([gated.(:pass), %{gated: false, verdict: nil}, gated.(:inconclusive)]) == :inconclusive
    assert Evaluate.overall([gated.(:improvement), gated.(:pass)]) == :pass
    assert Evaluate.overall([%{gated: false, verdict: nil}]) == :not_gated
  end
end
