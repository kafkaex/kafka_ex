defmodule KafkaExBench.Compare.RenderTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Compare.Render

  defp gate(overrides \\ %{}) do
    Map.merge(
      %{
        metric: :p50_us,
        direction: :lower_better,
        rule: "relative",
        base_usable: 5,
        base_total: 5,
        cand_usable: 4,
        cand_total: 5,
        base_median: 100.0,
        cand_median: 120.0,
        delta: 20.0,
        delta_pct: 20.0,
        noise_pct: 1.5,
        base_noise_pct: 1.5,
        cand_noise_pct: 0.5,
        limit_pct: 5.0,
        verdict: :fail,
        reason: "worse by 20.00%, limit 5.00%"
      },
      overrides
    )
  end

  defp compare(cells) do
    %{
      generated_at: "2026-11-16T10:00:00Z",
      config: %{
        base: "aaa",
        candidate: "bbb",
        runs: 5,
        threshold_pct: 5.0,
        min_valid: nil,
        allow_ungated: false,
        profiles: [:latency, :direct],
        scenarios: ["basic"],
        candidate_env: %{"BENCH_LATENCY_MS" => "3"}
      },
      env: %{
        base: %{"kafka_ex_sha" => "aaa", "otp" => "27", "latency_ms" => 2},
        candidate: %{"kafka_ex_sha" => "bbb", "otp" => "27", "latency_ms" => 3}
      },
      cells: cells,
      failures: [%{side: :candidate, profile: :latency, scenario: "basic", index: 2, reason: "bench.run exited with 1"}],
      verdict: :fail
    }
  end

  defp gated_cell do
    %{
      profile: :latency,
      scenario: "basic",
      gated: true,
      verdict: :fail,
      refusal: nil,
      warnings: ["cluster_topic_count differs between runs: base [4], candidate [5]"],
      topic_counts: %{base: [4], candidate: [5]},
      jobs: [%{job: "a|b", verdict: :fail, gates: [gate()]}],
      informational: [
        %{
          job: "a|b",
          metric: :p99_us,
          direction: nil,
          base_usable: 5,
          base_total: 5,
          cand_usable: 0,
          cand_total: 5,
          base_median: 9.0,
          cand_median: nil,
          delta_pct: nil,
          noise_pct: 0.0
        },
        %{
          job: "a|b",
          metric: :vm_cpu_us_per_op,
          direction: :lower_better,
          base_usable: 5,
          base_total: 5,
          cand_usable: 3,
          cand_total: 5,
          base_median: 2.0,
          cand_median: 3.0,
          delta_pct: 50.0,
          noise_pct: 1.0
        }
      ],
      extras: [%{job: "a|b", metric: :fetch_retries, base: 1, candidate: 5, agg: :sum}]
    }
  end

  test "markdown shows the verdict, both environments, the gate table, informational rows, warnings and failed runs" do
    md = Render.markdown(compare([gated_cell()]))

    assert md =~ "# Comparison"
    assert md =~ "Overall verdict: FAIL"
    assert md =~ "BENCH_LATENCY_MS=3"
    assert md =~ "| kafka_ex_sha | aaa | bbb |"
    assert md =~ "| latency_ms | 2 | 3 |"

    assert md =~
             "| job | metric | usable base/cand | base median | candidate median | delta % | noise % | noise base/cand % | limit % | verdict | reason |"

    assert md =~
             "| a\\|b | p50_us | 5/5 vs 4/5 | 100.0 | 120.0 | 20.00 | 1.50 | 1.50 / 0.50 | 5.00 | FAIL | worse by 20.00%, limit 5.00% |"

    assert md =~ "| job | metric | usable base/cand | base median | candidate median | raw Δ % | noise % |"
    assert md =~ "| a\\|b | p99_us | 5/5 vs 0/5 | 9.0 | n/a | n/a | 0.00 |"
    assert md =~ "| job | metric | usable base/cand | base median | candidate median | worse Δ % | noise % |"
    assert md =~ "| a\\|b | vm_cpu_us_per_op | 5/5 vs 3/5 | 2.0 | 3.0 | 50.00 | 1.00 |"
    assert md =~ "| job | metric | base | candidate | aggregate |"
    assert md =~ "| a\\|b | fetch_retries | 1 | 5 | sum |"
    assert md =~ "cluster_topic_count differs"
    assert md =~ "candidate latency basic run 2: bench.run exited with 1"
  end

  test "the home directory never appears in compare.md or compare.json" do
    home = System.get_env("HOME")
    assert byte_size(home) > 1

    base = %{
      compare([gated_cell()])
      | failures: [
          %{side: :base, profile: :latency, scenario: "basic", index: 1, reason: "no report at #{home}/x/y.json"}
        ]
    }

    compare = put_in(base.config.base, "#{home}/repo/kafka_ex") |> Map.put(:abort, "failed in #{home}/wt/base")

    for output <- [Render.markdown(compare), Render.json(compare)] do
      refute output =~ home
      assert output =~ "~/"
    end

    assert Render.markdown(compare) =~ "- base: ~/repo/kafka_ex"
    assert Render.markdown(compare) =~ "no report at ~/x/y.json"
  end

  test "aborted comparisons say so with the reason" do
    md = Render.markdown(Map.put(compare([]), :abort, "2 leaked bench- topics found"))
    assert md =~ "ABORTED: 2 leaked bench- topics found"
  end

  test "ungated allowed is labelled" do
    cfg = %{compare([]).config | allow_ungated: true}
    md = Render.markdown(%{compare([]) | verdict: :not_gated, config: cfg})
    assert md =~ "Overall verdict: NOT GATED (allowed)"
    assert Render.markdown(%{compare([]) | verdict: :not_gated}) =~ "Overall verdict: NOT GATED\n"
  end

  test "gate rows print the noise of each side and informational rows the usable counts" do
    md = Render.markdown(compare([gated_cell()]))
    assert md =~ "noise base/cand %"
    assert md =~ "1.50 / 0.50"
    assert md =~ "| a\\|b | vm_cpu_us_per_op | 5/5 vs 3/5 |"
  end

  test "a gate-less job gets a row" do
    cell = %{gated_cell() | jobs: [%{job: "a", verdict: :inconclusive, gates: []}]}
    assert Render.markdown(compare([cell])) =~ "| a | (no gates) |"
  end

  test "an informational-only cell has no verdict column and says so" do
    cell = %{gated_cell() | profile: :direct, gated: false, verdict: nil, jobs: []}
    md = Render.markdown(compare([cell]))
    assert md =~ "direct / basic (informational, not gated)"
    refute md =~ "| verdict |"
  end

  test "a refused cell lists the refusals" do
    cell = %{gated_cell() | verdict: :inconclusive, refusal: ["time differs across runs: [10, 3]"], jobs: []}
    md = Render.markdown(compare([cell]))
    assert md =~ "pairing refused"
    assert md =~ "time differs across runs"
  end

  test "json round-trips with string verdicts and no tuples" do
    decoded = [gated_cell()] |> compare() |> Render.json() |> Jason.decode!()
    assert decoded["verdict"] == "fail"
    assert [cell] = decoded["cells"]
    assert cell["jobs"] |> hd() |> Map.fetch!("gates") |> hd() |> Map.fetch!("verdict") == "fail"
    assert decoded["env"]["candidate"]["latency_ms"] == 3
  end

  test "write! creates compare.md and compare.json under the out dir" do
    dir = Path.join(System.tmp_dir!(), "compare-render-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    Render.write!(dir, compare([gated_cell()]))
    assert File.exists?(Path.join(dir, "compare.md"))
    assert dir |> Path.join("compare.json") |> File.read!() |> Jason.decode!() |> Map.has_key?("cells")
  end
end
