defmodule KafkaExBench.Compare.ResultsTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Compare.Results
  alias KafkaExBench.Report

  @tag :tmp_dir
  test "reads a written report back with atom keys on run and job records", %{tmp_dir: dir} do
    base = Path.join(dir, "r")
    Report.write!(base, report(jobs: [job("a", p50_us: 10.0, extras: %{gap_other_leaders_us: [1, 2]})]))

    assert {:ok, report} = Results.read_run(base)
    assert report.config["time"] == 10
    assert report.scenario_params == %{"x" => 1}
    assert [%{run: 1, cluster_topic_count: 4, jobs: [%{job: "a", valid: true, p50_us: 10.0} = job]}] = report.runs
    assert job.gap_other_leaders_us == [1, 2]
    assert report.env["kafka_ex_sha"] == "abc"
  end

  test "a missing or malformed file is a failure with a reason" do
    assert {:failed, reason} = Results.read_run("/nonexistent/r")
    assert reason =~ "no report"
  end

  @tag :tmp_dir
  test "malformed json is a failure", %{tmp_dir: dir} do
    File.write!(Path.join(dir, "bad.json"), "{nope")
    assert {:failed, reason} = Results.read_run(Path.join(dir, "bad"))
    assert reason =~ "unreadable"
  end

  test "job_runs groups records per job in run order and synthesizes invalid ones for failures" do
    entries = [
      {:ok, atomized(jobs: [job("a", p50_us: 1.0), job("b", p50_us: 2.0)])},
      {:failed, "bench.run exited with 1"},
      {:ok, atomized(jobs: [job("a", p50_us: 3.0)])}
    ]

    runs = Results.job_runs(entries, ["a", "b"])
    assert Enum.map(runs["a"], &{&1.valid, Map.get(&1, :p50_us)}) == [{true, 1.0}, {false, nil}, {true, 3.0}]
    assert [%{valid: true}, %{valid: false, error: "bench.run exited with 1"}] = runs["b"]
  end

  test "pairing passes when the run shape matches and warns when topic counts differ" do
    base = [{:ok, atomized(topic_count: 4)}]
    cand = [{:ok, atomized(topic_count: 5)}]
    assert {:ok, [warning], counts} = Results.check_pairing(base, cand)
    assert warning =~ "cluster_topic_count"
    assert counts == %{base: [4], candidate: [5]}
    assert {:ok, [], _} = Results.check_pairing(base, base)
  end

  for {field, change} <- [
        {"time", %{config: %{"time" => 3}}},
        {"warmup", %{config: %{"warmup" => 0}}},
        {"short", %{config: %{"short" => true}}},
        {"scenario_params", %{scenario_params: %{"x" => 2}}}
      ] do
    test "pairing is refused when #{field} differs" do
      change = unquote(Macro.escape(change))
      base = [{:ok, atomized([])}]
      cand = [{:ok, atomized(change |> Map.to_list())}]
      assert {:error, [message]} = Results.check_pairing(base, cand)
      assert message =~ unquote(field)
    end
  end

  test "pairing is refused when runs inside one side disagree" do
    base = [{:ok, atomized([])}, {:ok, atomized(config: %{"time" => 5})}]
    assert {:error, [message]} = Results.check_pairing(base, [{:ok, atomized([])}])
    assert message =~ "time"
  end

  test "failed entries do not take part in pairing" do
    assert {:ok, [], %{base: [], candidate: [4]}} =
             Results.check_pairing([{:failed, "x"}], [{:ok, atomized([])}])
  end

  test "abort reason is set for leaked topics and for an unhealthy cluster only" do
    assert Results.abort_reason({:ok, atomized(leaked: 2)}) =~ "2 leaked"

    assert Results.abort_reason({:ok, atomized(jobs: [job("a", valid: false, error: "cluster unhealthy: 2 brokers")])}) =~
             "unhealthy"

    assert Results.abort_reason({:ok, atomized([])}) == nil
    assert Results.abort_reason({:failed, "bench.run exited with 1"}) == nil
    assert Results.abort_reason({:ok, atomized(jobs: [job("a", valid: false, error: "boom")])}) == nil
  end

  defp job(name, opts \\ []) do
    base = %{
      job: name,
      parallel: 1,
      valid: Keyword.get(opts, :valid, true),
      error: Keyword.get(opts, :error),
      samples: 10,
      min_samples: 5,
      metrics: [p50_us: :lower_better],
      throughput_ops: 100.0,
      p50_us: Keyword.get(opts, :p50_us, 5.0),
      p99_us: 9.0,
      max_mailbox: 1,
      reductions: 1,
      client_reductions_per_op: 1.0,
      vm_reductions_per_op: 1.0,
      vm_cpu_us_per_op: 1.0,
      background_ops: nil,
      started_at: "t",
      duration_ms: 1
    }

    Map.merge(base, Keyword.get(opts, :extras, %{}))
  end

  defp report(opts) do
    %{
      env: %{kafka_ex_sha: "abc", otp: "27"},
      config: %{scenario: "basic", profile: :latency, runs: 1, warmup: 2, time: 10, short: false},
      scenario_params: %{x: 1},
      scenario: "basic",
      started_at: "t",
      runs: [
        %{
          run: 1,
          started_at: "t",
          duration_ms: 1,
          layout: nil,
          jobs: Keyword.get(opts, :jobs, [job("a")]),
          cluster_topic_count: Keyword.get(opts, :topic_count, 4),
          leaked_bench_topics: Keyword.get(opts, :leaked, 0)
        }
      ]
    }
  end

  defp atomized(opts) do
    {overrides, opts} = Keyword.split(opts, [:config, :scenario_params])
    original = report(opts)

    %{
      env: %{"kafka_ex_sha" => "abc"},
      config: Map.merge(stringify(original.config), Keyword.get(overrides, :config, %{})),
      scenario_params: Keyword.get(overrides, :scenario_params, %{"x" => 1}),
      runs: Enum.map(original.runs, &Map.drop(&1, [:started_at, :duration_ms, :layout]))
    }
  end

  defp stringify(map),
    do: Map.new(map, fn {k, v} -> {to_string(k), if(is_atom(v) and not is_boolean(v), do: to_string(v), else: v)} end)
end
