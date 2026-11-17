defmodule KafkaExBench.CompareTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.{Compare, Report}

  @moduletag :tmp_dir

  defp config(dir, overrides) do
    Map.merge(
      %{
        base: "aaa",
        candidate: "bbb",
        scenarios: ["basic", "restart_recovery"],
        profiles: [:latency, :direct],
        runs: 5,
        threshold_pct: 5.0,
        min_valid: nil,
        allow_ungated: false,
        candidate_env: [{"BENCH_LATENCY_MS", "3"}],
        out: dir,
        time: nil,
        allow_short: false
      },
      overrides
    )
  end

  defp sides do
    %{
      base: %{label: "base", path: "/wt/base", sha: "aaa", env: []},
      candidate: %{label: "candidate", path: "/wt/cand", sha: "bbb", env: [{"BENCH_LATENCY_MS", "3"}]}
    }
  end

  defp execute(cfg, runner, opts \\ []),
    do: Compare.execute(cfg, sides(), runner, Keyword.put_new(opts, :compile, fn _ -> {"", 0} end))

  defp arg(args, flag), do: args |> Enum.drop_while(&(&1 != flag)) |> Enum.at(1)

  defp job_record(name, valid, values) do
    Map.merge(
      %{
        job: name,
        parallel: 1,
        valid: valid,
        error: nil,
        samples: 1,
        min_samples: 1,
        metrics: [p50_us: :lower_better],
        throughput_ops: 100.0,
        p50_us: 100.0,
        p99_us: 200.0,
        max_mailbox: 1,
        reductions: 1,
        client_reductions_per_op: 1.0,
        vm_reductions_per_op: 1.0,
        vm_cpu_us_per_op: 1.0,
        background_ops: nil,
        started_at: "t",
        duration_ms: 1
      },
      values
    )
  end

  defp write_report(args, side, opts) do
    scenario = arg(args, "--scenario")
    jobs = KafkaExBench.Scenarios.Basic.job_specs() |> Enum.map(fn {name, _} -> name end)
    p50 = if side == :candidate, do: Keyword.get(opts, :candidate_p50, 100.0), else: 100.0

    records =
      if scenario == "restart_recovery",
        do: [job_record("restart_kafka2", true, %{gap_other_leaders_us: [1000, 2000], gap_restarted_leader_us: [1000]})],
        else: Enum.map(jobs, &job_record(&1, true, %{p50_us: p50}))

    Report.write!(arg(args, "--out"), %{
      env: %{kafka_ex_sha: if(side == :base, do: "aaa", else: "bbb"), latency_ms: 2},
      config: %{scenario: scenario, profile: arg(args, "--profile"), warmup: 2, time: 10, short: false, runs: 1},
      scenario_params: %{},
      scenario: scenario,
      started_at: "t",
      runs: [
        %{
          run: 1,
          started_at: "t",
          duration_ms: 1,
          layout: nil,
          jobs: records,
          cluster_topic_count: 3,
          leaked_bench_topics: Keyword.get(opts, :leaked, 0)
        }
      ]
    })
  end

  defp fresh_runner(test_pid, report_opts \\ fn _ -> [] end, exit_for \\ fn _ -> 0 end) do
    fn %{args: args, env: env} ->
      side = if {"KAFKA_EX_PATH", "/wt/cand"} in env, do: :candidate, else: :base
      send(test_pid, {:ran, side, arg(args, "--profile"), arg(args, "--scenario"), arg(args, "--runs"), env})
      out = arg(args, "--out")
      status = exit_for.({side, out})
      if status == 0, do: write_report(args, side, report_opts.({side, out}))
      status
    end
  end

  test "executes the plan in order, writes both reports and the comparison", %{tmp_dir: dir} do
    cfg = config(dir, %{runs: 5})
    result = execute(cfg, fresh_runner(self()))

    messages =
      for _ <- 1..40,
          do:
            (receive do
               {:ran, side, profile, scenario, runs, _env} -> {side, profile, scenario, runs}
             end)

    assert Enum.take(messages, 4) ==
             [{:base, "latency", "basic", "1"}, {:candidate, "latency", "basic", "1"}] ++
               [{:candidate, "latency", "basic", "1"}, {:base, "latency", "basic", "1"}]

    assert Enum.take(messages, -2) == [
             {:base, "direct", "restart_recovery", "1"},
             {:candidate, "direct", "restart_recovery", "1"}
           ]

    assert result.verdict == :pass
    assert File.exists?(Path.join(dir, "compare.md"))
    assert File.exists?(Path.join(dir, "compare.json"))
    assert File.exists?(Path.join(dir, "runs/candidate-latency-basic-3.json"))
    assert length(result.cells) == 4
    assert Enum.count(result.cells, & &1.gated) == 2
    assert result.env.base["kafka_ex_sha"] == "aaa"
    assert result.env.candidate["kafka_ex_sha"] == "bbb"
    assert result.config.candidate_env == %{"BENCH_LATENCY_MS" => "3"}
  end

  test "candidate env reaches candidate runs only", %{tmp_dir: dir} do
    execute(config(dir, %{runs: 1, scenarios: ["basic"], profiles: [:latency]}), fresh_runner(self()))
    assert_received {:ran, :base, _, _, _, base_env}
    assert_received {:ran, :candidate, _, _, _, cand_env}
    refute {"BENCH_LATENCY_MS", "3"} in base_env
    assert {"BENCH_LATENCY_MS", "3"} in cand_env
  end

  test "a slower candidate fails the latency gate", %{tmp_dir: dir} do
    cfg = config(dir, %{scenarios: ["basic"], profiles: [:latency]})

    result = execute(cfg, fresh_runner(self(), fn _ -> [candidate_p50: 150.0] end))

    assert result.verdict == :fail
    assert Compare.exit_status(result) == 1
  end

  test "a run that exits non-zero is recorded as failed and invalidates only that side", %{tmp_dir: dir} do
    cfg = config(dir, %{scenarios: ["basic"], profiles: [:latency]})

    exit_for = fn
      {:candidate, out} -> if String.ends_with?(out, "-2"), do: 1, else: 0
      _ -> 0
    end

    result = execute(cfg, fresh_runner(self(), fn _ -> [] end, exit_for))
    assert [%{side: :candidate, index: 2, reason: reason}] = result.failures
    assert reason =~ "exited with 1"
    assert result.verdict == :fail
  end

  test "a leaked bench topic aborts the comparison with a clear message", %{tmp_dir: dir} do
    cfg = config(dir, %{scenarios: ["basic"], profiles: [:latency]})

    runner =
      fresh_runner(self(), fn {side, out} ->
        if side == :candidate and String.ends_with?(out, "-2"), do: [leaked: 2], else: []
      end)

    assert_raise RuntimeError, ~r/aborting.*leaked bench- topics/s, fn -> execute(cfg, runner) end
    assert File.read!(Path.join(dir, "compare.md")) =~ "ABORTED"
    assert File.read!(Path.join(dir, "compare.md")) =~ "leaked bench- topics"
    assert dir |> Path.join("compare.json") |> File.read!() |> Jason.decode!() |> Map.fetch!("abort") =~ "leaked"
  end

  test "a comparison with no gated cell exits non-zero unless ungated is allowed", %{tmp_dir: dir} do
    cfg = config(dir, %{scenarios: ["basic"], profiles: [:direct], runs: 2})
    result = execute(cfg, fresh_runner(self()))
    assert result.verdict == :not_gated
    assert Compare.exit_status(result) == 1
    assert File.read!(Path.join(dir, "compare.md")) =~ "NOT GATED"
    refute File.read!(Path.join(dir, "compare.md")) =~ "NOT GATED (allowed)"

    allowed = execute(%{cfg | allow_ungated: true}, fresh_runner(self()))
    assert Compare.exit_status(allowed) == 0
    assert File.read!(Path.join(dir, "compare.md")) =~ "NOT GATED (allowed)"
  end

  test "min_valid comes from each scenario unless given explicitly", %{tmp_dir: dir} do
    cfg = config(dir, %{scenarios: ["basic", "restart_recovery"], profiles: [:latency], runs: 3})
    result = execute(cfg, fresh_runner(self()))
    by_scenario = Map.new(result.cells, &{&1.scenario, &1})
    assert by_scenario["restart_recovery"].verdict == :pass
    assert by_scenario["basic"].verdict == :inconclusive
    assert [%{gates: [%{reason: reason} | _]} | _] = by_scenario["basic"].jobs
    assert reason =~ "need 5"

    explicit = execute(%{cfg | min_valid: 3}, fresh_runner(self()))
    assert Enum.find(explicit.cells, &(&1.scenario == "basic")).verdict == :pass
  end

  test "a latency restart cell is gated on usable runs only", %{tmp_dir: dir} do
    cfg = config(dir, %{profiles: [:latency], runs: 5, min_valid: 3})
    result = execute(cfg, fresh_runner(self()))
    restart = Enum.find(result.cells, &(&1.scenario == "restart_recovery"))
    assert restart.gated
    assert restart.verdict == :pass
    assert [%{gates: [%{rule: "usable runs only", limit_pct: nil}, _]}] = restart.jobs
    assert result.verdict == :pass
    assert File.read!(Path.join(dir, "compare.md")) =~ "## latency / restart_recovery: PASS"
  end

  test "a restart-only comparison on the latency profile is gated", %{tmp_dir: dir} do
    cfg = config(dir, %{scenarios: ["restart_recovery"], profiles: [:latency], runs: 3})
    result = execute(cfg, fresh_runner(self()))
    assert result.verdict == :pass
    assert Compare.exit_status(result) == 0
  end

  test "a direct-only comparison is not gated and exits non-zero unless ungated is allowed", %{tmp_dir: dir} do
    cfg = config(dir, %{scenarios: ["restart_recovery"], profiles: [:direct], runs: 2})
    result = execute(cfg, fresh_runner(self()))
    assert result.verdict == :not_gated
    assert Compare.exit_status(result) == 1
    assert Compare.exit_status(%{result | config: %{result.config | allow_ungated: true}}) == 0
  end

  defmodule Ungated do
    def job_specs, do: [{"a", 1}]
    def gates, do: %{}
  end

  test "a job without gates makes its cell inconclusive and is listed in compare.md", %{tmp_dir: dir} do
    cfg = config(dir, %{scenarios: ["ungated"], profiles: [:latency], runs: 1, min_valid: 1})
    runner = fn %{args: args} -> write_report(args, :base, []) && 0 end
    result = execute(cfg, runner, scenarios: %{"ungated" => Ungated})
    assert [%{verdict: :inconclusive, jobs: [%{job: "a", verdict: :inconclusive, gates: []}]}] = result.cells
    assert File.read!(Path.join(dir, "compare.md")) =~ "| a | (no gates) |"
  end

  test "stale run reports from an earlier compare are removed", %{tmp_dir: dir} do
    File.mkdir_p!(Path.join(dir, "runs"))
    File.write!(Path.join(dir, "runs/base-latency-basic-1.json"), "stale")
    cfg = config(dir, %{scenarios: ["basic"], profiles: [:latency], runs: 1})
    execute(cfg, fn _ -> 1 end)
    refute File.exists?(Path.join(dir, "runs/base-latency-basic-1.json"))
  end

  describe "preflight build" do
    defp compile_stub(test_pid, status_for \\ fn _ -> {"ok", 0} end) do
      fn %{cmd: cmd, args: args, env: env} ->
        side = if {"KAFKA_EX_PATH", "/wt/cand"} in env, do: :candidate, else: :base
        send(test_pid, {:compiled, side, cmd, args, env})
        status_for.(side)
      end
    end

    test "each side is compiled with its own env before any measured run", %{tmp_dir: dir} do
      cfg = config(dir, %{runs: 1, scenarios: ["basic"], profiles: [:latency]})
      execute(cfg, fresh_runner(self()), compile: compile_stub(self()))

      assert {:messages,
              [
                {:compiled, :base, "mix", ["compile"], base_env},
                {:compiled, :candidate, _, _, cand_env},
                {:ran, :base, _, _, _, _} | _
              ]} = Process.info(self(), :messages)

      assert {"KAFKA_EX_PATH", "/wt/base"} in base_env
      assert {"BENCH_BUILD_PATH", "_build_base"} in base_env
      assert {"MIX_ENV", "dev"} in base_env
      assert {"BENCH_BUILD_PATH", "_build_candidate"} in cand_env
      assert {"BENCH_LATENCY_MS", "3"} in cand_env
    end

    test "a failing build aborts before measuring and keeps the last 40 output lines", %{tmp_dir: dir} do
      cfg = config(dir, %{runs: 1, scenarios: ["basic"], profiles: [:latency]})
      output = Enum.map_join(1..100, "\n", &"line #{&1}")
      stub = compile_stub(self(), fn side -> if side == :candidate, do: {output, 1}, else: {"ok", 0} end)

      assert_raise RuntimeError, ~r/aborting preflight build failed for candidate/, fn ->
        execute(cfg, fresh_runner(self()), compile: stub)
      end

      refute_received {:ran, _, _, _, _, _}
      md = File.read!(Path.join(dir, "compare.md"))
      assert md =~ "ABORTED: preflight build failed for candidate"
      assert md =~ "line 100"
      assert md =~ "line 61"
      refute md =~ "line 60\n"
      assert dir |> Path.join("compare.json") |> File.read!() |> Jason.decode!() |> Map.fetch!("abort") =~ "line 100"
    end

    test "the abort message raised to the terminal has the home directory masked", %{tmp_dir: dir} do
      cfg = config(dir, %{runs: 1, scenarios: ["basic"], profiles: [:latency]})
      home = System.get_env("HOME")

      stub =
        compile_stub(self(), fn side -> if side == :candidate, do: {"error in #{home}/x.ex", 1}, else: {"ok", 0} end)

      error = assert_raise RuntimeError, fn -> execute(cfg, fresh_runner(self()), compile: stub) end
      refute error.message =~ home
      assert error.message =~ "~/x.ex"
    end
  end

  describe "repeated failures" do
    defp ran_count(side),
      do: Process.info(self(), :messages) |> elem(1) |> Enum.count(&match?({:ran, ^side, _, _, _, _}, &1))

    test "two consecutive failed runs of one side abort the comparison", %{tmp_dir: dir} do
      cfg = config(dir, %{scenarios: ["basic"], profiles: [:latency]})

      exit_for = fn
        {:candidate, out} -> if Regex.match?(~r/-[23]$/, out), do: 1, else: 0
        _ -> 0
      end

      assert_raise RuntimeError, ~r/aborting.*candidate had 2 consecutive failed runs/s, fn ->
        execute(cfg, fresh_runner(self(), fn _ -> [] end, exit_for))
      end

      assert File.read!(Path.join(dir, "compare.md")) =~ "ABORTED"
      assert ran_count(:candidate) == 3
    end

    test "a success between two failures does not abort", %{tmp_dir: dir} do
      cfg = config(dir, %{scenarios: ["basic"], profiles: [:latency], runs: 4})

      exit_for = fn
        {:candidate, out} -> if Regex.match?(~r/-[13]$/, out), do: 1, else: 0
        _ -> 0
      end

      result = execute(cfg, fresh_runner(self(), fn _ -> [] end, exit_for))
      assert length(result.failures) == 2
    end
  end

  test "exit status follows the rule" do
    assert Compare.exit_status(%{verdict: :inconclusive}) == 1
    assert Compare.exit_status(%{verdict: :pass}) == 0
    assert Compare.exit_status(%{verdict: :improvement}) == 0
  end
end
