defmodule KafkaExBench.Compare do
  alias KafkaExBench.Compare.{Evaluate, Plan, Render, Results}
  alias KafkaExBench.{Clock, Rule, Runner}

  @default_min_valid 5

  @tail_lines 40

  def execute(config, sides, runner, opts) do
    scenarios = Keyword.get_lazy(opts, :scenarios, &Mix.Tasks.Bench.Run.scenarios/0)
    compile = Keyword.fetch!(opts, :compile)
    runs_dir = Path.join(config.out, "runs")
    File.rm_rf!(runs_dir)
    File.mkdir_p!(runs_dir)

    {entries, abort} =
      case preflight(sides, compile) do
        :ok -> run_plan(config, sides, runner)
        {:error, reason} -> {[], reason}
      end

    compare = build(config, entries, scenarios, abort)
    Render.write!(config.out, compare)
    if abort, do: raise(Runner.mask_home("aborting #{abort}", System.get_env("HOME")))
    compare
  end

  def exit_status(%{verdict: :not_gated, config: %{allow_ungated: true}}), do: 0
  def exit_status(%{verdict: :not_gated}), do: 1
  def exit_status(%{verdict: verdict}), do: Rule.exit_status(verdict)

  def system_runner(bench_dir) do
    fn %{cmd: cmd, args: args, env: env} ->
      {_output, status} =
        System.cmd(cmd, args, cd: bench_dir, env: env, into: IO.stream(:stdio, :line), stderr_to_stdout: true)

      status
    end
  end

  def system_compile(bench_dir) do
    fn %{cmd: cmd, args: args, env: env} ->
      {output, status} = System.cmd(cmd, args, cd: bench_dir, env: env, stderr_to_stdout: true)
      if status != 0, do: IO.puts(tail(output))
      {output, status}
    end
  end

  def tail(output) do
    output |> String.split("\n") |> Enum.take(-@tail_lines) |> Enum.join("\n") |> String.trim_trailing()
  end

  defp preflight(sides, compile) do
    Enum.find_value([:base, :candidate], :ok, fn name ->
      side = Map.fetch!(sides, name)

      case compile.(Plan.compile_command(side)) do
        {_output, 0} ->
          nil

        {output, status} ->
          {:error, "preflight build failed for #{side.label} (mix compile exited with #{status}):\n" <> tail(output)}
      end
    end)
  end

  defp build(config, entries, scenarios, abort) do
    cells = cells(config, entries, scenarios, abort != nil)

    compare = %{
      generated_at: Clock.now_iso8601(),
      config: report_config(config),
      env: %{base: first_env(entries, :base), candidate: first_env(entries, :candidate)},
      cells: cells,
      failures: failures(entries),
      verdict: if(abort, do: :inconclusive, else: Evaluate.overall(cells))
    }

    if abort, do: Map.put(compare, :abort, abort), else: compare
  end

  defp run_plan(config, sides, runner) do
    config.scenarios
    |> Plan.runs(config.profiles, config.runs)
    |> Enum.reduce_while({[], nil}, fn run, {entries, nil} ->
      entry = execute_run(run, Map.fetch!(sides, run.side), config, runner)
      previous = entries
      entries = entries ++ [{run, entry}]

      case Results.abort_reason(entry) || repeated_failure(previous, run, entry) do
        nil -> {:cont, {entries, nil}}
        reason -> {:halt, {entries, "after #{run.side} #{run.profile} #{run.scenario} run #{run.index}: #{reason}"}}
      end
    end)
  end

  defp repeated_failure(previous, run, {:failed, _reason}) do
    last_of_side = previous |> Enum.reverse() |> Enum.find(fn {earlier, _entry} -> earlier.side == run.side end)

    case last_of_side do
      {_earlier, {:failed, _reason}} -> "#{run.side} had 2 consecutive failed runs"
      _ -> nil
    end
  end

  defp repeated_failure(_previous, _run, _entry), do: nil

  defp execute_run(run, side, config, runner) do
    command = Plan.command(run, side, config)

    case runner.(command) do
      0 -> Results.read_run(Plan.output_base(config.out, run))
      status -> {:failed, "bench.run exited with #{status}"}
    end
  end

  defp cells(config, entries, scenarios, partial) do
    for profile <- Enum.uniq(config.profiles),
        scenario <- Enum.uniq(config.scenarios),
        not partial or Enum.any?(entries, &match?({%{profile: ^profile, scenario: ^scenario}, _}, &1)) do
      scenario_module = Map.fetch!(scenarios, scenario)

      spec = %{
        profile: profile,
        scenario: scenario,
        gates: scenario_module.gates(),
        job_names: Enum.map(scenario_module.job_specs(), &elem(&1, 0))
      }

      cell_entries = fn side ->
        for {%{side: ^side, profile: ^profile, scenario: ^scenario}, entry} <- entries, do: entry
      end

      Evaluate.cell(spec, cell_entries.(:base), cell_entries.(:candidate), evaluate_opts(config, scenario_module))
    end
  end

  defp evaluate_opts(config, scenario_module),
    do: [threshold_pct: config.threshold_pct, min_valid: config.min_valid || scenario_min_valid(scenario_module)]

  defp scenario_min_valid(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :default_opts, 0),
      do: Keyword.get(module.default_opts(), :min_valid, @default_min_valid),
      else: @default_min_valid
  end

  defp first_env(entries, side) do
    Enum.find_value(entries, fn
      {%{side: ^side}, {:ok, report}} -> report.env
      _ -> nil
    end)
  end

  defp failures(entries) do
    for {run, {:failed, reason}} <- entries,
        do: %{side: run.side, profile: run.profile, scenario: run.scenario, index: run.index, reason: reason}
  end

  defp report_config(config) do
    %{
      base: config.base,
      candidate: config.candidate,
      runs: config.runs,
      threshold_pct: config.threshold_pct,
      min_valid: config.min_valid,
      allow_ungated: config.allow_ungated,
      profiles: config.profiles,
      scenarios: config.scenarios,
      candidate_env: Map.new(config.candidate_env)
    }
  end
end
