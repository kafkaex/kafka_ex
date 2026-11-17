defmodule KafkaExBench.Report do
  alias KafkaExBench.Stats

  @columns [
    "run",
    "valid",
    "min samples",
    "throughput ops/s",
    "p50 µs",
    "p99 µs",
    "max mailbox",
    "reductions",
    "client reductions/op",
    "vm reductions/op",
    "vm cpu µs/op",
    "error"
  ]

  def write!(base_path, %{runs: runs} = report) do
    File.mkdir_p!(Path.dirname(base_path))
    File.write!(base_path <> ".json", Jason.encode!(encodable(report), pretty: true))
    File.write!(base_path <> ".md", markdown(report, runs))
  end

  defp encodable(%{runs: runs} = report) do
    %{
      report
      | runs: Enum.map(runs, fn run -> Map.update!(run, :jobs, &Enum.map(&1, fn job -> encodable_job(job) end)) end)
    }
  end

  defp encodable_job(%{metrics: metrics} = job), do: %{job | metrics: Map.new(metrics)}
  defp encodable_job(job), do: job

  defp markdown(report, runs) do
    jobs =
      runs
      |> Enum.flat_map(fn %{run: run, jobs: jobs} -> Enum.map(jobs, &{&1.job, run, &1}) end)
      |> Enum.group_by(&elem(&1, 0), &{elem(&1, 1), elem(&1, 2)})

    job_names = runs |> Enum.flat_map(& &1.jobs) |> Enum.map(& &1.job) |> Enum.uniq()

    [
      "# #{report.scenario}\n\n",
      "started: #{report.started_at}\n\n",
      env_block(report.env),
      config_block(report.config, report.scenario_params),
      Enum.map(job_names, &job_section(&1, Map.fetch!(jobs, &1)))
    ]
    |> IO.iodata_to_binary()
  end

  @required_shas [:kafka_ex_sha, :harness_sha]

  defp env_block(env) do
    lines =
      Enum.map(env, fn
        {key, nil} when key in @required_shas -> "- #{key}: MISSING\n"
        {key, value} -> "- #{key}: #{format_env(value)}\n"
      end)

    ["## Environment\n\n", lines, "\n"]
  end

  defp config_block(config, params) do
    lines = for {key, value} <- config_pairs(config, params), do: "- #{key}: #{format_env(value)}\n"
    ["## Configuration\n\n", lines, "\n"]
  end

  defp config_pairs(config, params) do
    case Map.fetch(params, :security) do
      {:ok, scenario_security} ->
        cli = Map.get(config, :security)
        differing = if cli != nil and cli != scenario_security, do: [cli_security: cli], else: []
        Enum.reject(config, fn {key, _} -> key == :security end) ++ differing ++ Enum.to_list(params)

      :error ->
        Enum.concat(config, params)
    end
  end

  defp format_env(list) when is_list(list), do: inspect(list)
  defp format_env(%{} = map), do: map |> Enum.map_join(", ", fn {k, v} -> "#{k} #{v}" end)
  defp format_env(nil), do: "n/a"
  defp format_env(value), do: to_string(value)

  defp job_section(name, rows) do
    valid = for {_run, %{valid: true} = rec} <- rows, do: rec
    invalid = length(rows) - length(valid)
    throughput = numbers(valid, :throughput_ops)
    p99 = numbers(valid, :p99_us)
    vm_cpu = numbers(valid, :vm_cpu_us_per_op)

    [
      "## #{name}\n\n",
      table(rows),
      "\n",
      "- median throughput: #{number(Stats.median(throughput), " ops/s")}\n",
      "- throughput CV: #{percent(Stats.cv(throughput))}\n",
      "- median p99: #{number(Stats.median(p99), " µs")}\n",
      "- p99 CV: #{percent(Stats.cv(p99))}\n",
      "- median vm cpu: #{number(Stats.median(vm_cpu), " µs/op")}\n",
      background_line(valid),
      extras_lines(valid),
      "- invalid runs: #{invalid}\n\n"
    ]
  end

  defp extras_lines(valid) do
    [
      median_line(valid, :gap_restarted_leader_us, "median gap restarted leader", " µs"),
      median_line(valid, :gap_other_leaders_us, "median gap other leaders", " µs"),
      median_line(valid, :rejoin_ms, "median rejoin", " ms"),
      median_line(valid, :restart_cmd_started_ms, "median restart command started", " ms"),
      median_line(valid, :restart_cmd_returned_ms, "median restart command returned", " ms"),
      median_line(valid, :gap_restarted_leader_start_ms, "median gap restarted leader start", " ms"),
      median_line(valid, :gap_restarted_leader_end_ms, "median gap restarted leader end", " ms"),
      median_line(valid, :gap_other_leaders_start_ms, "median gap other leaders start", " ms"),
      median_line(valid, :gap_other_leaders_end_ms, "median gap other leaders end", " ms"),
      total_line(valid, :errors_during_repetitions, "errors during repetitions"),
      total_line(valid, :fetch_retries, "fetch retries"),
      layouts_line(valid)
    ]
  end

  defp median_line(valid, key, label, unit) do
    case flat_values(valid, key) do
      [] -> []
      values -> "- #{label}: #{whole(Stats.median(values))}#{unit}\n"
    end
  end

  defp total_line(valid, key, label) do
    case numbers(valid, key) do
      [] -> []
      values -> "- #{label}: #{Enum.sum(values)}\n"
    end
  end

  defp layouts_line(valid) do
    case flat_values(valid, :layouts) do
      [] -> []
      layouts -> "- layouts: #{length(layouts)} recorded, #{layouts |> Enum.uniq() |> length()} distinct\n"
    end
  end

  defp flat_values(records, key), do: for(%{^key => values} <- records, is_list(values), value <- values, do: value)

  defp background_line(valid) do
    case numbers(valid, :background_ops) do
      [] -> []
      ops -> "- median background ops: #{whole(Stats.median(ops))}\n"
    end
  end

  defp table(rows) do
    header = "| " <> Enum.join(@columns, " | ") <> " |\n"
    divider = "|" <> String.duplicate(" --- |", length(@columns)) <> "\n"

    body =
      Enum.map(rows, fn {run, rec} ->
        cells = [
          run,
          rec.valid,
          whole(rec.min_samples),
          fixed(rec.throughput_ops),
          whole(rec.p50_us),
          whole(rec.p99_us),
          whole(rec.max_mailbox),
          whole(rec.reductions),
          fixed(rec.client_reductions_per_op),
          fixed(rec.vm_reductions_per_op),
          fixed(rec.vm_cpu_us_per_op),
          escape_cell(rec.error)
        ]

        "| " <> Enum.map_join(cells, " | ", &to_string/1) <> " |\n"
      end)

    [header, divider, body]
  end

  defp escape_cell(nil), do: ""
  defp escape_cell(error), do: String.replace(error, "|", "\\|")

  defp numbers(records, key), do: for(%{^key => value} <- records, is_number(value), do: value)

  defp fixed(nil), do: "n/a"
  defp fixed(value), do: :erlang.float_to_binary(value / 1, decimals: 1)

  defp whole(nil), do: "n/a"
  defp whole(value), do: round(value)

  defp number(nil, _unit), do: "n/a"
  defp number(value, unit), do: fixed(value) <> unit

  defp percent(nil), do: "n/a"
  defp percent(ratio), do: :erlang.float_to_binary(ratio * 100, decimals: 1) <> "%"
end
