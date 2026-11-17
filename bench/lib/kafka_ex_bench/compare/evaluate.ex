defmodule KafkaExBench.Compare.Evaluate do
  alias KafkaExBench.Compare.Results
  alias KafkaExBench.{Rule, Stats}

  @informational_metrics [
    :throughput_ops,
    :p50_us,
    :p99_us,
    :vm_cpu_us_per_op,
    :vm_reductions_per_op,
    :client_reductions_per_op,
    :max_mailbox
  ]
  @gated_profiles [:latency]
  @extras [background_ops: :median, rejoin_ms: :median, errors_during_repetitions: :sum, fetch_retries: :sum]

  def cell(spec, base_entries, candidate_entries, opts) do
    gated = spec.profile in @gated_profiles
    shell = %{profile: spec.profile, scenario: spec.scenario, gated: gated}

    case Results.check_pairing(base_entries, candidate_entries) do
      {:error, refusals} ->
        Map.merge(shell, %{
          verdict: :inconclusive,
          refusal: refusals,
          warnings: [],
          topic_counts: nil,
          jobs: [],
          informational: [],
          extras: []
        })

      {:ok, warnings, counts} ->
        base = Results.job_runs(base_entries, spec.job_names)
        candidate = Results.job_runs(candidate_entries, spec.job_names)
        jobs = if gated, do: Enum.map(spec.job_names, &gated_job(&1, spec.gates, base, candidate, opts)), else: []

        Map.merge(shell, %{
          verdict: if(gated, do: Rule.overall(Enum.map(jobs, & &1.verdict))),
          refusal: nil,
          warnings: warnings,
          topic_counts: counts,
          jobs: jobs,
          informational: informational(spec, gated, base, candidate),
          extras: extras(spec, base, candidate)
        })
    end
  end

  def overall(cells) do
    case for(%{gated: true, verdict: verdict} <- cells, do: verdict) do
      [] -> :not_gated
      verdicts -> Rule.overall(verdicts)
    end
  end

  defp gated_job(job, gates, base, candidate, opts) do
    rule_opts = Keyword.take(opts, [:threshold_pct, :min_valid])

    results =
      for {metric, direction, rule} = gate <- Map.get(gates, job, []) do
        base[job]
        |> Rule.verdict(candidate[job], gate, rule_opts)
        |> Map.merge(%{metric: metric, direction: direction, rule: rule_label(rule)})
      end

    %{job: job, verdict: Rule.job_verdict(results), gates: results}
  end

  defp rule_label({:relative}), do: "relative"
  defp rule_label({:relative, threshold_pct}), do: "relative #{threshold_pct}%"
  defp rule_label({:absolute_us, limit}), do: "absolute #{limit} us"
  defp rule_label({:usable_only}), do: "usable runs only"

  defp informational(spec, gated, base, candidate) do
    for job <- spec.job_names,
        metric <- Enum.uniq(@informational_metrics ++ gate_metrics(spec.gates, job)),
        not (gated and metric in gate_metrics(spec.gates, job)),
        row = informational_row(spec.gates, job, metric, base[job], candidate[job]),
        row.base_median != nil or row.cand_median != nil do
      Map.merge(row, %{job: job, metric: metric})
    end
  end

  defp informational_row(gates, job, metric, base, candidate),
    do: Rule.informational(base, candidate, metric, direction(gates, job, metric, base ++ candidate))

  defp direction(gates, job, metric, runs) do
    Enum.find_value(Map.get(gates, job, []), fn
      {^metric, direction, _rule} -> direction
      _ -> nil
    end) || declared_direction(runs, metric)
  end

  defp declared_direction(runs, metric) do
    key = Atom.to_string(metric)

    runs
    |> Enum.find_value(fn
      %{metrics: %{^key => direction}} -> direction
      _ -> nil
    end)
    |> case do
      "lower_better" -> :lower_better
      "higher_better" -> :higher_better
      _ -> nil
    end
  end

  defp extras(spec, base, candidate) do
    for job <- spec.job_names,
        {metric, agg} <- @extras,
        values = {aggregate(base[job], metric, agg), aggregate(candidate[job], metric, agg)},
        values != {nil, nil} do
      %{job: job, metric: metric, base: elem(values, 0), candidate: elem(values, 1), agg: agg}
    end
  end

  defp aggregate(runs, metric, agg) do
    values = for %{valid: true} = run <- runs, value = run_value(Map.get(run, metric)), do: value

    case values do
      [] -> nil
      values when agg == :median -> Stats.median(values)
      values -> Enum.sum(values)
    end
  end

  defp run_value(list) when is_list(list), do: Stats.median(list)
  defp run_value(value), do: value

  defp gate_metrics(gates, job),
    do: for({metric, _direction, _rule} <- Map.get(gates, job, []), do: metric)
end
