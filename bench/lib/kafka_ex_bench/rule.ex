defmodule KafkaExBench.Rule do
  alias KafkaExBench.Stats

  @default_threshold_pct 5.0
  @default_min_valid 5
  @max_noise_pct 10.0
  @severity %{fail: 3, inconclusive: 2, pass: 1, improvement: 0}

  def verdict(base_runs, cand_runs, {metric, direction, rule}, opts \\ []) do
    threshold_pct = Keyword.get(opts, :threshold_pct, @default_threshold_pct)
    min_valid = Keyword.get(opts, :min_valid, @default_min_valid)
    base = samples(base_runs, metric)
    cand = samples(cand_runs, metric)

    stats =
      base
      |> stats(cand, sign(direction))
      |> Map.merge(%{
        base_usable: length(base),
        base_total: length(base_runs),
        cand_usable: length(cand),
        cand_total: length(cand_runs)
      })

    base_unusable = stats.base_total - stats.base_usable
    cand_unusable = stats.cand_total - stats.cand_usable

    cond do
      stats.base_usable < min_valid ->
        result(stats, :inconclusive, "baseline invalid, need #{min_valid} usable runs")

      stats.cand_usable < min_valid ->
        result(stats, :fail, "candidate invalid runs: #{invalid_summary(cand_runs, metric)}, need #{min_valid} usable")

      cand_unusable > base_unusable ->
        result(
          stats,
          :inconclusive,
          "candidate had #{cand_unusable} unusable of #{stats.cand_total} runs (base #{base_unusable} of #{stats.base_total})"
        )

      true ->
        decide(rule, stats, threshold_pct, spread(base, cand))
    end
  end

  def informational(base_runs, cand_runs, metric, direction \\ nil) do
    base = samples(base_runs, metric)
    cand = samples(cand_runs, metric)

    base
    |> stats(cand, sign(direction))
    |> Map.drop([:delta, :limit_pct])
    |> Map.merge(%{
      direction: direction,
      base_usable: length(base),
      base_total: length(base_runs),
      cand_usable: length(cand),
      cand_total: length(cand_runs)
    })
  end

  def job_verdict(gate_results), do: worst(Enum.map(gate_results, & &1.verdict))

  def overall(job_verdicts), do: worst(job_verdicts)

  def exit_status(:pass), do: 0
  def exit_status(:improvement), do: 0
  def exit_status(:fail), do: 1
  def exit_status(:inconclusive), do: 1

  defp decide({:relative}, stats, cli_threshold_pct, spread),
    do: decide({:relative, cli_threshold_pct}, stats, cli_threshold_pct, spread)

  defp decide({:relative, threshold_pct}, %{noise_pct: noise} = stats, _cli_threshold_pct, _spread) do
    noise = noise || 0.0

    cond do
      noise > @max_noise_pct ->
        result(stats, :inconclusive, "noise #{fmt(noise)}% exceeds #{fmt(@max_noise_pct)}%")

      stats.delta_pct == nil ->
        result(stats, :inconclusive, "baseline median is zero")

      true ->
        limit = max(threshold_pct, 2 * noise)
        stats = %{stats | limit_pct: limit}

        cond do
          stats.delta_pct > limit -> result(stats, :fail, "worse by #{fmt(stats.delta_pct)}%, limit #{fmt(limit)}%")
          stats.delta_pct < -limit -> result(stats, :improvement, "better by #{fmt(-stats.delta_pct)}%")
          true -> result(stats, :pass, "within #{fmt(limit)}%")
        end
    end
  end

  defp decide({:usable_only}, stats, _threshold_pct, _spread),
    do: result(stats, :pass, "usable runs only, value not judged")

  defp decide({:absolute_us, limit_us}, stats, _threshold_pct, spread) do
    cond do
      spread > limit_us ->
        result(stats, :inconclusive, "run-to-run spread #{fmt(spread)} us exceeds the absolute limit #{limit_us}")

      stats.delta > limit_us ->
        result(stats, :fail, "worse by #{fmt(stats.delta)}, limit #{limit_us}")

      stats.delta < -limit_us ->
        result(stats, :improvement, "better by #{fmt(-stats.delta)}")

      true ->
        result(stats, :pass, "within #{limit_us}")
    end
  end

  defp spread(base, cand), do: Enum.max([range(base), range(cand)])

  defp range([]), do: 0
  defp range(values), do: Enum.max(values) - Enum.min(values)

  defp stats(base, cand, sign) do
    base_median = Stats.median(base)
    cand_median = Stats.median(cand)
    delta = if base_median && cand_median, do: sign * (cand_median - base_median)

    %{
      base_median: base_median,
      cand_median: cand_median,
      delta: delta,
      delta_pct: delta_pct(delta, base_median),
      noise_pct: noise_pct(base, cand),
      base_noise_pct: cv_pct(base),
      cand_noise_pct: cv_pct(cand),
      limit_pct: nil
    }
  end

  defp delta_pct(nil, _base_median), do: nil
  defp delta_pct(_delta, base_median) when base_median == 0, do: nil
  defp delta_pct(delta, base_median), do: delta / base_median * 100

  defp cv_pct(values), do: if(cv = Stats.cv(values), do: cv * 100)

  defp noise_pct(base, cand) do
    case Enum.reject([Stats.cv(base), Stats.cv(cand)], &is_nil/1) do
      [] -> nil
      cvs -> Enum.max(cvs) * 100
    end
  end

  defp result(stats, verdict, reason) do
    counts = "(usable: base #{usable(stats, :base)}, candidate #{usable(stats, :cand)})"
    stats |> Map.put(:verdict, verdict) |> Map.put(:reason, "#{reason} #{counts}")
  end

  defp usable(stats, side), do: "#{stats[:"#{side}_usable"]}/#{stats[:"#{side}_total"]}"

  defp samples(runs, metric) do
    runs
    |> Enum.filter(& &1.valid)
    |> Enum.map(&sample(Map.get(&1, metric)))
    |> Enum.reject(&is_nil/1)
  end

  defp sample(list) when is_list(list), do: Stats.median(list)
  defp sample(value), do: value

  defp invalid_summary(runs, metric) do
    runs
    |> Enum.reject(&(&1.valid && sample(Map.get(&1, metric)) != nil))
    |> Enum.map_join("; ", &(Map.get(&1, :error) || "no #{metric} sample"))
  end

  defp sign(nil), do: 1
  defp sign(:lower_better), do: 1
  defp sign(:higher_better), do: -1

  defp worst([]), do: :inconclusive
  defp worst(verdicts), do: Enum.max_by(verdicts, &Map.fetch!(@severity, &1))

  defp fmt(number), do: :erlang.float_to_binary(number / 1, decimals: 2)
end
