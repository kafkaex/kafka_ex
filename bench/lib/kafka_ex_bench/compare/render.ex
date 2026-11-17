defmodule KafkaExBench.Compare.Render do
  alias KafkaExBench.Runner

  def write!(out, compare) do
    File.mkdir_p!(out)
    File.write!(Path.join(out, "compare.json"), json(compare))
    File.write!(Path.join(out, "compare.md"), markdown(compare))
  end

  def json(compare), do: compare |> Jason.encode!(pretty: true) |> mask_home()

  def markdown(compare) do
    [
      "# Comparison\n\n",
      "generated: #{compare.generated_at}\n\n",
      "Overall verdict: #{verdict_label(compare)}\n\n",
      abort(compare),
      settings(compare.config),
      env_table(compare.env),
      failures(compare.failures),
      Enum.map(compare.cells, &cell/1)
    ]
    |> IO.iodata_to_binary()
    |> mask_home()
  end

  defp mask_home(text), do: Runner.mask_home(text, System.get_env("HOME"))

  defp abort(%{abort: reason}), do: "ABORTED: #{reason}\n\n"
  defp abort(_compare), do: []

  defp verdict_label(%{verdict: :not_gated, config: %{allow_ungated: true}}), do: "NOT GATED (allowed)"
  defp verdict_label(%{verdict: verdict}), do: label(verdict)

  defp settings(config) do
    candidate_env = Enum.map_join(config.candidate_env, ", ", fn {key, value} -> "#{key}=#{value}" end)

    [
      "## Settings\n\n",
      "- base: #{config.base}\n",
      "- candidate: #{config.candidate}\n",
      "- runs per side: #{config.runs}\n",
      "- threshold: #{config.threshold_pct}%\n",
      "- min valid runs: #{config.min_valid || "scenario default"}\n",
      "- profiles: #{Enum.join(config.profiles, ", ")}\n",
      "- scenarios: #{Enum.join(config.scenarios, ", ")}\n",
      "- candidate env: #{if candidate_env == "", do: "none", else: candidate_env}\n\n"
    ]
  end

  defp env_table(%{base: base, candidate: candidate}) do
    keys = (Map.keys(base || %{}) ++ Map.keys(candidate || %{})) |> Enum.uniq() |> Enum.sort()

    rows =
      for key <- keys,
          do: "| #{key} | #{env_value((base || %{})[key])} | #{env_value((candidate || %{})[key])} |\n"

    ["## Environment\n\n", "| key | base | candidate |\n", "| --- | --- | --- |\n", rows, "\n"]
  end

  defp env_value(nil), do: "n/a"
  defp env_value(%{} = map), do: Enum.map_join(map, ", ", fn {key, value} -> "#{key} #{value}" end)
  defp env_value(list) when is_list(list), do: inspect(list)
  defp env_value(value), do: to_string(value)

  defp failures([]), do: []

  defp failures(failures) do
    lines =
      for failure <- failures,
          do: "- #{failure.side} #{failure.profile} #{failure.scenario} run #{failure.index}: #{failure.reason}\n"

    ["## Failed runs\n\n", lines, "\n"]
  end

  defp cell(%{gated: true} = cell) do
    [
      "## #{cell.profile} / #{cell.scenario}: #{label(cell.verdict)}\n\n",
      warnings(cell.warnings),
      refusal(cell.refusal),
      gate_table(cell.jobs),
      informational(cell.informational),
      extras(cell.extras)
    ]
  end

  defp cell(cell) do
    [
      "## #{cell.profile} / #{cell.scenario} (informational, not gated)\n\n",
      warnings(cell.warnings),
      refusal(cell.refusal),
      informational(cell.informational),
      extras(cell.extras)
    ]
  end

  defp warnings(warnings), do: for(warning <- warnings, do: "warning: #{warning}\n\n")

  defp refusal(nil), do: []
  defp refusal(refusals), do: ["pairing refused:\n\n", for(refusal <- refusals, do: "- #{refusal}\n"), "\n"]

  defp gate_table([]), do: []

  defp gate_table(jobs) do
    rows = for job <- jobs, row <- gate_rows(job), do: row

    [
      "| job | metric | usable base/cand | base median | candidate median | delta % | noise % | noise base/cand % | limit % | verdict | reason |\n",
      "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n",
      rows,
      "\n"
    ]
  end

  defp gate_rows(%{gates: [], job: job, verdict: verdict}),
    do: [row([job, "(no gates)", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", label(verdict), "no gates declared"])]

  defp gate_rows(%{gates: gates, job: job}) do
    for gate <- gates do
      row([
        job,
        gate.metric,
        "#{gate.base_usable}/#{gate.base_total} vs #{gate.cand_usable}/#{gate.cand_total}",
        number(gate.base_median),
        number(gate.cand_median),
        percent(gate.delta_pct),
        percent(gate.noise_pct),
        "#{percent(gate.base_noise_pct)} / #{percent(gate.cand_noise_pct)}",
        percent(gate.limit_pct),
        label(gate.verdict),
        gate.reason
      ])
    end
  end

  defp informational([]), do: []

  defp informational(rows) do
    {directed, raw} = Enum.split_with(rows, & &1.direction)
    [informational_table(directed, "worse Δ %"), informational_table(raw, "raw Δ %")]
  end

  defp informational_table([], _delta_header), do: []

  defp informational_table(rows, delta_header) do
    body =
      for info <- rows do
        row([
          info.job,
          info.metric,
          "#{info.base_usable}/#{info.base_total} vs #{info.cand_usable}/#{info.cand_total}",
          number(info.base_median),
          number(info.cand_median),
          percent(info.delta_pct),
          percent(info.noise_pct)
        ])
      end

    [
      "Informational:\n\n",
      "| job | metric | usable base/cand | base median | candidate median | #{delta_header} | noise % |\n",
      "| --- | --- | --- | --- | --- | --- | --- |\n",
      body,
      "\n"
    ]
  end

  defp extras([]), do: []

  defp extras(rows) do
    body =
      for extra <- rows,
          do: row([extra.job, extra.metric, extra_value(extra.base), extra_value(extra.candidate), extra.agg])

    [
      "Extras (median or sum across runs):\n\n",
      "| job | metric | base | candidate | aggregate |\n",
      "| --- | --- | --- | --- | --- |\n",
      body,
      "\n"
    ]
  end

  defp extra_value(nil), do: "n/a"
  defp extra_value(value) when is_float(value), do: number(value)
  defp extra_value(value), do: to_string(value)

  defp row(cells),
    do: "| " <> Enum.map_join(cells, " | ", &(&1 |> to_string() |> String.replace("|", "\\|"))) <> " |\n"

  defp number(nil), do: "n/a"
  defp number(value), do: :erlang.float_to_binary(value / 1, decimals: 1)

  defp percent(nil), do: "n/a"
  defp percent(value), do: :erlang.float_to_binary(value / 1, decimals: 2)

  defp label(:not_gated), do: "NOT GATED"
  defp label(verdict), do: verdict |> to_string() |> String.upcase()
end
