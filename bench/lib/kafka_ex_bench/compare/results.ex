defmodule KafkaExBench.Compare.Results do
  @pairing_fields [{"time", [:config, "time"]}, {"warmup", [:config, "warmup"]}, {"short", [:config, "short"]}]

  def read_run(base_path) do
    with {:ok, binary} <- read(base_path <> ".json"),
         {:ok, decoded} <- decode(binary) do
      {:ok, normalize(decoded)}
    end
  end

  defp read(path) do
    case File.read(path) do
      {:ok, binary} -> {:ok, binary}
      {:error, reason} -> {:failed, "no report at #{path}: #{:file.format_error(reason)}"}
    end
  end

  defp decode(binary) do
    case Jason.decode(binary) do
      {:ok, %{"runs" => runs} = decoded} when is_list(runs) -> {:ok, decoded}
      _ -> {:failed, "unreadable report"}
    end
  end

  defp normalize(decoded) do
    %{
      env: decoded["env"],
      config: decoded["config"],
      scenario_params: decoded["scenario_params"],
      runs: Enum.map(decoded["runs"], &run/1)
    }
  end

  defp run(record) do
    record |> atomize() |> Map.update(:jobs, [], fn jobs -> Enum.map(jobs, &atomize/1) end)
  end

  defp atomize(map), do: Map.new(map, fn {key, value} -> {String.to_atom(key), value} end)

  def job_runs(entries, job_names) do
    per_entry = Enum.map(entries, &entry_jobs(&1, job_names))
    Map.new(job_names, fn name -> {name, Enum.flat_map(per_entry, &Map.get(&1, name, []))} end)
  end

  defp entry_jobs({:ok, report}, _job_names) do
    report.runs |> Enum.flat_map(& &1.jobs) |> Enum.group_by(& &1.job)
  end

  defp entry_jobs({:failed, reason}, job_names), do: Map.new(job_names, &{&1, [invalid(&1, reason)]})

  defp invalid(name, reason), do: %{job: name, valid: false, error: reason}

  def check_pairing(base_entries, candidate_entries) do
    reports = for {:ok, report} <- base_entries ++ candidate_entries, do: report

    refusals =
      for {label, path} <- @pairing_fields ++ [{"scenario_params", [:scenario_params]}],
          values = reports |> Enum.map(&get_in(&1, path)) |> Enum.uniq(),
          length(values) > 1,
          do: "#{label} differs across runs: #{inspect(values)}"

    counts = %{base: topic_counts(base_entries), candidate: topic_counts(candidate_entries)}

    case refusals do
      [] -> {:ok, topic_warnings(counts), counts}
      _ -> {:error, refusals}
    end
  end

  defp topic_counts(entries) do
    for {:ok, report} <- entries, run <- report.runs, is_integer(run[:cluster_topic_count]), do: run.cluster_topic_count
  end

  defp topic_warnings(counts) do
    case counts |> Map.values() |> List.flatten() |> Enum.uniq() do
      distinct when length(distinct) > 1 ->
        [
          "cluster_topic_count differs between runs: base #{inspect(counts.base)}, candidate #{inspect(counts.candidate)}"
        ]

      _ ->
        []
    end
  end

  def abort_reason({:failed, _reason}), do: nil

  def abort_reason({:ok, report}) do
    leaked =
      for run <- report.runs,
          is_integer(run[:leaked_bench_topics]),
          run.leaked_bench_topics > 0,
          do: run.leaked_bench_topics

    unhealthy = for run <- report.runs, job <- run.jobs, unhealthy?(job), do: job.error

    cond do
      leaked != [] -> "#{Enum.sum(leaked)} leaked bench- topics found before the run; delete them and rerun"
      unhealthy != [] -> "cluster unhealthy: #{hd(unhealthy)}"
      true -> nil
    end
  end

  defp unhealthy?(%{error: error}) when is_binary(error), do: String.starts_with?(error, "cluster unhealthy")
  defp unhealthy?(_job), do: false
end
