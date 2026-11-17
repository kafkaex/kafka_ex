defmodule KafkaExBench.OneShotJob do
  @enforce_keys [:name, :repetitions, :fun]
  defstruct [:name, :repetitions, :fun, timeout_ms: 120_000, discard: 0]

  @type t :: %__MODULE__{}
end

defmodule KafkaExBench.OneShot do
  alias KafkaExBench.{Clock, Runner, Stats}

  @metrics [{:p50_us, :lower_better}]
  @default_timeout_ms 120_000

  def metrics, do: @metrics

  def measure(name, repetitions, fun, opts) when repetitions >= 1 do
    started_at = Clock.now_iso8601()
    started = System.monotonic_time(:millisecond)
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)

    outcome =
      with :ok <- discard(Keyword.get(opts, :discard, 0), fun, timeout_ms),
           do: collect(repetitions, fun, timeout_ms, [], %{})

    Map.merge(record(name, repetitions, outcome), %{
      started_at: started_at,
      duration_ms: System.monotonic_time(:millisecond) - started
    })
  end

  defp discard(0, _fun, _timeout_ms), do: :ok

  defp discard(remaining, fun, timeout_ms) do
    case attempt(fun, timeout_ms) do
      {:ok, {:ok, _duration_us}} -> discard(remaining - 1, fun, timeout_ms)
      {:ok, {:ok, _duration_us, _extras}} -> discard(remaining - 1, fun, timeout_ms)
      {:ok, {:error, reason}} -> {:error, Runner.sanitize_error(reason)}
      {:ok, other} -> {:error, "unexpected result: " <> Runner.sanitize_error(other)}
      {:error, message} -> {:error, message}
    end
  end

  defp collect(0, _fun, _timeout_ms, durations, extras), do: {:ok, durations, extras}

  defp collect(remaining, fun, timeout_ms, durations, extras) do
    case attempt(fun, timeout_ms) do
      {:ok, {:ok, duration_us}} when is_number(duration_us) ->
        collect(remaining - 1, fun, timeout_ms, [duration_us | durations], extras)

      {:ok, {:ok, duration_us, more}} when is_number(duration_us) and is_map(more) ->
        collect(
          remaining - 1,
          fun,
          timeout_ms,
          [duration_us | durations],
          Map.merge(extras, more, &merge_extra/3)
        )

      {:ok, {:error, reason}} ->
        {:error, Runner.sanitize_error(reason)}

      {:ok, other} ->
        {:error, "unexpected result: " <> Runner.sanitize_error(other)}

      {:error, message} ->
        {:error, message}
    end
  end

  defp merge_extra(_key, a, b) when is_list(a) and is_list(b), do: a ++ b
  defp merge_extra(_key, a, b), do: a + b

  defp attempt(fun, timeout_ms) do
    parent = self()
    ref = make_ref()
    {pid, monitor} = spawn_monitor(fn -> send(parent, {ref, attempt_here(fun)}) end)

    receive do
      {^ref, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, _, _, reason} ->
        {:error, Runner.sanitize_error({:exit, reason})}
    after
      timeout_ms ->
        Process.exit(pid, :kill)
        Process.demonitor(monitor, [:flush])

        receive do
          {^ref, _} -> :ok
        after
          0 -> :ok
        end

        {:error, "timeout after #{timeout_ms} ms"}
    end
  end

  defp attempt_here(fun) do
    {:ok, fun.()}
  rescue
    error -> {:error, Runner.sanitize_error(error)}
  catch
    kind, reason -> {:error, Runner.sanitize_error({:caught, kind, reason})}
  end

  defp record(name, repetitions, {:ok, durations, extras}) do
    base(name, repetitions, true, nil, repetitions)
    |> Map.merge(%{p50_us: round(Stats.median(durations)), p99_us: Enum.max(durations)})
    |> Map.merge(extras)
  end

  defp record(name, repetitions, {:error, message}),
    do: base(name, repetitions, false, message, 0) |> Map.merge(%{p50_us: nil, p99_us: nil})

  defp base(name, repetitions, valid, error, samples) do
    %{
      job: name,
      parallel: 1,
      valid: valid,
      error: error,
      samples: samples,
      min_samples: repetitions,
      metrics: @metrics,
      throughput_ops: nil,
      max_mailbox: nil,
      reductions: nil,
      client_reductions_per_op: nil,
      vm_reductions_per_op: nil,
      vm_cpu_us_per_op: nil
    }
  end
end
