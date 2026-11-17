defmodule KafkaExBench.Runner do
  alias KafkaExBench.{Job, Sampler, Stats}

  @error_limit 500
  @no_observation %{max_mailbox: 0, reductions: 0, samples: 0}

  def run_job(%Job{} = job, client_pid, opts) do
    warmup = Keyword.fetch!(opts, :warmup)
    time = Keyword.fetch!(opts, :time)
    min_samples = Keyword.get(opts, :min_samples, job.min_samples)
    timeout_ms = Keyword.get_lazy(opts, :timeout_ms, fn -> round(((warmup + time) * 3 + 30) * 1000) end)

    measure? = Keyword.get(opts, :measure, true)

    with_background(job, min_samples, fn ->
      case warm_up(job, warmup, timeout_ms) do
        :ok -> measure(job, client_pid, measure?, time, timeout_ms, min_samples)
        {:error, message} -> build_record(job, {:error, "warmup: " <> message}, min_samples, @no_observation, nil)
      end
    end)
  end

  defp with_background(%Job{background: nil}, _min_samples, run), do: Map.put(run.(), :background_ops, nil)

  defp with_background(%Job{background: background} = job, min_samples, run) do
    case guarded(fn -> background.start.() end) do
      {:ok, state} ->
        record =
          try do
            run.()
          catch
            kind, reason ->
              stacktrace = __STACKTRACE__
              stop_background(background, state)
              :erlang.raise(kind, reason, stacktrace)
          end

        {outcome, ops} = stop_background(background, state)
        record |> append_background_error(outcome) |> Map.put(:background_ops, ops)

      {:error, message} ->
        build_record(job, {:error, "background: " <> message}, min_samples, @no_observation, nil)
        |> Map.put(:background_ops, nil)
    end
  end

  defp stop_background(background, state) do
    case guarded(fn -> background.stop.(state) end) do
      {:ok, {:ok, ops}} -> {:ok, ops}
      {:ok, {:error, message, ops}} when is_binary(message) -> {{:error, message}, ops}
      {:ok, other} -> {{:error, sanitize_error(other)}, nil}
      {:error, message} -> {{:error, message}, nil}
    end
  end

  defp append_background_error(record, :ok), do: record

  defp append_background_error(%{valid: true} = record, {:error, message}),
    do: invalid(record, "background: " <> message)

  defp append_background_error(record, {:error, message}),
    do: %{record | error: record.error <> "; background: " <> message}

  def guarded(fun) do
    {:ok, fun.()}
  rescue
    error -> {:error, sanitize_error(error)}
  catch
    kind, reason -> {:error, sanitize_error({:caught, kind, reason})}
  end

  defp warm_up(_job, warmup, _timeout_ms) when warmup <= 0, do: :ok

  defp warm_up(job, warmup, timeout_ms) do
    with {:ok, _samples} <- benchmark(job, warmup, timeout_ms, nil, false) |> elem(0), do: :ok
  end

  defp measure(job, client_pid, measure?, time, timeout_ms, min_samples) do
    {outcome, window} = benchmark(job, time, timeout_ms, client_pid, measure?)
    build_record(job, outcome, min_samples, window.observed, vm_window(window, job.parallel))
  end

  defp vm_window(%{before: before, after: after_marks}, parallel)
       when length(before) == parallel and length(after_marks) == parallel do
    {reductions_start, runtime_start} = Enum.min(before)
    {reductions_end, runtime_end} = Enum.max(after_marks)
    %{reductions: reductions_end - reductions_start, runtime_ms: max(runtime_end - runtime_start, 0)}
  end

  defp vm_window(_incomplete, _parallel), do: nil

  defp snapshot do
    {reductions, _} = :erlang.statistics(:reductions)
    {runtime_ms, _} = :erlang.statistics(:runtime)
    {reductions, runtime_ms}
  end

  defp window_hooks(false, _parent, _ref), do: []

  defp window_hooks(true, parent, ref) do
    [
      before_scenario: fn input ->
        send(parent, {ref, :before, snapshot()})
        input
      end,
      after_scenario: fn input ->
        send(parent, {ref, :after, snapshot()})
        input
      end
    ]
  end

  defp benchmark(job, time, timeout_ms, client_pid, measure?) do
    parent = self()
    ref = make_ref()
    hooks = window_hooks(measure?, parent, ref)

    {pid, monitor} =
      spawn_monitor(fn ->
        suite =
          Benchee.run(
            %{job.name => job.fun},
            [
              parallel: job.parallel,
              warmup: 0,
              time: time,
              memory_time: 0,
              reduction_time: 0,
              pre_check: false,
              formatters: [],
              print: [benchmarking: false, configuration: false, fast_warning: false]
            ] ++ hooks
          )

        send(parent, {ref, suite})
      end)

    window = %{sampler: nil, observed: @no_observation, before: [], after: []}
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    {outcome, window} = await(%{ref: ref, pid: pid, monitor: monitor, job: job, client: client_pid}, deadline, window)
    {outcome, stop_sampler(window)}
  end

  defp await(run, deadline, window) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {ref, :before, snapshot} when ref == run.ref ->
        sampler = window.sampler || start_sampler(run.client)
        await(run, deadline, %{window | sampler: sampler, before: [snapshot | window.before]})

      {ref, :after, snapshot} when ref == run.ref ->
        window = %{window | after: [snapshot | window.after]}
        window = if length(window.after) == run.job.parallel, do: stop_sampler(window), else: window
        await(run, deadline, window)

      {ref, suite} when ref == run.ref ->
        Process.demonitor(run.monitor, [:flush])
        {{:ok, suite.scenarios |> hd() |> then(& &1.run_time_data.samples)}, window}

      {:DOWN, monitor, :process, pid, reason} when monitor == run.monitor and pid == run.pid ->
        {{:error, sanitize_error(reason)}, window}
    after
      remaining ->
        Process.exit(run.pid, :kill)

        receive do
          {:DOWN, monitor, :process, _, _} when monitor == run.monitor -> :ok
        end

        {{:error, "timeout"}, window}
    end
  end

  defp start_sampler(nil), do: nil
  defp start_sampler(client), do: Sampler.start(client, 10)

  defp stop_sampler(%{sampler: nil} = window), do: window

  defp stop_sampler(%{sampler: sampler} = window),
    do: %{window | sampler: nil, observed: Sampler.stop(sampler)}

  defp build_record(job, {:ok, samples}, min_samples, observed, vm) do
    count = length(samples)

    base = %{
      job: job.name,
      parallel: job.parallel,
      samples: count,
      min_samples: min_samples,
      metrics: job.metrics,
      max_mailbox: observed.max_mailbox,
      reductions: observed.reductions
    }

    if count < max(min_samples, 1) do
      invalid(base, "too few samples: #{count}")
    else
      Map.merge(base, %{
        valid: true,
        error: nil,
        throughput_ops: throughput(job.parallel, samples),
        p50_us: trunc(Stats.percentile(samples, 50) / 1000),
        p99_us: trunc(Stats.percentile(samples, 99) / 1000),
        client_reductions_per_op: client_per_op(vm, observed, count),
        vm_reductions_per_op: vm && vm.reductions / count,
        vm_cpu_us_per_op: vm && vm.runtime_ms * 1000 / count
      })
    end
  end

  defp build_record(job, {:error, message}, min_samples, observed, _vm) do
    invalid(
      %{
        job: job.name,
        parallel: job.parallel,
        samples: 0,
        min_samples: min_samples,
        metrics: job.metrics,
        max_mailbox: observed.max_mailbox,
        reductions: observed.reductions
      },
      message
    )
  end

  defp client_per_op(vm, %{samples: observations} = observed, count) when vm != nil and observations > 0,
    do: observed.reductions / count

  defp client_per_op(_vm, _observed, _count), do: nil

  defp invalid(base, message) do
    Map.merge(base, %{
      valid: false,
      error: message,
      throughput_ops: nil,
      p50_us: nil,
      p99_us: nil,
      client_reductions_per_op: nil,
      vm_reductions_per_op: nil,
      vm_cpu_us_per_op: nil
    })
  end

  def throughput(parallel, samples), do: parallel * 1.0e9 / (Enum.sum(samples) / length(samples))

  def sanitize_error(reason) do
    reason
    |> describe()
    |> mask_home(System.get_env("HOME"))
    |> String.replace(~r/\s*[\r\n]+\s*/, " ")
    |> String.trim()
    |> String.slice(0, @error_limit)
  end

  def mask_home(text, home) when is_binary(home) and byte_size(home) > 1, do: String.replace(text, home, "~")
  def mask_home(text, _home), do: text

  defp describe({%{__exception__: true} = exception, stacktrace}) when is_list(stacktrace),
    do: Exception.format_banner(:error, exception, stacktrace)

  defp describe({reason, {module, function, args}}) when is_atom(module) and is_atom(function) and is_list(args),
    do: "** (exit) " <> exit_text({reason, {module, function, args}})

  defp describe({:caught, :exit, reason}), do: "** (exit) " <> exit_text(reason)
  defp describe({:caught, kind, reason}), do: Exception.format_banner(kind, reason)

  defp describe({reason, [first | _] = stacktrace}) when is_tuple(first) and tuple_size(first) == 4,
    do: Exception.format_banner(:error, Exception.normalize(:error, reason, stacktrace), stacktrace)

  defp describe(%{__exception__: true} = exception), do: Exception.format_banner(:error, exception)
  defp describe(reason), do: inspect(reason)

  defp exit_text({reason, {module, function, args}}) when is_atom(module) and is_atom(function) and is_list(args) do
    "exited in: #{inspect(module)}.#{function}(#{Enum.map_join(args, ", ", &inspect/1)}) ** (EXIT) #{exit_reason(reason)}"
  end

  defp exit_text(reason), do: exit_reason(reason)

  defp exit_reason({_reason, [first | _]} = pair) when is_tuple(first) and tuple_size(first) == 4,
    do: pair |> describe() |> String.replace_prefix("** ", "")

  defp exit_reason({%{__exception__: true}, stacktrace} = pair) when is_list(stacktrace),
    do: pair |> describe() |> String.replace_prefix("** ", "")

  defp exit_reason(reason), do: Exception.format_exit(reason)
end
