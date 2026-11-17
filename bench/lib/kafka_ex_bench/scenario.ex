defmodule KafkaExBench.Scenario do
  alias KafkaEx.API
  alias KafkaExBench.{Clock, Cluster, Job, OneShot, OneShotJob, Profile, Runner}

  @type rule :: {:relative} | {:relative, number()} | {:absolute_us, pos_integer()} | {:usable_only}
  @type gate :: {atom(), :higher_better | :lower_better, rule()}

  @callback job_specs() :: [{String.t(), pos_integer()}]
  @callback default_opts() :: [time: number(), warmup: number(), min_valid: pos_integer()]
  @callback setup(client :: pid(), profile :: atom()) :: ctx :: term()
  @callback params() :: map()
  @callback gates() :: %{String.t() => [gate()]}
  @callback layout(ctx :: term()) :: map()
  @callback jobs(client :: pid(), ctx :: term()) :: [Job.t() | OneShotJob.t()]
  @callback check(client :: pid(), ctx :: term()) :: :ok | {:error, String.t()}
  @callback teardown(client :: pid(), ctx :: term()) :: :ok

  def run_with_client(scenario, profile, opts) do
    start_client = Keyword.fetch!(opts, :start_client)
    previous = Process.flag(:trap_exit, true)

    try do
      case attempt(start_client) do
        {:ok, {:ok, client}} -> run_supervised(scenario, client, profile, opts)
        {:ok, other} -> failed(scenario, "client start failed: #{Runner.sanitize_error(other)}", opts)
        {:error, message} -> failed(scenario, "client start failed: #{message}", opts)
      end
    after
      Process.flag(:trap_exit, previous)
    end
  end

  defp run_supervised(scenario, client, profile, opts) do
    result =
      try do
        run_once(scenario, client, profile, opts)
      after
        stop_client(client, Keyword.get(opts, :stop_timeout, 10_000))
      end

    case client_exit(client) do
      nil -> result
      reason -> invalidated(scenario, "client exited: #{Runner.sanitize_error(reason)}", opts)
    end
  end

  defp stop_client(client, timeout) do
    GenServer.stop(client, :normal, timeout)
  rescue
    _ -> kill_if_alive(client)
  catch
    _, _ -> kill_if_alive(client)
  end

  defp kill_if_alive(client) do
    if Process.alive?(client) do
      monitor = Process.monitor(client)
      Process.exit(client, :kill)

      receive do
        {:DOWN, ^monitor, _, _, _} -> :ok
      after
        5_000 -> Process.demonitor(monitor, [:flush])
      end
    end

    :ok
  end

  defp client_exit(client) do
    receive do
      {:EXIT, ^client, reason} when reason in [:normal, :shutdown] -> nil
      {:EXIT, ^client, {:shutdown, _}} -> nil
      {:EXIT, ^client, reason} -> reason
    after
      50 -> nil
    end
  end

  def run_once(scenario, client, profile, opts) do
    prepare = Keyword.get(opts, :prepare, &Profile.prepare!/1)

    cluster_info = Keyword.get(opts, :cluster_info, &cluster_info/1)

    expected = Cluster.expected_brokers()

    case attempt(fn ->
           prepare.(profile)
           cluster_info.(client)
         end) do
      {:ok, %{brokers: brokers} = info} when brokers == expected -> setup_and_run(scenario, client, profile, info, opts)
      {:ok, %{brokers: brokers} = info} -> unhealthy(scenario, brokers, info, opts)
      {:error, message} -> failed(scenario, message, opts)
    end
  end

  def steady_gates(job_specs, opts \\ []) do
    vm_reductions = Keyword.get(opts, :vm_reductions, false)

    Map.new(job_specs, fn {name, parallel} ->
      {name,
       [{:p50_us, :lower_better, {:relative}}] ++
         if(parallel > 1, do: [{:throughput_ops, :higher_better, {:relative}}], else: []) ++
         if(vm_reductions, do: [{:vm_reductions_per_op, :lower_better, {:relative}}], else: [])}
    end)
  end

  def leaked_count(topics), do: Enum.count(topics, &String.starts_with?(&1, "bench-"))

  defp cluster_info(client) do
    {:ok, metadata} = API.metadata(client, nil, [])
    topics = Map.keys(metadata.topics)
    %{brokers: map_size(metadata.brokers), topic_count: length(topics), leaked_bench_topics: leaked_count(topics)}
  end

  defp unhealthy(scenario, brokers, info, opts) do
    jobs = reported_invalid_jobs(scenario, "cluster unhealthy: #{brokers} brokers", opts)
    Map.merge(%{layout: nil, jobs: jobs}, counts(info))
  end

  defp setup_and_run(scenario, client, profile, info, opts) do
    case attempt(fn -> scenario.setup(client, profile) end) do
      {:ok, ctx} -> scenario |> run_prepared(client, ctx, opts) |> Map.merge(counts(info))
      {:error, message} -> Map.merge(failed(scenario, message, opts), counts(info))
    end
  end

  defp counts(info), do: %{cluster_topic_count: info.topic_count, leaked_bench_topics: info.leaked_bench_topics}

  defp run_prepared(scenario, client, ctx, opts) do
    measured = attempt(fn -> measure(scenario, client, ctx, opts) end)
    torn_down = attempt(fn -> scenario.teardown(client, ctx) end)

    jobs =
      case {measured, torn_down} do
        {{:ok, records}, {:ok, _}} ->
          records

        {{:ok, records}, {:error, message}} ->
          if Enum.all?(records, & &1.valid) do
            invalidated_jobs(scenario, "teardown failed: #{message}", opts)
          else
            warn_teardown(message)
            records
          end

        {{:error, message}, torn_down} ->
          with {:error, teardown_message} <- torn_down, do: warn_teardown(teardown_message)
          invalidated_jobs(scenario, message, opts)
      end

    %{layout: scenario.layout(ctx), jobs: jobs}
  end

  defp measure(scenario, client, ctx, opts) do
    run_opts = Keyword.take(opts, [:warmup, :time, :min_samples])
    on_job = on_job(opts)

    scenario.jobs(client, ctx)
    |> Enum.map_reduce(nil, fn
      job, nil ->
        record = timed(fn -> job |> run_job(client, run_opts) |> checked(scenario, client, ctx) end)
        on_job.(record)
        {record, if(record.valid, do: nil, else: job.name)}

      job, failed ->
        record = skipped_record(job, failed)
        on_job.(record)
        {record, failed}
    end)
    |> elem(0)
  end

  defp run_job(%Job{} = job, client, run_opts), do: Runner.run_job(job, client, [measure: true] ++ run_opts)

  defp run_job(%OneShotJob{} = job, _client, run_opts),
    do:
      OneShot.measure(
        job.name,
        job.repetitions,
        job.fun,
        [timeout_ms: job.timeout_ms, discard: job.discard] ++ run_opts
      )

  defp skipped_record(%Job{} = job, failed),
    do: job.name |> invalid_record(job.parallel, "skipped after #{failed} failed") |> Map.put(:metrics, job.metrics)

  defp skipped_record(%OneShotJob{} = job, failed),
    do: job.name |> invalid_record(1, "skipped after #{failed} failed") |> Map.put(:metrics, OneShot.metrics())

  defp timed(fun) do
    started_at = Clock.now_iso8601()
    started = System.monotonic_time(:millisecond)
    record = fun.()
    Map.merge(record, %{started_at: started_at, duration_ms: System.monotonic_time(:millisecond) - started})
  end

  defp checked(%{valid: false} = record, _scenario, _client, _ctx), do: record

  defp checked(record, scenario, client, ctx) do
    case attempt(fn -> scenario.check(client, ctx) end) do
      {:ok, :ok} -> record
      {:ok, {:error, message}} -> cluster_changed(record, message)
      {:error, message} -> cluster_changed(record, message)
    end
  end

  defp cluster_changed(record, message) do
    record.job
    |> invalid_record(record.parallel, "cluster changed: " <> message)
    |> Map.merge(Map.take(record, [:min_samples, :metrics]))
  end

  defp attempt(fun) do
    {:ok, fun.()}
  rescue
    error -> {:error, Runner.sanitize_error(error)}
  catch
    kind, reason -> {:error, Runner.sanitize_error({:caught, kind, reason})}
  end

  defp on_job(opts), do: Keyword.get(opts, :on_job, fn _record -> :ok end)

  defp failed(scenario, message, opts) do
    %{
      layout: nil,
      jobs: reported_invalid_jobs(scenario, message, opts),
      cluster_topic_count: nil,
      leaked_bench_topics: nil
    }
  end

  defp invalidated(scenario, message, opts) do
    %{layout: nil, jobs: invalidated_jobs(scenario, message, opts), cluster_topic_count: nil, leaked_bench_topics: nil}
  end

  defp invalidated_jobs(scenario, message, opts) do
    Keyword.get(opts, :on_invalidated, fn _message -> :ok end).(message)
    invalid_jobs(scenario, message)
  end

  defp reported_invalid_jobs(scenario, message, opts) do
    jobs = invalid_jobs(scenario, message)
    Enum.each(jobs, on_job(opts))
    jobs
  end

  defp invalid_jobs(scenario, message) do
    for {name, parallel} <- scenario.job_specs(), do: invalid_record(name, parallel, message)
  end

  defp invalid_record(name, parallel, message) do
    %{
      job: name,
      parallel: parallel,
      valid: false,
      error: message,
      samples: 0,
      min_samples: nil,
      metrics: Job.default_metrics(),
      throughput_ops: nil,
      p50_us: nil,
      p99_us: nil,
      max_mailbox: nil,
      reductions: nil,
      client_reductions_per_op: nil,
      vm_reductions_per_op: nil,
      vm_cpu_us_per_op: nil,
      background_ops: nil,
      started_at: Clock.now_iso8601(),
      duration_ms: 0
    }
  end

  defp warn_teardown(message), do: IO.warn("teardown failed after a failed run: #{message}")
end
