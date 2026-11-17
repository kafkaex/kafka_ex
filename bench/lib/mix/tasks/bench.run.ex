defmodule Mix.Tasks.Bench.Run do
  use Mix.Task
  alias KafkaEx.API
  alias KafkaExBench.{Build, Clock, EnvInfo, Profile, Report, Scenario}

  @shortdoc "Runs a load scenario against the Docker cluster and writes a JSON and Markdown report"

  @scenarios %{
    "basic" => KafkaExBench.Scenarios.Basic,
    "mixed" => KafkaExBench.Scenarios.Mixed,
    "one_broker" => KafkaExBench.Scenarios.OneBroker,
    "large_fetch" => KafkaExBench.Scenarios.LargeFetch,
    "cold_start" => KafkaExBench.Scenarios.ColdStart,
    "restart_recovery" => KafkaExBench.Scenarios.RestartRecovery
  }
  @switches [
    scenario: :string,
    profile: :string,
    security: :string,
    runs: :integer,
    warmup: :integer,
    time: :integer,
    allow_short: :boolean,
    out: :string
  ]

  def run(argv) do
    config = parse!(argv)
    Mix.Task.run("app.start")
    Build.verify!()

    env = EnvInfo.collect(config.profile, [])
    Enum.each(env_warnings(env), &Mix.shell().error("warning: " <> &1))
    started_at = Clock.now_iso8601()
    runs = for index <- 1..config.runs, do: execute_run(config, index)

    warn_leaked(runs)

    Report.write!(config.out, %{
      env: env,
      config: report_config(config, env),
      scenario_params: config.scenario.params(),
      scenario: config.scenario_name,
      started_at: started_at,
      runs: runs
    })

    Mix.shell().info("report written to #{config.out}.json and #{config.out}.md")
  end

  def scenarios, do: @scenarios

  def parse!(argv) do
    {opts, _} = OptionParser.parse!(argv, strict: @switches)
    name = Keyword.get(opts, :scenario, "basic")
    runs = Keyword.get(opts, :runs, 5)

    scenario =
      Map.get(@scenarios, name) ||
        Mix.raise("unknown scenario #{name}, expected one of #{Enum.join(Map.keys(@scenarios), ", ")}")

    out = Keyword.get(opts, :out) || Mix.raise("--out is required")
    if runs < 1, do: Mix.raise("--runs must be at least 1")

    profile = Profile.parse!(Keyword.get(opts, :profile, "direct"))

    defaults = scenario.default_opts()
    time = Keyword.get(opts, :time, defaults[:time])
    short = time < defaults[:time]
    if short and not Keyword.get(opts, :allow_short, false), do: Mix.raise(short_message(name, time, defaults[:time]))

    %{
      scenario: scenario,
      scenario_name: name,
      profile: profile,
      security: Profile.parse_security!(Keyword.get(opts, :security, "ssl")),
      runs: runs,
      warmup: Keyword.get(opts, :warmup, defaults[:warmup]),
      time: time,
      short: short,
      out: out
    }
  end

  defp short_message(name, time, default),
    do:
      "--time #{time} is below the #{name} default of #{default} s, too few samples to trust; add --allow-short for a smoke test"

  defp execute_run(config, index) do
    Mix.shell().info("run #{index}/#{config.runs} started")
    started_at = Clock.now_iso8601()
    started = System.monotonic_time(:millisecond)

    result =
      Scenario.run_with_client(config.scenario, config.profile,
        start_client: fn -> API.start_client(Profile.client_opts(config.profile, config.security)) end,
        warmup: config.warmup,
        time: config.time,
        on_job: fn record -> Mix.shell().info(job_line(index, record)) end,
        on_invalidated: fn message -> Mix.shell().info(invalidation_line(index, message)) end
      )

    %{
      run: index,
      started_at: started_at,
      duration_ms: System.monotonic_time(:millisecond) - started,
      layout: result.layout,
      jobs: result.jobs,
      cluster_topic_count: result.cluster_topic_count,
      leaked_bench_topics: result.leaked_bench_topics
    }
  end

  def env_warnings(env) do
    for key <- [:kafka_ex_sha, :harness_sha], is_nil(Map.get(env, key)), do: "#{key} is MISSING"
  end

  def report_config(config, env) do
    %{
      scenario: config.scenario_name,
      profile: config.profile,
      security: config.security,
      runs: config.runs,
      warmup: config.warmup,
      time: config.time,
      short: config.short,
      latency_ms: env.latency_ms
    }
  end

  def leak_warning(runs) do
    leaks =
      for %{run: run, leaked_bench_topics: count} <- runs, is_integer(count) and count > 0, do: "run #{run}: #{count}"

    if leaks != [],
      do: "leaked bench- topics found before setup (#{Enum.join(leaks, ", ")}); delete them before trusting the results"
  end

  defp warn_leaked(runs) do
    if message = leak_warning(runs), do: Mix.shell().error("warning: " <> message)
  end

  def invalidation_line(index, message), do: "run=#{index} invalidated: #{message}"

  def job_line(index, job) do
    "run=#{index} job=#{job.job} valid=#{job.valid} thr=#{fmt(job.throughput_ops)} p50=#{fmt(job.p50_us)} " <>
      "p99=#{fmt(job.p99_us)} mbox=#{fmt(job.max_mailbox)}" <> if(job.error, do: " error=#{job.error}", else: "")
  end

  defp fmt(nil), do: "n/a"
  defp fmt(value) when is_float(value), do: :erlang.float_to_binary(value, decimals: 1)
  defp fmt(value), do: to_string(value)
end
