defmodule KafkaExBench.Compare.Plan do
  @erl_flags "+S 8:8 +sbwt none +sbwtdcpu none +sbwtdio none"
  @last_scenarios ["restart_recovery"]

  def runs(scenarios, profiles, runs_per_side) do
    {last, regular} = Enum.split_with(scenarios, &(&1 in @last_scenarios))
    block(profiles, regular, runs_per_side) ++ block(profiles, last, runs_per_side)
  end

  defp block(profiles, scenarios, runs_per_side) do
    for profile <- profiles,
        scenario <- scenarios,
        index <- 1..runs_per_side//1,
        side <- pair_order(index),
        do: %{side: side, profile: profile, scenario: scenario, index: index}
  end

  defp pair_order(index) when rem(index, 2) == 1, do: [:base, :candidate]
  defp pair_order(_index), do: [:candidate, :base]

  def output_base(out, run), do: Path.join([out, "runs", "#{run.side}-#{run.profile}-#{run.scenario}-#{run.index}"])

  def compile_command(side), do: %{cmd: "mix", args: ["compile"], env: build_env(side)}

  def command(run, side, config) do
    args =
      ["--erl", @erl_flags, "-S", "mix", "bench.run"] ++
        ["--scenario", run.scenario, "--profile", to_string(run.profile), "--runs", "1"] ++
        ["--out", output_base(config.out, run)] ++
        time_args(config)

    %{cmd: "elixir", args: args, env: build_env(side)}
  end

  defp build_env(side),
    do: [{"KAFKA_EX_PATH", side.path}, {"BENCH_BUILD_PATH", "_build_" <> side.label}, {"MIX_ENV", "dev"}] ++ side.env

  defp time_args(%{time: nil}), do: []

  defp time_args(%{time: time} = config),
    do: ["--time", to_string(time)] ++ if(config.allow_short, do: ["--allow-short"], else: [])
end
