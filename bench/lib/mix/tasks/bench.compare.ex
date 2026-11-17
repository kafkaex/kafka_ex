defmodule Mix.Tasks.Bench.Compare do
  use Mix.Task
  alias KafkaExBench.Compare
  alias KafkaExBench.Compare.Sides
  alias KafkaExBench.Profile

  @shortdoc "Runs a baseline and a candidate kafka_ex interleaved and applies the pass/fail rule"

  @switches [
    base: :string,
    candidate: :string,
    scenarios: :string,
    profiles: :string,
    runs: :integer,
    threshold: :float,
    min_valid: :integer,
    candidate_env: :keep,
    allow_ungated: :boolean,
    time: :integer,
    allow_short: :boolean,
    allow_dirty: :boolean,
    out: :string
  ]

  def run(argv) do
    config = argv |> parse!() |> Map.update!(:out, &Path.expand/1)
    bench_dir = Path.dirname(Mix.Project.project_file())

    compare =
      Sides.with_sides(config, Sides.default_opts(config), fn sides ->
        Compare.execute(config, sides, Compare.system_runner(bench_dir), compile: Compare.system_compile(bench_dir))
      end)

    Mix.shell().info("overall verdict: #{compare.verdict}; report written to #{config.out}/compare.md")
    warn_ungated(compare)

    case Compare.exit_status(compare) do
      0 -> :ok
      status -> exit({:shutdown, status})
    end
  end

  defp warn_ungated(%{verdict: :not_gated, config: %{allow_ungated: allowed}}) do
    if not allowed,
      do:
        Mix.shell().error(
          "WARNING: no gated cell in this comparison (only the latency profile gates); nothing was judged, exiting non-zero. Pass --allow-ungated for an informational run."
        )
  end

  defp warn_ungated(_compare), do: :ok

  def parse!(argv) do
    {opts, _} = OptionParser.parse!(argv, strict: @switches)
    runs = Keyword.get(opts, :runs, 6)
    if runs < 1, do: Mix.raise("--runs must be at least 1")
    min_valid = Keyword.get(opts, :min_valid)
    if min_valid && min_valid < 2, do: Mix.raise("--min-valid must be at least 2")

    %{
      base: required!(opts, :base),
      candidate: required!(opts, :candidate),
      scenarios: scenarios!(Keyword.get(opts, :scenarios)),
      profiles: profiles!(Keyword.get(opts, :profiles, "latency,direct")),
      runs: runs,
      threshold_pct: opts |> Keyword.get(:threshold, 5.0) |> Kernel./(1),
      min_valid: min_valid,
      allow_ungated: Keyword.get(opts, :allow_ungated, false),
      candidate_env: opts |> Keyword.get_values(:candidate_env) |> Enum.map(&env_pair!/1),
      out: required!(opts, :out),
      time: Keyword.get(opts, :time),
      allow_short: Keyword.get(opts, :allow_short, false),
      allow_dirty: Keyword.get(opts, :allow_dirty, false)
    }
  end

  defp required!(opts, key),
    do: Keyword.get(opts, key) || Mix.raise("--#{key |> to_string() |> String.replace("_", "-")} is required")

  defp scenarios!(nil), do: ~w(basic mixed one_broker large_fetch cold_start restart_recovery)

  defp scenarios!(csv) do
    known = Mix.Tasks.Bench.Run.scenarios()
    names = csv |> String.split(",", trim: true) |> Enum.uniq()

    for name <- names,
        not Map.has_key?(known, name),
        do: Mix.raise("unknown scenario #{name}, expected one of #{known |> Map.keys() |> Enum.join(", ")}")

    names
  end

  defp profiles!(csv), do: csv |> String.split(",", trim: true) |> Enum.uniq() |> Enum.map(&Profile.parse!/1)

  defp env_pair!(pair) do
    case String.split(pair, "=", parts: 2) do
      [key, value] when key != "" -> {key, value}
      _ -> Mix.raise("--candidate-env expects KEY=VAL, got #{pair}")
    end
  end
end
