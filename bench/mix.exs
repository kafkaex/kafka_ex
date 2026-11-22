defmodule KafkaExBench.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafka_ex_bench,
      version: "0.1.0",
      elixir: "~> 1.17",
      build_path: System.get_env("BENCH_BUILD_PATH", "_build"),
      deps: deps()
    ]
  end

  def application, do: [extra_applications: [:logger, :inets, :ssl]]

  defp deps do
    [
      {:kafka_ex, path: System.get_env("KAFKA_EX_PATH", "..")},
      {:benchee, "~> 1.3"},
      {:jason, "~> 1.4"}
    ]
  end
end
