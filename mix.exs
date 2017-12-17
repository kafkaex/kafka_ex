defmodule KafkaEx.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kafka_ex,
      version: "0.8.1",
      elixir: "~> 1.1",
      dialyzer: [
        plt_add_deps: :transitive,
        flags: [
          :error_handling,
          :race_conditions
        ]
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test],
      description: description(),
      package: package(),
      deps: deps(),
      docs: [
        main: "readme",
        extras: ["README.md"],
        source_url: "https://github.com/kafkaex/kafka_ex"
      ]
    ]
  end

  def application do
    [
      mod: {KafkaEx, []},
      applications: [:logger, :ssl]
    ]
  end

  defp deps do
    [
      {:credo, "~> 0.8.6", only: :dev},
      {:dialyxir, "~> 0.5.1", only: :dev},
      {:earmark, "~> 1.0.3", only: :dev},
      {:excoveralls, "~> 0.7", only: :test},
      {:ex_doc, "~> 0.14.5", only: :dev},
      {:snappy,
       git: "https://github.com/fdmanana/snappy-erlang-nif",
       only: [:dev, :test]}
    ]
  end

  defp description do
    "Kafka client for Elixir/Erlang."
  end

  defp package do
    [maintainers: ["Abejide Ayodele", "Dan Swain", "Jack Lund"],
     files: ["lib", "config/config.exs", "mix.exs", "README.md"],
     licenses: ["MIT"],
     links: %{"Github" => "https://github.com/kafkaex/kafka_ex"}]
  end
end
