defmodule KafkaEx.Mixfile do
  @moduledoc false
  use Mix.Project

  @source_url "https://github.com/kafkaex/kafka_ex"
  @version "1.0.0"

  def project do
    [
      app: :kafka_ex,
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      dialyzer: [
        plt_add_apps: [:ssl],
        plt_core_path: "priv/plts",
        plt_file: {:no_warn, "priv/plts/kafka_ex.plt"},
        flags: [:error_handling]
      ],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.html": :test,
        "test.unit": :test,
        "test.integration": :test,
        "test.chaos": :test
      ],
      description: description(),
      package: package(),
      deps: deps(),
      aliases: aliases(),
      docs: [
        main: "readme",
        extras: [
          "README.md",
          "AUTH.md",
          "CHANGELOG.md",
          "CONTRIBUTING.md",
          "UPGRADING.md"
        ],
        source_url: @source_url,
        source_ref: @version
      ]
    ]
  end

  def application do
    [
      mod: {KafkaEx, []},
      extra_applications: [:logger, :ssl, :crypto, :public_key]
    ]
  end

  defp deps do
    [
      {:kayrock, path: "../kayrock"},
      {:telemetry, "~> 1.2"},
      {:credo, "~> 1.1", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: :test, runtime: false},
      {:ex_doc, "~> 0.23", only: :dev, runtime: false},
      {:hammox, "~> 0.5.0", only: :test},
      {:snappyer, "~> 1.2", optional: true},
      {:aws_credentials, "~> 1.0", optional: true},
      {:aws_signature, "~> 0.4.2", optional: true},
      {:jason, "~> 1.0", optional: true},
      {:testcontainers, "~> 1.14", only: :test}
    ]
  end

  defp description do
    "Elixir client for Apache Kafka with automatic API version negotiation, SASL authentication (PLAIN, SCRAM, OAuth, MSK IAM), consumer groups, compression, and telemetry support."
  end

  defp aliases do
    [
      "test.unit":
        "test --exclude auth --exclude consume --exclude consumer_group --exclude chaos --exclude lifecycle --exclude produce",
      "test.integration":
        "test --include auth --include consume --include consumer_group --include lifecycle --include produce",
      "test.chaos": "test --only chaos"
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      maintainers: ["Abejide Ayodele", "Dan Swain", "Jack Lund", "Joshua Scott", "Piotr Rybarczyk"],
      files: [
        "lib",
        "config/config.exs",
        "mix.exs",
        "README.md",
        "LICENSE",
        "AUTH.md",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "UPGRADING.md",
        "usage-rules.md"
      ],
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Documentation" => "https://hexdocs.pm/kafka_ex",
        "Changelog" => "#{@source_url}/blob/master/CHANGELOG.md"
      }
    ]
  end
end
