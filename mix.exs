defmodule KafkaEx.Mixfile do
  use Mix.Project

  def project do
    [app: :kafka_ex,
     version: "0.3.0",
     elixir: "~> 1.0",
     description: description,
     package: package,
     deps: deps,
     aliases: [test: "test --no-start"]]
  end

  def application do
    [
      mod: {KafkaEx, []},
      applications: [:logger]
    ]
  end

  defp deps do
    [
      {:earmark, "~> 0.1", only: :dev},
      {:dialyze, "~> 0.1.3", only: :dev},
      {:ex_doc, "~> 0.7", only: :dev},
      {:snappy,
       git: "https://github.com/fdmanana/snappy-erlang-nif",
       only: [:dev, :test]}
    ]
  end

  defp description do
    "Kafka client for Elixir/Erlang."
  end

  defp package do
    [maintainers: ["Abejide Ayodele", "Jack Lund"],
     files: ["lib", "mix.exs", "README.md"],
     licenses: ["MIT"],
     links: %{"Github" => "https://github.com/jacklund/kafka_ex"}]
  end
end
