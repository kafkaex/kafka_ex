defmodule KafkaEx.Mixfile do
  use Mix.Project

  def project do
    [app: :kafka_ex,
     version: "0.0.1",
     elixir: "~> 1.0",
     description: description,
     package: package,
     deps: deps]
  end

  def application do
    [
      mod: {KafkaEx, []},
      applications: [:logger]
    ]
  end

  defp deps do
    [
      {:mock, ">= 0.1.0", only: :test},
      {:earmark, "~> 0.1", only: :dev},
      {:ex_doc, "~> 0.7", only: :dev},
    ]
  end

  defp description do
    "Kafka client for Elixir/Erlang."
  end

  defp package do
    [contributors: ["Abejide Ayodele", "Jack Lund"],
     files: ["lib", "mix.exs", "README.md"],
     links: %{"Github" => "https://github.com/jacklund/kafka_ex"}]
  end
end
