defmodule KafkaEx.Mixfile do
  use Mix.Project

  def project do
    [app: :kafka_ex,
     version: "0.3.0",
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
      {:earmark, "~> 0.1", only: :dev},
      {:dialyze, "~> 0.1.3", only: :dev},
      {:ex_doc, "~> 0.7", only: :dev},
      # TODO: this is a fork of fdmanana/snappy-erlang-nif
      # with R18 supported -
      #    see https://github.com/fdmanana/snappy-erlang-nif/pull/12
      {:snappy,
       git: "https://github.com/ricecake/snappy-erlang-nif",
       tag: "270fa36bee692c97f00c3f18a5fb81c5275b83a3",
       only: [:dev, :test]}
    ]
  end

  defp description do
    "Kafka client for Elixir/Erlang."
  end

  defp package do
    [contributors: ["Abejide Ayodele", "Jack Lund"],
     files: ["lib", "mix.exs", "README.md"],
     licenses: ["MIT"],
     links: %{"Github" => "https://github.com/jacklund/kafka_ex"}]
  end
end
