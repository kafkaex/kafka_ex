defmodule KafkaEx.Mixfile do
  use Mix.Project

  def project do
    [app: :kafka_ex,
     version: "0.0.1",
     elixir: "~> 1.0",
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
end
