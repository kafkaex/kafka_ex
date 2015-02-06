defmodule Kafka.Mixfile do
  use Mix.Project

  def project do
    [app: :kafka,
     version: "0.0.1",
     elixir: "~> 1.0",
     deps: deps]
  end

  def application do
  end

  defp deps do
    [{:mock, ">= 0.1.0"}]
  end
end
