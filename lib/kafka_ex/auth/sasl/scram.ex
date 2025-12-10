defmodule KafkaEx.Auth.SASL.Scram do
  @moduledoc """
  SASL/SCRAM mechanism for Kafka (SCRAM-SHA-256/512).
  """
  @sha256 "SCRAM-SHA-256"
  @sha512 "SCRAM-SHA-512"

  @behaviour KafkaEx.Auth.Mechanism
  alias KafkaEx.Auth.{Config, ScramFlow}

  @impl true
  def mechanism_name(%Config{mechanism_opts: %{algo: :sha512}}), do: @sha512
  @impl true
  # default
  def mechanism_name(%Config{}), do: @sha256

  @impl true
  def authenticate(%Config{username: u, password: p, mechanism_opts: opts}, send_fun) do
    algo = (opts[:algo] == :sha512 && :sha512) || :sha256
    ScramFlow.authenticate(u, p, algo, send_fun)
  end
end
