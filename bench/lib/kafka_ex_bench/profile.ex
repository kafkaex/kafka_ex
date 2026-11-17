defmodule KafkaExBench.Profile do
  alias KafkaExBench.Toxiproxy

  @ports %{
    {:direct, :ssl} => [9092, 9093, 9094],
    {:direct, :scram} => [9292, 9293, 9294],
    {:latency, :ssl} => [9492, 9493, 9494],
    {:latency, :scram} => [9592, 9593, 9594]
  }

  def parse!("direct"), do: :direct
  def parse!("latency"), do: :latency

  def parse_security!("ssl"), do: :ssl
  def parse_security!("scram"), do: :scram

  def latency_ms, do: String.to_integer(System.get_env("BENCH_LATENCY_MS", "2"))

  def prepare!(:direct), do: :ok
  def prepare!(:latency), do: Toxiproxy.set_latency!(latency_ms())

  def client_opts(profile, security \\ :ssl) do
    [
      brokers: Enum.map(Map.fetch!(@ports, {profile, security}), &{"localhost", &1}),
      use_ssl: true,
      ssl_options: [verify: :verify_none],
      consumer_group: :no_consumer_group,
      allow_auto_topic_creation: false
    ] ++ auth(security)
  end

  defp auth(:ssl), do: []

  defp auth(:scram) do
    [
      auth:
        KafkaEx.Auth.Config.new(%{
          mechanism: :scram,
          username: "test",
          password: "secret",
          mechanism_opts: %{algo: :sha256}
        })
    ]
  end
end
